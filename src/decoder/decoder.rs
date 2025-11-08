use brotli::Decompressor as BrotliDecompressor;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use bzip2::read::BzDecoder;
use flate2::read::{GzDecoder, ZlibDecoder};
use lz4_flex::block::decompress_size_prepended as lz4_decompress_size_prepended;
use lz4_flex::frame::FrameDecoder as Lz4FrameDecoder;
use snap::raw::Decoder as SnappyRawDecoder;
use snap::read::FrameDecoder as SnappyFrameDecoder;
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::{Mutex, OnceLock};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use zstd::Decoder as ZstdDecoder;

/// A very small, self-contained Decoder layer implementation for the Shred Pipeline.
///
/// Notes:
/// - This decoder is intentionally implementation-light so it can be integrated into the
///   scaffold without depending on external protobufs or Solana crates.
/// - It expects each incoming datagram to contain a single serialized Solana
///   VersionedTransaction (from Jito ShredStream's Entry.entries) or another
///   representation; the decode_raw_tx function below attempts to deserialize it.
///
/// The decoder emits DecodedShred instances on `output`. Downstream components may
/// call `decode_raw_tx` to convert raw transaction bytes into a summarized structure.
#[derive(Debug, Clone)]
pub struct DecodedShred {
    pub slot: u64,
    pub index: u32,
    pub is_data: bool,
    pub payload: Vec<u8>,
}

pub struct Decoder {
    /// handle to the spawned decoder task
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

static BE_LEN_PREFIX_FAILURE_COUNTS: OnceLock<Mutex<HashMap<(u64, usize), u32>>> = OnceLock::new();
const BE_LEN_PREFIX_FAILURE_LIMIT: u32 = 3;

fn should_log_be_len_prefixed_failure(slot: u64, len: usize) -> bool {
    let map = BE_LEN_PREFIX_FAILURE_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .expect("be len-prefixed failure tracker mutex poisoned");
    let count = guard.entry((slot, len)).or_insert(0);
    if *count < BE_LEN_PREFIX_FAILURE_LIMIT {
        *count += 1;
        true
    } else {
        false
    }
}

#[cfg(test)]
fn reset_be_len_prefixed_failure_log() {
    if let Some(map) = BE_LEN_PREFIX_FAILURE_COUNTS.get() {
        map.lock()
            .expect("be len-prefixed failure tracker mutex poisoned")
            .clear();
    }
}

impl Decoder {
    /// Spawn the decoder task.
    ///
    /// - `mut input` is the mpsc receiver that yields raw datagrams from the Receiver layer.
    /// - `output` is an mpsc sender that will receive decoded shreds.
    ///
    /// Returns a Decoder instance which can be shutdown via `shutdown().await`.
    pub fn spawn(mut input: mpsc::Receiver<Vec<u8>>, output: mpsc::Sender<DecodedShred>) -> Self {
        let (tx, mut rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            // simple in-memory grouping by slot - placeholder for reassembly/erasure logic
            let mut slot_buffer: HashMap<u64, Vec<DecodedShred>> = HashMap::new();

            loop {
                tokio::select! {
                    biased;

                    // shutdown requested
                    _ = &mut rx => {
                        // flush or perform any finalization if needed
                        break;
                    }

                    // receive the next raw datagram
                    maybe_pkt = input.recv() => {
                        match maybe_pkt {
                            Some(pkt) => {
                                match parse_shred(&pkt) {
                                    Ok(shred) => {
                                        // send decoded shred downstream (best-effort)
                                        if output.send(shred.clone()).await.is_err() {
                                            // downstream closed: stop processing
                                            break;
                                        }

                                        // placeholder grouping logic: collect per-slot
                                        let slot = shred.slot;
                                        {
                                            let entry = slot_buffer.entry(slot).or_default();
                                            entry.push(shred);
                                        }

                                        // check length without holding a mutable borrow, then remove if needed
                                        let len = slot_buffer.get(&slot).map(|v| v.len()).unwrap_or(0);
                                        if len >= 50 {
                                            // in a real implementation you'd attempt reconstruction here
                                            let _ = slot_buffer.remove(&slot);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("decoder: failed to parse shred header: {}", e);
                                        // drop malformed packet and continue
                                    }
                                }
                            }
                            None => {
                                // input closed, stop processing
                                break;
                            }
                        }
                    }
                }
            }
        });

        Decoder {
            handle: Some(handle),
            shutdown_tx: Some(tx),
        }
    }

    /// Request graceful shutdown and wait for the task to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

/// Parse a raw datagram into a DecodedShred.
///
/// Expected wire-format for the shred layer used in this simplified scaffold:
///   0..8   -> slot (u64 little-endian)
///   8..12  -> index (u32 little-endian)
///   12     -> is_data (0 or 1)
///   13..   -> payload
fn parse_shred(buf: &[u8]) -> Result<DecodedShred, String> {
    if buf.len() < 13 {
        return Err(format!("packet too small: {}", buf.len()));
    }

    // Using bytes::Bytes for convenient LE reads.
    let mut b = Bytes::copy_from_slice(buf);

    let slot = b.get_u64_le();
    let index = b.get_u32_le();
    let is_data = match b.get_u8() {
        0 => false,
        1 => true,
        v => return Err(format!("invalid is_data flag: {}", v)),
    };

    let payload = b.to_vec();

    Ok(DecodedShred {
        slot,
        index,
        is_data,
        payload,
    })
}

//
// New: transaction summary & decoder helper
//
use crate::shredstream::Entry as ProtoEntry;
use anyhow::anyhow;
use anyhow::{bail, Context, Result};
use base64::Engine; // <- brings `.encode()` into scope
use bincode;
use bincode::DefaultOptions;
use bincode::Options;
use prost::Message;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use solana_entry::entry::Entry as SolanaEntry;
use solana_sdk::message::VersionedMessage;
use solana_sdk::transaction::Transaction; // <- legacy Transaction type
use solana_sdk::transaction::VersionedTransaction;
use std::convert::TryFrom; // <- for VersionedTransaction::try_from(legacy)

#[derive(Debug, Serialize, Clone)]
pub struct DecodedTxSummary {
    pub slot: u64,
    pub signature: String,
    pub signers: Vec<String>,
    pub account_keys: Vec<String>,
    pub program_ids: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
struct LegacyEntry {
    pub num_hashes: u64,
    pub hash: solana_sdk::hash::Hash,
    pub transactions: Vec<Transaction>,
}

fn decode_legacy_transaction(slot: u64, tx: Transaction) -> Option<DecodedTxSummary> {
    VersionedTransaction::try_from(tx)
        .ok()
        .and_then(|versioned| decode_raw_tx(slot, &versioned).ok())
}

fn deserialize_with_varint_fallback<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode::Error> {
    bincode::deserialize::<T>(bytes)
        .or_else(|_| {
            DefaultOptions::new()
                .with_varint_encoding()
                .deserialize(bytes)
        })
        .or_else(|_| DefaultOptions::new().with_big_endian().deserialize(bytes))
        .or_else(|_| {
            DefaultOptions::new()
                .with_big_endian()
                .with_varint_encoding()
                .deserialize(bytes)
        })
}

fn deserialize_from_with_varint_fallback<T, R>(cur: &mut R) -> Result<T, bincode::Error>
where
    T: DeserializeOwned,
    R: Read + Seek,
{
    let pos = cur.stream_position().unwrap_or(0);
    match bincode::deserialize_from::<_, T>(&mut *cur) {
        Ok(v) => Ok(v),
        Err(_e) => {
            cur.seek(SeekFrom::Start(pos)).ok();
            let result = DefaultOptions::new()
                .with_varint_encoding()
                .deserialize_from::<_, T>(&mut *cur);
            if result.is_err() {
                cur.seek(SeekFrom::Start(pos)).ok();
            }
            result.or_else(|_| {
                let _ = cur.seek(SeekFrom::Start(pos));
                let res = DefaultOptions::new()
                    .with_big_endian()
                    .deserialize_from::<_, T>(&mut *cur);
                if res.is_err() {
                    let _ = cur.seek(SeekFrom::Start(pos));
                    DefaultOptions::new()
                        .with_big_endian()
                        .with_varint_encoding()
                        .deserialize_from::<_, T>(&mut *cur)
                } else {
                    res
                }
            })
        }
    }
}

/// Decode a single VersionedTransaction -> DecodedTxSummary (same logic as before)
pub fn decode_raw_tx(slot: u64, tx: &VersionedTransaction) -> Result<DecodedTxSummary> {
    // signature (first signature if present)
    let signature = tx
        .signatures
        .get(0)
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Access versioned message via the public field
    let message = &tx.message;

    // account_keys and other derived fields
    let (account_keys, num_required_signatures, instructions): (Vec<String>, usize, Vec<_>) =
        match message {
            VersionedMessage::Legacy(msg) => {
                let keys = msg
                    .account_keys
                    .iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>();
                let nreq = msg.header.num_required_signatures as usize;
                let instrs = msg.instructions.clone();
                (keys, nreq, instrs)
            }
            VersionedMessage::V0(_) => {
                // V0 messages require address-table expansion to obtain the full account keys list.
                // Return an explicit error for now.
                bail!("V0 VersionedMessage not supported yet (address-table expansion required)");
            }
        };

    // signers: first num_required_signatures account keys
    let signers: Vec<String> = account_keys
        .iter()
        .take(num_required_signatures)
        .cloned()
        .collect();

    // program_ids: unique program id keys used by instructions
    let mut program_ids = Vec::<String>::new();
    for ix in &instructions {
        let pid_idx = ix.program_id_index as usize;
        if let Some(pid) = account_keys.get(pid_idx) {
            if !program_ids.contains(pid) {
                program_ids.push(pid.clone());
            }
        }
    }

    Ok(DecodedTxSummary {
        slot,
        signature,
        signers,
        account_keys,
        program_ids,
    })
}

/// Try to extract inner tx byte slices from several common container encodings.
fn extract_tx_byte_slices(raw: &[u8]) -> Vec<Vec<u8>> {
    // 1) Try decode as bincode Vec<Vec<u8>>
    if let Ok(v) = deserialize_with_varint_fallback::<Vec<Vec<u8>>>(raw) {
        if !v.is_empty() {
            return v;
        }
    }

    // 2) Try parse as u32_le length-prefixed frames
    let mut frames = Vec::new();
    let mut cur = Cursor::new(raw);
    while (cur.position() as usize) < raw.len() {
        if let Ok(len) = cur.read_u32::<LittleEndian>() {
            let len_usize = len as usize;
            let pos = cur.position() as usize;
            if pos + len_usize > raw.len() {
                frames.clear();
                break;
            }
            frames.push(raw[pos..pos + len_usize].to_vec());
            let _ = cur.set_position((pos + len_usize) as u64);
        } else {
            frames.clear();
            break;
        }
    }
    if !frames.is_empty() {
        return frames;
    }

    // 3) Try parse as u16_le length-prefixed frames
    let mut frames16 = Vec::new();
    let mut cur2 = Cursor::new(raw);
    while (cur2.position() as usize) < raw.len() {
        if let Ok(len) = cur2.read_u16::<LittleEndian>() {
            let len_usize = len as usize;
            let pos = cur2.position() as usize;
            if pos + len_usize > raw.len() {
                frames16.clear();
                break;
            }
            frames16.push(raw[pos..pos + len_usize].to_vec());
            let _ = cur2.set_position((pos + len_usize) as u64);
        } else {
            frames16.clear();
            break;
        }
    }
    if !frames16.is_empty() {
        return frames16;
    }

    // nothing extracted
    Vec::new()
}

/// Try multiple strategies to obtain one-or-many VersionedTransaction objects from raw bytes.
///
/// Strategies:
/// 1) bincode deserialize as a single VersionedTransaction
/// 2) bincode deserialize as Vec<VersionedTransaction>
/// 3) bincode deserialize as Vec<Vec<u8>> -> each inner bincode deserialize VersionedTransaction
/// 4) bincode deserialize as Entry (single) -> iterate entry.transactions
/// 5) bincode deserialize as Vec<Entry> (Solana entries) -> for each entry.transactions try decode each tx
/// 6) length-prefixed frames (u32/u16) -> each frame bincode deserialize VersionedTransaction
/// 7) protobuf shredstream.Entry message (with embedded bincode Vec<Entry>)
/// 8) bincode deserialize Entry stream (consecutive Entry objects without outer Vec)
pub fn decode_raw_txs(slot: u64, raw_tx: &[u8]) -> anyhow::Result<Vec<DecodedTxSummary>> {
    // 1) Try single tx
    if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(raw_tx) {
        let s =
            decode_raw_tx(slot, &tx).context("failed to summarize single VersionedTransaction")?;
        return Ok(vec![s]);
    }

    if let Ok(legacy_tx) = deserialize_with_varint_fallback::<Transaction>(raw_tx) {
        if let Some(summary) = decode_legacy_transaction(slot, legacy_tx) {
            return Ok(vec![summary]);
        }
    }

    // 2) Try Vec<VersionedTransaction>
    if let Ok(txs) = deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(raw_tx) {
        let mut summaries = Vec::with_capacity(txs.len());
        for tx in txs.into_iter() {
            let s = decode_raw_tx(slot, &tx)
                .context("failed to summarize one tx inside Vec<VersionedTransaction>")?;
            summaries.push(s);
        }
        return Ok(summaries);
    }

    if let Ok(txs) = deserialize_with_varint_fallback::<Vec<Transaction>>(raw_tx) {
        let mut summaries = Vec::new();
        for tx in txs.into_iter() {
            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                summaries.push(summary);
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 3) Try Vec<Vec<u8>>
    if let Ok(frames) = deserialize_with_varint_fallback::<Vec<Vec<u8>>>(raw_tx) {
        let mut summaries = Vec::new();
        for frame_bytes in frames.into_iter() {
            let mut decoded = false;

            if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(&frame_bytes) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    summaries.push(s);
                    decoded = true;
                }
            }

            if !decoded {
                if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame_bytes) {
                    if let Some(summary) = decode_legacy_transaction(slot, tx) {
                        summaries.push(summary);
                        decoded = true;
                    }
                }
            }

            if !decoded {
                if let Ok(nested) =
                    deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&frame_bytes)
                {
                    for tx in nested {
                        if let Ok(s) = decode_raw_tx(slot, &tx) {
                            summaries.push(s);
                        }
                    }
                    decoded = true;
                }

                if !decoded {
                    if let Ok(nested) =
                        deserialize_with_varint_fallback::<Vec<Transaction>>(&frame_bytes)
                    {
                        for tx in nested {
                            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                summaries.push(summary);
                            }
                        }
                    }
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 4) Try single Entry (Solana entry container)
    if let Ok(entry) = deserialize_with_varint_fallback::<SolanaEntry>(raw_tx) {
        let mut summaries = Vec::new();
        for tx in entry.transactions.into_iter() {
            if let Ok(s) = decode_raw_tx(slot, &tx) {
                summaries.push(s);
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    if let Ok(entry) = deserialize_with_varint_fallback::<LegacyEntry>(raw_tx) {
        let mut summaries = Vec::new();
        for tx in entry.transactions.into_iter() {
            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                summaries.push(summary);
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 5) Try Vec<Entry> (Solana entries container)
    if let Ok(entries) = deserialize_with_varint_fallback::<Vec<SolanaEntry>>(raw_tx) {
        let mut summaries = Vec::new();
        for entry in entries.into_iter() {
            for tx in entry.transactions.into_iter() {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    summaries.push(s);
                    decoded = true;
                }
            }

            if !decoded {
                if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame_bytes) {
                    if let Some(summary) = decode_legacy_transaction(slot, tx) {
                        summaries.push(summary);
                        decoded = true;
                    }
                }
            }

            if !decoded {
                if let Ok(nested) =
                    deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&frame_bytes)
                {
                    for tx in nested {
                        if let Ok(s) = decode_raw_tx(slot, &tx) {
                            summaries.push(s);
                        }
                    }
                    decoded = true;
                }

                if !decoded {
                    if let Ok(nested) =
                        deserialize_with_varint_fallback::<Vec<Transaction>>(&frame_bytes)
                    {
                        for tx in nested {
                            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                summaries.push(summary);
                            }
                        }
                    }
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    if let Ok(entries) = deserialize_with_varint_fallback::<Vec<LegacyEntry>>(raw_tx) {
        let mut summaries = Vec::new();
        for entry in entries.into_iter() {
            for tx in entry.transactions.into_iter() {
                if let Some(summary) = decode_legacy_transaction(slot, tx) {
                    summaries.push(summary);
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 6) length-prefixed frames (u32/u16)
    let frames_lp = extract_tx_byte_slices(raw_tx);
    if !frames_lp.is_empty() {
        let mut summaries = Vec::new();
        for frame_bytes in frames_lp.into_iter() {
            let mut decoded = false;

            if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(&frame_bytes) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    summaries.push(s);
                    decoded = true;
                }
            }

            if !decoded {
                if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame_bytes) {
                    if let Some(summary) = decode_legacy_transaction(slot, tx) {
                        summaries.push(summary);
                        decoded = true;
                    }
                }
            }

            if !decoded {
                if let Ok(nested) =
                    deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&frame_bytes)
                {
                    for tx in nested {
                        if let Ok(s) = decode_raw_tx(slot, &tx) {
                            summaries.push(s);
                        }
                    }
                    decoded = true;
                }

                if !decoded {
                    if let Ok(nested) =
                        deserialize_with_varint_fallback::<Vec<Transaction>>(&frame_bytes)
                    {
                        for tx in nested {
                            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                summaries.push(summary);
                            }
                        }
                    }
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // Try big-endian 4-byte length-prefixed stream (existing)
    if let Ok(summaries) = try_be_len_prefixed_stream(slot, raw_tx) {
        return Ok(summaries);
    }

    // Try little-endian 4-byte length-prefixed stream
    if let Ok(summaries) = try_le_len_prefixed_stream(slot, raw_tx) {
        return Ok(summaries);
    }

    // Try little-endian 2-byte length-prefixed stream
    if let Ok(summaries) = try_le_u16_prefixed_stream(slot, raw_tx) {
        return Ok(summaries);
    }

    // 7) Try protobuf Entry message from Shredstream (direct payload)
    let try_proto_entry = |bytes: &[u8]| -> Option<Vec<DecodedTxSummary>> {
        if let Ok(proto) = ProtoEntry::decode(bytes) {
            if proto.entries.is_empty() {
                return None;
            }

            // Avoid infinite recursion if the embedded bytes equal the original slice
            if proto.entries.as_slice() != bytes {
                if let Ok(summaries) = decode_raw_txs(proto.slot, &proto.entries) {
                    return Some(summaries);
                }
            }

            // Fallback: directly attempt to decode the embedded entries as Solana entries
            if let Ok(entries) =
                deserialize_with_varint_fallback::<Vec<SolanaEntry>>(&proto.entries)
            {
                let mut summaries = Vec::new();
                for entry in entries.into_iter() {
                    for tx in entry.transactions.into_iter() {
                        if let Ok(s) = decode_raw_tx(proto.slot, &tx) {
                            summaries.push(s);
                        }
                    }
                }
                if !summaries.is_empty() {
                    return Some(summaries);
                }
            }

            if let Ok(entries) =
                deserialize_with_varint_fallback::<Vec<LegacyEntry>>(&proto.entries)
            {
                let mut summaries = Vec::new();
                for entry in entries.into_iter() {
                    for tx in entry.transactions.into_iter() {
                        if let Some(s) = decode_legacy_transaction(proto.slot, tx) {
                            summaries.push(s);
                        }
                    }
                }
                if !summaries.is_empty() {
                    return Some(summaries);
                }
            }

            if let Ok(entry) = deserialize_with_varint_fallback::<SolanaEntry>(&proto.entries) {
                let mut summaries = Vec::new();
                for tx in entry.transactions.into_iter() {
                    if let Ok(s) = decode_raw_tx(proto.slot, &tx) {
                        summaries.push(s);
                    }
                }
                if !summaries.is_empty() {
                    return Some(summaries);
                }
            }

            if let Ok(entry) = deserialize_with_varint_fallback::<LegacyEntry>(&proto.entries) {
                let mut summaries = Vec::new();
                for tx in entry.transactions.into_iter() {
                    if let Some(s) = decode_legacy_transaction(proto.slot, tx) {
                        summaries.push(s);
                    }
                }
                if !summaries.is_empty() {
                    return Some(summaries);
                }
            }
        }
        None
    };

    if let Some(summaries) = try_proto_entry(raw_tx) {
        return Ok(summaries);
    }

    if raw_tx.len() > 5 {
        let flag = raw_tx[0];
        let len = u32::from_be_bytes([raw_tx[1], raw_tx[2], raw_tx[3], raw_tx[4]]) as usize;
        if flag <= 1 && len <= raw_tx.len().saturating_sub(5) {
            let frame = &raw_tx[5..5 + len];
            if let Some(summaries) = try_proto_entry(frame) {
                return Ok(summaries);
            }
        }
    }

    // 8) Try Entry stream (no outer Vec)
    if let Ok(summaries) = try_deserialize_entry_stream(slot, raw_tx) {
        return Ok(summaries);
    }

    // Heuristic: skip known shred header lengths before retrying decode on the remaining slice.
    const HEADER_OFFSETS: [usize; 4] = [64, 83, 96, 104];
    for &offset in &HEADER_OFFSETS {
        if raw_tx.len() > offset {
            if let Ok(summaries) = decode_raw_txs(slot, &raw_tx[offset..]) {
                return Ok(summaries);
            }
        }
    }

    // Try decompress (zlib/gzip/snappy) and re-run decode on decompressed bytes
    if let Ok(summaries) = try_decompress_and_decode(slot, raw_tx) {
        return Ok(summaries);
    }

    // As a last resort, scan for embedded transactions at arbitrary offsets
    let scanned = scan_for_embedded_txs(slot, raw_tx);
    if !scanned.is_empty() {
        return Ok(scanned);
    }

    // nothing matched
    Err(anyhow::anyhow!("raw bytes are neither bincode-serialized VersionedTransaction nor Vec<VersionedTransaction> nor recognized framed tx list nor Solana Entry container nor protobuf shredstream.Entry payload nor Entry stream nor BE/LE-length-prefixed stream nor recognized compressed form"))
}

/// Try to deserialize a stream of bincode-serialized Entry objects from raw bytes.
/// This handles the case when entries were serialized consecutively (no outer Vec).
fn try_deserialize_entry_stream(slot: u64, raw: &[u8]) -> Result<Vec<DecodedTxSummary>> {
    let mut cur = Cursor::new(raw);
    let mut summaries = Vec::new();

    while (cur.position() as usize) < raw.len() {
        // Attempt to deserialize a single Entry from the current cursor.
        let start_pos = cur.position();
        match deserialize_from_with_varint_fallback::<SolanaEntry, _>(&mut cur) {
            Ok(entry) => {
                for tx_item in entry.transactions {
                    if let Ok(s) = decode_raw_tx(slot, &tx_item) {
                        summaries.push(s);
                    }
                }
            }
            Err(first_err) => {
                cur.seek(SeekFrom::Start(start_pos)).ok();
                match deserialize_from_with_varint_fallback::<LegacyEntry, _>(&mut cur) {
                    Ok(entry) => {
                        for tx in entry.transactions {
                            if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                summaries.push(summary);
                            }
                        }
                    }
                    Err(second_err) => {
                        return Err(anyhow!(
                            "failed to deserialize Entry from stream (versioned error: {}; legacy error: {})",
                            first_err,
                            second_err
                        ));
                    }
                }
            }
        }
    }

    if summaries.is_empty() {
        Err(anyhow!("no VersionedTransaction decoded from Entry stream"))
    } else {
        Ok(summaries)
    }
}

fn try_be_len_prefixed_stream(slot: u64, raw: &[u8]) -> anyhow::Result<Vec<DecodedTxSummary>> {
    let mut cur = Cursor::new(raw);
    let mut out = Vec::new();
    let mut last_err: Option<anyhow::Error> = None;

    while (cur.position() as usize) < raw.len() {
        // need at least 4 bytes for a BE length; skip zero bytes if they appear as padding
        let mut peek = [0u8; 4];
        let available = (raw.len() as u64).saturating_sub(cur.position()) as usize;
        if available < 4 {
            last_err = Some(anyhow::anyhow!("not enough bytes for BE length prefix"));
            break;
        }

        // read 4-byte BE length
        cur.read_exact(&mut peek)?;
        let len_be = Cursor::new(peek).read_u32::<BigEndian>()?;
        if len_be == 0 {
            // skip zero-length prefix (likely padding) and continue
            continue;
        }
        let len_usize = len_be as usize;
        let remaining = (raw.len() as u64).saturating_sub(cur.position()) as usize;
        if remaining < len_usize {
            last_err = Some(anyhow::anyhow!(
                "length-prefixed chunk (len={}) truncated (remaining={})",
                len_usize,
                remaining
            ));
            break;
        }

        let mut chunk = vec![0u8; len_usize];
        cur.read_exact(&mut chunk)?;

        // Primary: try the top-level decoder (it already tries many formats)
        match decode_raw_txs(slot, &chunk) {
            Ok(mut v) => {
                out.append(&mut v);
                continue;
            }
            Err(_primary_err) => {
                // fallback attempts: try common bincode container forms and entry stream
                let mut decoded_any = false;

                // a) direct VersionedTransaction
                if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(&chunk) {
                    if let Ok(s) = decode_raw_tx(slot, &tx) {
                        out.push(s);
                        decoded_any = true;
                    }
                }

                // b) Vec<VersionedTransaction>
                if !decoded_any {
                    if let Ok(txs) =
                        deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&chunk)
                    {
                        for tx in txs.into_iter() {
                            if let Ok(s) = decode_raw_tx(slot, &tx) {
                                out.push(s);
                            }
                        }
                        decoded_any = true;
                    }
                }

                // c) Vec<Vec<u8>>
                if !decoded_any {
                    if let Ok(frames) = deserialize_with_varint_fallback::<Vec<Vec<u8>>>(&chunk) {
                        for frame in frames.into_iter() {
                            if let Ok(tx) =
                                deserialize_with_varint_fallback::<VersionedTransaction>(&frame)
                            {
                                if let Ok(s) = decode_raw_tx(slot, &tx) {
                                    out.push(s);
                                    continue;
                                }
                            }

                            if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame)
                            {
                                if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                    out.push(summary);
                                }
                            }

                            if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame)
                            {
                                if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                    out.push(summary);
                                }
                            }

                            if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&frame)
                            {
                                if let Some(summary) = decode_legacy_transaction(slot, tx) {
                                    out.push(summary);
                                }
                            } else if let Some(summary) =
                                deserialize_with_varint_fallback::<Transaction>(&frame)
                                    .ok()
                                    .and_then(|tx| decode_legacy_transaction(slot, tx))
                            {
                                out.push(summary);
                            }
                        }
                        if !out.is_empty() {
                            decoded_any = true;
                        }
                    }
                }

                // d) try deserialize consecutive Entry objects inside the chunk
                if !decoded_any {
                    let mut cur2 = Cursor::new(&chunk);
                    let mut any = false;
                    loop {
                        let pos = cur2.position();
                        match deserialize_from_with_varint_fallback::<SolanaEntry, _>(&mut cur2) {
                            Ok(entry) => {
                                for tx_item in entry.transactions {
                                    if let Ok(s) = decode_raw_tx(slot, &tx_item) {
                                        out.push(s);
                                    }
                                }
                                any = true;
                            }
                            Err(_) => {
                                cur2.seek(SeekFrom::Start(pos)).ok();
                                match deserialize_from_with_varint_fallback::<LegacyEntry, _>(
                                    &mut cur2,
                                ) {
                                    Ok(entry) => {
                                        for tx in entry.transactions {
                                            if let Some(summary) =
                                                decode_legacy_transaction(slot, tx)
                                            {
                                                out.push(summary);
                                            }
                                        }
                                        any = true;
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    if any {
                        decoded_any = true;
                    }
                }

                if !decoded_any {
                    if let Ok(mut summaries) = try_decompress_and_decode(slot, &chunk) {
                        if !summaries.is_empty() {
                            out.append(&mut summaries);
                            decoded_any = true;
                        }
                    }
                }

                if !decoded_any {
                    let mut scanned = scan_for_embedded_txs(slot, &chunk);
                    if !scanned.is_empty() {
                        out.append(&mut scanned);
                        decoded_any = true;
                    }
                }

                if !decoded_any {
                    // Log chunk sample for inspection (base64 + hex) so we can decide next steps
                    if should_log_be_len_prefixed_failure(slot, chunk.len()) {
                        let b64_chunk = base64::engine::general_purpose::STANDARD.encode(&chunk);
                        let hex_chunk = chunk
                            .iter()
                            .take(128)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join("");
                        eprintln!(
                            "try_be_len_prefixed_stream: unable to decode chunk len={} slot={} sample_base64_prefix={} sample_hex_prefix={}",
                            chunk.len(),
                            slot,
                            if b64_chunk.len() > 512 { &b64_chunk[..512] } else { &b64_chunk },
                            hex_chunk,
                        );
                    }

                    // Attempt: legacy Transaction (non-versioned) -> convert to VersionedTransaction
                    if let Ok(legacy_tx) = deserialize_with_varint_fallback::<Transaction>(&chunk) {
                        match VersionedTransaction::try_from(legacy_tx) {
                            Ok(vtx) => {
                                if let Ok(s) = decode_raw_tx(slot, &vtx) {
                                    out.push(s);
                                    decoded_any = true;
                                }
                            }
                            Err(_) => {
                                // Infallible for current conversions; ignore if it ever errors
                            }
                        }
                    }
                }
                if decoded_any {
                    continue;
                }
            }
        }
    }

    if !out.is_empty() {
        Ok(out)
    } else {
        Err(last_err
            .unwrap_or_else(|| anyhow::anyhow!("no tx decoded from BE-length-prefixed stream")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn be_len_prefixed_failure_logging_is_throttled() {
        reset_be_len_prefixed_failure_log();

        assert!(should_log_be_len_prefixed_failure(58, 5));
        assert!(should_log_be_len_prefixed_failure(58, 5));
        assert!(should_log_be_len_prefixed_failure(58, 5));
        assert!(!should_log_be_len_prefixed_failure(58, 5));

        // ensure other tuples are unaffected
        assert!(should_log_be_len_prefixed_failure(59, 5));
        assert!(should_log_be_len_prefixed_failure(58, 6));

        reset_be_len_prefixed_failure_log();
    }
}

fn try_le_len_prefixed_stream(slot: u64, raw: &[u8]) -> anyhow::Result<Vec<DecodedTxSummary>> {
    let mut cur = Cursor::new(raw);
    let mut out = Vec::new();
    while (cur.position() as usize) < raw.len() {
        let available = raw.len().saturating_sub(cur.position() as usize);
        if available < 4 {
            return Err(anyhow::anyhow!("not enough bytes for LE u32 length prefix"));
        }
        // read little-endian u32 length
        let len = cur.read_u32::<LittleEndian>()? as usize;
        if len == 0 {
            continue;
        }
        let remaining = raw.len().saturating_sub(cur.position() as usize);
        if remaining < len {
            return Err(anyhow::anyhow!(
                "le-length-prefixed chunk truncated (len={} remaining={})",
                len,
                remaining
            ));
        }
        let mut chunk = vec![0u8; len];
        cur.read_exact(&mut chunk)?;
        if let Ok(mut v) = decode_raw_txs(slot, &chunk) {
            out.append(&mut v);
        } else {
            let mut handled = false;

            if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(&chunk) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    out.push(s);
                    handled = true;
                }
            }

            if !handled {
                if let Ok(txs) =
                    deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&chunk)
                {
                    for tx in txs {
                        if let Ok(s) = decode_raw_tx(slot, &tx) {
                            out.push(s);
                        }
                    }
                    handled = true;
                }
            }

            if !handled {
                if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&chunk) {
                    if let Some(summary) = decode_legacy_transaction(slot, tx) {
                        out.push(summary);
                        handled = true;
                    }
                }
            }

            if !handled {
                if let Ok(txs) = deserialize_with_varint_fallback::<Vec<Transaction>>(&chunk) {
                    for tx in txs {
                        if let Some(summary) = decode_legacy_transaction(slot, tx) {
                            out.push(summary);
                        }
                    }
                }
            }
        }
    }
    if out.is_empty() {
        Err(anyhow::anyhow!(
            "no tx decoded from LE-length-prefixed stream"
        ))
    } else {
        Ok(out)
    }
}

fn try_le_u16_prefixed_stream(slot: u64, raw: &[u8]) -> anyhow::Result<Vec<DecodedTxSummary>> {
    let mut cur = Cursor::new(raw);
    let mut out = Vec::new();
    while (cur.position() as usize) < raw.len() {
        let available = raw.len().saturating_sub(cur.position() as usize);
        if available < 2 {
            return Err(anyhow::anyhow!("not enough bytes for LE u16 length prefix"));
        }
        let len = cur.read_u16::<LittleEndian>()? as usize;
        if len == 0 {
            continue;
        }
        let remaining = raw.len().saturating_sub(cur.position() as usize);
        if remaining < len {
            return Err(anyhow::anyhow!(
                "le-u16-length-prefixed chunk truncated (len={} remaining={})",
                len,
                remaining
            ));
        }
        let mut chunk = vec![0u8; len];
        cur.read_exact(&mut chunk)?;
        if let Ok(mut v) = decode_raw_txs(slot, &chunk) {
            out.append(&mut v);
        } else {
            let mut handled = false;

            if let Ok(tx) = deserialize_with_varint_fallback::<VersionedTransaction>(&chunk) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    out.push(s);
                    handled = true;
                }
            }

            if !handled {
                if let Ok(txs) =
                    deserialize_with_varint_fallback::<Vec<VersionedTransaction>>(&chunk)
                {
                    for tx in txs {
                        if let Ok(s) = decode_raw_tx(slot, &tx) {
                            out.push(s);
                        }
                    }
                    handled = true;
                }
            }

            if !handled {
                if let Ok(tx) = deserialize_with_varint_fallback::<Transaction>(&chunk) {
                    if let Some(summary) = decode_legacy_transaction(slot, tx) {
                        out.push(summary);
                        handled = true;
                    }
                }
            }

            if !handled {
                if let Ok(txs) = deserialize_with_varint_fallback::<Vec<Transaction>>(&chunk) {
                    for tx in txs {
                        if let Some(summary) = decode_legacy_transaction(slot, tx) {
                            out.push(summary);
                        }
                    }
                }
            }
        }
    }
    if out.is_empty() {
        Err(anyhow::anyhow!(
            "no tx decoded from LE-u16-length-prefixed stream"
        ))
    } else {
        Ok(out)
    }
}

fn scan_for_embedded_txs(slot: u64, chunk: &[u8]) -> Vec<DecodedTxSummary> {
    let mut found = Vec::new();
    // try starting at each offset (cheap) to find bincode-deserializable tx objects
    for start in 0..chunk.len().saturating_sub(4) {
        let mut cur = Cursor::new(&chunk[start..]);
        if let Ok(tx) = deserialize_from_with_varint_fallback::<VersionedTransaction, _>(&mut cur) {
            if let Ok(s) = decode_raw_tx(slot, &tx) {
                found.push(s);
                // advance start by consumed bytes to avoid overlapping re-decode (best-effort)
            }
        } else {
            // try Vec<VersionedTransaction> at this offset
            let mut cur2 = Cursor::new(&chunk[start..]);
            if let Ok(txs) =
                deserialize_from_with_varint_fallback::<Vec<VersionedTransaction>, _>(&mut cur2)
            {
                for tx in txs {
                    if let Ok(s) = decode_raw_tx(slot, &tx) {
                        found.push(s);
                    }
                }
            }
        }
        // small optimization: stop early if we already found many
        if found.len() > 16 {
            break;
        }
    }
    found
}

fn try_decompress_and_decode(slot: u64, raw: &[u8]) -> anyhow::Result<Vec<DecodedTxSummary>> {
    // zlib (raw DEFLATE with zlib header)
    {
        let mut dec = ZlibDecoder::new(raw);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try gzip
    {
        let mut dec = GzDecoder::new(raw);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try snappy (frame format)
    {
        let mut dec = SnappyFrameDecoder::new(raw);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try snappy (raw format)
    {
        let mut dec = SnappyRawDecoder::new();
        if let Ok(out) = dec.decompress_vec(raw) {
            if !out.is_empty() {
                if let Ok(summaries) = decode_raw_txs(slot, &out) {
                    return Ok(summaries);
                }
            }
        }
    }

    // try bzip2
    {
        let mut dec = BzDecoder::new(Cursor::new(raw));
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try brotli
    {
        let mut dec = BrotliDecompressor::new(Cursor::new(raw), 4096);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try lz4 frame
    {
        let mut dec = Lz4FrameDecoder::new(Cursor::new(raw));
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
            if let Ok(summaries) = decode_raw_txs(slot, &out) {
                return Ok(summaries);
            }
        }
    }

    // try lz4 block with size prefix
    {
        if let Ok(out) = lz4_decompress_size_prepended(raw) {
            if !out.is_empty() {
                if let Ok(summaries) = decode_raw_txs(slot, &out) {
                    return Ok(summaries);
                }
            }
        }
    }

    // try zstd
    {
        if let Ok(mut dec) = ZstdDecoder::new(Cursor::new(raw)) {
            let mut out = Vec::new();
            if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
                if let Ok(summaries) = decode_raw_txs(slot, &out) {
                    return Ok(summaries);
                }
            }
        }
    }

    Err(anyhow::anyhow!("decompression attempts failed"))
}
