use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use bytes::{Buf, Bytes};
use std::collections::HashMap;

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

impl Decoder {
    /// Spawn the decoder task.
    ///
    /// - `mut input` is the mpsc receiver that yields raw datagrams from the Receiver layer.
    /// - `output` is an mpsc sender that will receive decoded shreds.
    ///
    /// Returns a Decoder instance which can be shutdown via `shutdown().await`.
    pub fn spawn(
        mut input: mpsc::Receiver<Vec<u8>>,
        output: mpsc::Sender<DecodedShred>,
    ) -> Self {
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
use serde::Serialize;
use anyhow::{Result, Context, bail};
use bincode;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::message::VersionedMessage;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use solana_entry::entry::Entry;
use anyhow::anyhow;

#[derive(Debug, Serialize, Clone)]
pub struct DecodedTxSummary {
    pub slot: u64,
    pub signature: String,
    pub signers: Vec<String>,
    pub account_keys: Vec<String>,
    pub program_ids: Vec<String>,
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
                let keys = msg.account_keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
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
    if let Ok(v) = bincode::deserialize::<Vec<Vec<u8>>>(raw) {
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
/// 4) length-prefixed frames (u32/u16) -> each frame bincode deserialize VersionedTransaction
/// 5) bincode deserialize as Vec<Entry> (Solana entries) -> for each entry.transactions try deserialize each tx bytes
pub fn decode_raw_txs(slot: u64, raw_tx: &[u8]) -> Result<Vec<DecodedTxSummary>> {
    // 1) Try single tx
    if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(raw_tx) {
        let s = decode_raw_tx(slot, &tx)
            .context("failed to summarize single VersionedTransaction")?;
        return Ok(vec![s]);
    }

    // 2) Try Vec<VersionedTransaction>
    if let Ok(txs) = bincode::deserialize::<Vec<VersionedTransaction>>(raw_tx) {
        let mut summaries = Vec::with_capacity(txs.len());
        for tx in txs.into_iter() {
            let s = decode_raw_tx(slot, &tx)
                .context("failed to summarize one tx inside Vec<VersionedTransaction>")?;
            summaries.push(s);
        }
        return Ok(summaries);
    }

    // 3) Try Vec<Vec<u8>>
    if let Ok(frames) = bincode::deserialize::<Vec<Vec<u8>>>(raw_tx) {
        let mut summaries = Vec::new();
        for frame in frames.into_iter() {
            if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(&frame) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    summaries.push(s);
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 4) length-prefixed frames (u32/u16)
    let frames_lp = extract_tx_byte_slices(raw_tx);
    if !frames_lp.is_empty() {
        let mut summaries = Vec::new();
        for frame in frames_lp.into_iter() {
            if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(&frame) {
                if let Ok(s) = decode_raw_tx(slot, &tx) {
                    summaries.push(s);
                }
            }
        }
        if !summaries.is_empty() {
            return Ok(summaries);
        }
    }

    // 5b) Try deserializing as a stream of Entries (many Entry objects concatenated)
    if let Ok(summaries) = try_deserialize_entry_stream(slot, raw_tx) {
        return Ok(summaries);
    }

    // nothing matched
    Err(anyhow::anyhow!("raw bytes are neither bincode-serialized VersionedTransaction nor Vec<VersionedTransaction> nor recognized framed tx list nor Vec<Entry> nor Entry stream"))
}

/// Try to deserialize a stream of bincode-serialized Entry objects from raw bytes.
/// This handles the case when entries were serialized consecutively (no outer Vec).
fn try_deserialize_entry_stream(slot: u64, raw: &[u8]) -> Result<Vec<DecodedTxSummary>> {
    let mut cur = Cursor::new(raw);
    let mut summaries = Vec::new();

    while (cur.position() as usize) < raw.len() {
        // Attempt to deserialize a single Entry from the current cursor.
        match bincode::deserialize_from::<_, Entry>(&mut cur) {
            Ok(entry) => {
                // entry.transactions in current solana-entry versions is Vec<VersionedTransaction>
                for tx_item in entry.transactions {
                    if let Ok(s) = decode_raw_tx(slot, &tx_item) {
                        summaries.push(s);
                    }
                }
            }
            Err(e) => {
                // If we cannot deserialize the next Entry, bail with context.
                return Err(anyhow!("failed to deserialize Entry from stream: {}", e));
            }
        }
    }

    if summaries.is_empty() {
        Err(anyhow!("no VersionedTransaction decoded from Entry stream"))
    } else {
        Ok(summaries)
    }
}