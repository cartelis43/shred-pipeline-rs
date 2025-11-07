use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use bytes::{Buf, Bytes};
use std::collections::HashMap;

use anyhow::{Context, bail};

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

/// Try multiple strategies to obtain one-or-many VersionedTransaction objects from raw bytes.
///
/// Strategies:
/// 1) bincode deserialize as a single VersionedTransaction
/// 2) bincode deserialize as Vec<VersionedTransaction>
///
/// Returns Vec<DecodedTxSummary> on success, or an error describing why none matched.
pub fn decode_raw_txs(slot: u64, raw_tx: &[u8]) -> Result<Vec<DecodedTxSummary>> {
    // Try single tx
    match bincode::deserialize::<VersionedTransaction>(raw_tx) {
        Ok(tx) => {
            let s = decode_raw_tx(slot, &tx)
                .context("failed to summarize single VersionedTransaction")?;
            return Ok(vec![s]);
        }
        Err(_) => {
            // fallthrough to try vec
        }
    }

    // Try Vec<VersionedTransaction>
    match bincode::deserialize::<Vec<VersionedTransaction>>(raw_tx) {
        Ok(txs) => {
            let mut summaries = Vec::with_capacity(txs.len());
            for tx in txs.iter() {
                match decode_raw_tx(slot, tx) {
                    Ok(s) => summaries.push(s),
                    Err(e) => {
                        // if one tx in the vec fails (e.g. V0), return error for clarity
                        return Err(e).context("failed to summarize one tx inside Vec<VersionedTransaction>");
                    }
                }
            }
            return Ok(summaries);
        }
        Err(e) => {
            // both attempts failed: return original error
            return Err(e).context("raw bytes are neither bincode-serialized VersionedTransaction nor Vec<VersionedTransaction>");
        }
    }
}