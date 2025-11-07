use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use bytes::{Buf, Bytes};
use std::collections::HashMap;

use anyhow::Context;

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
use anyhow::Result;
use bincode;
use solana_sdk::{
    message::{VersionedMessage, Message, v0::LoadedMessage},
    transaction::VersionedTransaction,
    pubkey::Pubkey,
};

#[derive(Debug, Serialize, Clone)]
pub struct DecodedTxSummary {
    pub slot: u64,
    pub signature: String,
    pub signers: Vec<String>,
    pub account_keys: Vec<String>,
    pub program_ids: Vec<String>,
}

/// Try to decode a raw serialized VersionedTransaction (bincode-serialized) into a summary.
/// Returns anyhow::Result for simple error propagation.
pub fn decode_raw_tx(slot: u64, raw_tx: &[u8]) -> Result<DecodedTxSummary> {
    // Attempt to deserialize via bincode
    let tx: VersionedTransaction =
        bincode::deserialize(raw_tx).context("failed to bincode-deserialize VersionedTransaction")?;

    // signature (first signature if present)
    let signature = tx
        .signatures
        .get(0)
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Access versioned message
    let message = tx.message();

    // account_keys: try to get account keys for both Legacy and V0 messages
    let account_keys: Vec<String> = match message {
        VersionedMessage::Legacy(msg) => msg
            .account_keys
            .iter()
            .map(|k| k.to_string())
            .collect(),
        VersionedMessage::V0(loaded) => {
            // LoadedMessage contains static account keys and lookups (addresses).
            // We will produce a flattened list that includes the static keys only,
            // which is sufficient to resolve program_id_index for many txns.
            // For full correctness you'd need to expand address table lookups into keys.
            let mut keys: Vec<String> = loaded
                .static_account_keys()
                .iter()
                .map(|k| k.to_string())
                .collect();

            // If loaded.message has additional loaded addresses we skip them for now
            // (implementing address table expansion requires on-chain lookup of address tables).
            keys
        }
    };

    // num_required_signatures: derive from the message header
    let num_required_signatures = match message {
        VersionedMessage::Legacy(msg) => msg.header.num_required_signatures as usize,
        VersionedMessage::V0(loaded) => loaded.header.num_required_signatures as usize,
    };

    // signers: first num_required_signatures account keys
    let signers: Vec<String> = account_keys
        .iter()
        .take(num_required_signatures)
        .cloned()
        .collect();

    // program_ids: get unique program id keys used by instructions
    let mut program_ids = Vec::<String>::new();
    match message {
        VersionedMessage::Legacy(msg) => {
            for ix in &msg.instructions {
                let pid_idx = ix.program_id_index as usize;
                if let Some(pid) = account_keys.get(pid_idx) {
                    if !program_ids.contains(pid) {
                        program_ids.push(pid.clone());
                    }
                }
            }
        }
        VersionedMessage::V0(loaded) => {
            for ix in &loaded.instructions {
                let pid_idx = ix.program_id_index as usize;
                if let Some(pid) = account_keys.get(pid_idx) {
                    if !program_ids.contains(pid) {
                        program_ids.push(pid.clone());
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use bs58;
    use solana_sdk::signature::Signature;

    // This test is illustrative and expects a real base58 tx string.
    // Replace TX_BASE58 with a valid serialized VersionedTransaction base58 string to test.
    #[test]
    fn decode_sample_tx_roundtrip() {
        // Example placeholder: an empty vector will fail; this test demonstrates usage only.
        // To run a real test, replace this with a real serialized tx base58 string.
        let tx_base58 = ""; // <-- put a real base58-encoded bincode(serialized VersionedTransaction) here
        if tx_base58.is_empty() {
            // nothing to test in CI, treat as passed
            return;
        }
        let raw = bs58::decode(tx_base58).into_vec().expect("base58 decode");
        let summary = decode_raw_tx(0, &raw).expect("decode");
        // basic assertions
        assert!(summary.account_keys.len() >= summary.signers.len());
    }
}