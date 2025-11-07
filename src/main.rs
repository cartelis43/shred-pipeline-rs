use std::env;
use std::fs;
use std::path::Path;

use tokio::sync::mpsc;
use serde::Serialize;

use shred_pipeline_rs::decoder::{DecodedShred, decode_raw_txs};
use shred_pipeline_rs::receiver::GrpcReceiver;

#[derive(Serialize)]
struct OutputTx {
    signature: String,
    slot: Option<u64>,
    index: Option<u32>,
    success: Option<bool>,
    fee: Option<u64>,
    instructions: Vec<serde_json::Value>,
    accounts: Vec<serde_json::Value>,
    tags: Vec<(String, String)>,
    // replaced raw_tx with decoded summary fields below when available
    signature_raw: String,
    signers: Vec<String>,
    account_keys: Vec<String>,
    program_ids: Vec<String>,
    meta: std::collections::HashMap<String, String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let endpoint = env::var("GRPC_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());

    // Raw byte channel from Receiver -> Decoder
    let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(1024);

    // Decoded shreds channel from Decoder -> printer
    let (shred_tx, mut shred_rx) = mpsc::channel::<DecodedShred>(1024);

    // start gRPC receiver
    let receiver = GrpcReceiver::spawn(&endpoint, raw_tx).await?;
    eprintln!("connected to gRPC endpoint: {}", endpoint);

    // spawn decoder task
    let decoder = shred_pipeline_rs::decoder::Decoder::spawn(raw_rx, shred_tx);

    // Print JSONL output until stream closed or Ctrl+C
    loop {
        tokio::select! {
            maybe = shred_rx.recv() => {
                match maybe {
                    Some(shred) => {
                        // Try to decode the payload as one-or-many transactions.
                        match decode_raw_txs(shred.slot, &shred.payload) {
                            Ok(summaries) => {
                                for summary in summaries {
                                    let out = OutputTx {
                                        signature: summary.signature.clone(),
                                        slot: Some(summary.slot),
                                        index: Some(shred.index),
                                        success: None,
                                        fee: None,
                                        instructions: Vec::new(),
                                        accounts: Vec::new(),
                                        tags: Vec::new(),
                                        signature_raw: summary.signature,
                                        signers: summary.signers,
                                        account_keys: summary.account_keys,
                                        program_ids: summary.program_ids,
                                        meta: std::collections::HashMap::new(),
                                    };
                                    if let Ok(line) = serde_json::to_string(&out) {
                                        println!("{}", line);
                                    }
                                }
                            }
                            Err(e) => {
                                // Log error + write full base64 payload to a file for inspection
                                let b64 = base64::encode(&shred.payload);
                                let filename = format!("/tmp/shred_payload_slot{}_index{}.b64", shred.slot, shred.index);
                                // write only once (ignore errors)
                                if !Path::new(&filename).exists() {
                                    let _ = fs::write(&filename, &b64);
                                }
                                eprintln!(
                                    "decode_raw_tx error slot={} index={}: {} payload_len={} written={}",
                                    shred.slot,
                                    shred.index,
                                    e,
                                    shred.payload.len(),
                                    filename
                                );
                            }
                        }
                    }
                    None => break,
                }
            }

            _ = tokio::signal::ctrl_c() => {
                eprintln!("shutdown signal received");
                break;
            }
        }
    }

    // graceful shutdown
    decoder.shutdown().await;
    receiver.shutdown().await;

    Ok(())
}