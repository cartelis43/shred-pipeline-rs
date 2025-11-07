use std::env;

use tokio::sync::mpsc;
use serde::Serialize;

use shred_pipeline_rs::decoder::Decoder;
use shred_pipeline_rs::receiver::GrpcReceiver;
use shred_pipeline_rs::decoder::DecodedShred;

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
    raw_tx: Vec<u8>,
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
    let decoder = Decoder::spawn(raw_rx, shred_tx);

    // Print JSONL output until stream closed or Ctrl+C
    loop {
        tokio::select! {
            maybe = shred_rx.recv() => {
                match maybe {
                    Some(shred) => {
                        let out = OutputTx {
                            signature: String::new(),
                            slot: Some(shred.slot),
                            index: Some(shred.index),
                            success: None,
                            fee: None,
                            instructions: Vec::new(),
                            accounts: Vec::new(),
                            tags: Vec::new(),
                            raw_tx: shred.payload,
                            meta: std::collections::HashMap::new(),
                        };
                        if let Ok(line) = serde_json::to_string(&out) {
                            println!("{}", line);
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