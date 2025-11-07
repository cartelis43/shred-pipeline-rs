use std::env;

use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use serde::Serialize;

use decoder::DecodedShred;
use receiver::Receiver;

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
    // bind address can be overridden with UDP_BIND env var
    let bind_addr = env::var("UDP_BIND").unwrap_or_else(|_| "0.0.0.0:8001".to_string());

    // channel carrying raw datagrams from Receiver -> Decoder
    let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(1024);

    // channel carrying decoded shreds from Decoder -> this printer
    let (shred_tx, mut shred_rx) = mpsc::channel::<DecodedShred>(1024);

    // create Receiver and start UDP listener
    let mut receiver = Receiver::new(raw_tx.clone());
    if let Err(e) = receiver.spawn_udp_listener(&bind_addr) {
        eprintln!("failed to bind UDP listener to {}: {}", bind_addr, e);
        // continue — user may feed packets via receiver.feed_bytes in other code paths
    } else {
        eprintln!("listening for UDP datagrams on {}", bind_addr);
    }

    // spawn decoder task that reads from raw_rx and sends DecodedShred into shred_tx
    // decoder::Decoder::spawn consumes the receiver side (raw_rx)
    let decoder = decoder::Decoder::spawn(raw_rx, shred_tx);

    // spawn a task that listens for Ctrl+C and triggers shutdown
    let shutdown_handle = tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
    });

    // print decoded shreds as JSONL of DecodedTx-like objects
    while let Some(shred) = shred_rx.recv().await {
        // map the DecodedShred into a JSON-serializable OutputTx (a simplified DecodedTx)
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

        match serde_json::to_string(&out) {
            Ok(line) => {
                println!("{}", line);
            }
            Err(e) => {
                eprintln!("failed to serialize decoded tx: {}", e);
            }
        }

        // cooperative yield to allow shutdown detection
        tokio::select! {
            _ = sleep(Duration::from_millis(0)) => {}
            _ = &mut shutdown_handle.clone() => {
                break;
            }
        }
    }

    // graceful shutdown: stop decoder and receiver
    decoder.shutdown().await;
    receiver.shutdown().await;

    Ok(())
}