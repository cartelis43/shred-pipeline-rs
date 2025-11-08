use std::env;
use tokio::sync::mpsc;

use shred_pipeline_rs::decoder::{decode_raw_txs, DecodedShred, DecodedTxSummary, Decoder};
use shred_pipeline_rs::receiver::GrpcReceiver;

fn format_list(values: &[String]) -> String {
    values.join(", ")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let endpoint =
        env::var("GRPC_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());

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
                        match decode_raw_txs(shred.slot, &shred.payload) {
                            Ok(summaries) => {
                                for summary in summaries {
                                    log_summary(&summary, shred.index);
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "decode_raw_txs_failed slot={} index={} err={}",
                                    shred.slot,
                                    shred.index,
                                    e
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

fn log_summary(summary: &DecodedTxSummary, shred_index: u32) {
    println!(
        "decoded_tx slot={} index={} signature={} signers=[{}] account_keys=[{}] program_ids=[{}]",
        summary.slot,
        shred_index,
        summary.signature,
        format_list(&summary.signers),
        format_list(&summary.account_keys),
        format_list(&summary.program_ids),
    );
}
