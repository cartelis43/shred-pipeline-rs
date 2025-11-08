use anyhow::Result;
use base64::Engine;
use serde_json;
use shred_pipeline_rs::decoder::decode_raw_txs;
use std::env;

fn main() -> Result<()> {
    let b64 = env::args()
        .nth(1)
        .expect("usage: debug_entries <base64_payload>");
    let raw = base64::engine::general_purpose::STANDARD.decode(&b64)?;
    match decode_raw_txs(0, &raw) {
        Ok(summaries) => {
            println!("{}", serde_json::to_string_pretty(&summaries)?);
        }
        Err(e) => {
            eprintln!("decode_raw_txs failed: {}", e);
            eprintln!("hex dump (first 256 bytes):");
            for (i, b) in raw.iter().enumerate().take(256) {
                if i % 16 == 0 {
                    eprint!("\n{:04x}: ", i);
                }
                eprint!("{:02x} ", b);
            }
            eprintln!();
        }
    }
    Ok(())
}
