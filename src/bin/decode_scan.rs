use std::env;
use anyhow::Result;
use base64::Engine;
use shred_pipeline_rs::decoder::decode_raw_txs;
use serde_json::json;

fn main() -> Result<()> {
    let b64 = env::args().nth(1).expect("usage: decode_scan <base64_payload>");
    let raw = base64::engine::general_purpose::STANDARD.decode(&b64)?;
    let len = raw.len();
    println!("payload len = {}", len);

    // try offsets 0..min(len, 512) first (fast); if nothing found, increase range
    let max_start = std::cmp::min(len.saturating_sub(1), 1024);
    for start in 0..=max_start {
        let slice = &raw[start..];
        match decode_raw_txs(0, slice) {
            Ok(summaries) => {
                println!("SUCCESS at offset {} (slice len={})", start, slice.len());
                println!("{}", serde_json::to_string_pretty(&summaries)?);
                return Ok(());
            }
            Err(_e) => {
                // skip printing every error to keep output compact
            }
        }
    }

    // if no success in first window, try larger step scan (step 4) for whole payload
    for start in (0..len).step_by(4) {
        let slice = &raw[start..];
        if let Ok(summaries) = decode_raw_txs(0, slice) {
            println!("SUCCESS at offset {} (slice len={})", start, slice.len());
            println!("{}", serde_json::to_string_pretty(&summaries)?);
            return Ok(());
        }
    }

    eprintln!("no decode success scanning offsets up to {}", len);
    // print a hex preview for manual inspection
    eprintln!("hex preview (first 128 bytes):");
    for (i, b) in raw.iter().enumerate().take(128) {
        if i % 16 == 0 {
            eprint!("\n{:04x}: ", i);
        }
        eprint!("{:02x} ", b);
    }
    eprintln!();
    Ok(())
}