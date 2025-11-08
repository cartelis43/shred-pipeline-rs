// This file serves as the main library entry point for the Shred Pipeline project.
// It declares the public interface of the library and includes module declarations for the receiver and decoder layers.

pub mod decoder;
pub mod error;
pub mod receiver;
pub mod types;

// generated proto module (from build.rs / tonic-build)
pub mod shredstream {
    tonic::include_proto!("shredstream");
}

// Re-export the DecodedTxSummary so main.rs can import it if desired
pub use crate::decoder::DecodedTxSummary;
