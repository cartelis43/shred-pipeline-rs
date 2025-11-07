// This file serves as the main library entry point for the Shred Pipeline project.
// It declares the public interface of the library and includes module declarations for the receiver and decoder layers.

pub mod receiver;
pub mod decoder;
pub mod types;
pub mod error;

// generated proto module (from build.rs / tonic-build)
pub mod shredstream {
    tonic::include_proto!("shredstream");
}