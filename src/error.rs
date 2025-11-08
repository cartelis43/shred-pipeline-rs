// Shared error types for the Shred Pipeline.
// Integrates thiserror for ergonomic error definitions and provides conversions
// from common error types (io, mpsc send, anyhow).
use thiserror::Error;

/// Top-level error type for the shred pipeline layers.
#[derive(Debug, Error)]
pub enum ShredPipelineError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("invalid packet: {0}")]
    InvalidPacket(String),

    #[error("channel send error")]
    ChannelSend,

    #[error("gRPC error: {0}")]
    Grpc(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ShredPipelineError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ShredPipelineError::ChannelSend
    }
}

impl From<&str> for ShredPipelineError {
    fn from(s: &str) -> Self {
        ShredPipelineError::Decode(s.to_string())
    }
}

impl From<String> for ShredPipelineError {
    fn from(s: String) -> Self {
        ShredPipelineError::Decode(s)
    }
}
