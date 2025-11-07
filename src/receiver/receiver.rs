use crate::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use crate::shredstream::SubscribeEntriesRequest;
use tokio::sync::{mpsc::Sender, oneshot};
use tokio::task::JoinHandle;
use tonic::Request;
use anyhow::Context;

/// GrpcReceiver connects to a ShredstreamProxy gRPC endpoint and forwards each
/// `Entry.entries` bytes into the provided mpsc::Sender<Vec<u8>> for the Decoder.
///
/// Note: UDP-related code has been removed; this file now only contains gRPC client logic.
pub struct GrpcReceiver {
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl GrpcReceiver {
    /// Connect to `endpoint` (e.g. "http://127.0.0.1:9000") and start reading the
    /// SubscribeEntries stream, forwarding each Entry.entries to `sender`.
    pub async fn spawn(endpoint: &str, sender: Sender<Vec<u8>>) -> anyhow::Result<Self> {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let mut client = ShredstreamProxyClient::connect(endpoint.to_string())
            .await
            .context("failed to connect to gRPC endpoint")?;

        let req = Request::new(SubscribeEntriesRequest {});
        let response = client
            .subscribe_entries(req)
            .await
            .context("subscribe_entries RPC failed")?;
        let mut stream = response.into_inner();

        let task_sender = sender.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown_rx => {
                        break;
                    }

                    msg = stream.message() => {
                        match msg {
                            Ok(Some(entry)) => {
                                if task_sender.send(entry.entries).await.is_err() {
                                    // downstream closed; stop
                                    break;
                                }
                            }
                            Ok(None) => {
                                // server closed stream
                                break;
                            }
                            Err(e) => {
                                eprintln!("grpc stream error: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Request graceful shutdown and wait for background task to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}