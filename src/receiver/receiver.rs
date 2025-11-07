use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc::Sender, oneshot};
use tokio::task::JoinHandle;

/// Async Receiver for the Shred Pipeline.
///
/// - listens on a UDP socket and forwards received datagrams into the provided mpsc::Sender<Vec<u8>>
/// - accepts an abstract async stream (for gRPC SubscribeEntries) and forwards items into the same channel
///
/// This file purposely does not depend on any generated gRPC types; instead, use `spawn_grpc_handler`
/// with the concrete stream type produced by your tonic/grpc client (the stream must yield Result<Vec<u8>, E>).
pub struct Receiver {
    sender: Sender<Vec<u8>>,
    // used to signal spawned tasks to stop if desired
    shutdown_tx: Option<oneshot::Sender<()>>,
    // keep a handle to the task that listens on UDP (optional)
    udp_handle: Option<JoinHandle<()>>,
    // keep a handle to the task that handles grpc stream (optional)
    grpc_handle: Option<JoinHandle<()>>,
}

impl Receiver {
    /// Create a new Receiver that will forward incoming bytes to `sender`.
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        Receiver {
            sender,
            shutdown_tx: None,
            udp_handle: None,
            grpc_handle: None,
        }
    }

    /// Spawn an asynchronous UDP listener bound to `addr` (e.g. "0.0.0.0:8001").
    ///
    /// Each received datagram is forwarded as a Vec<u8> into the configured mpsc sender.
    /// Returns a JoinHandle; the task will stop when the returned oneshot sender (stored inside `self`)
    /// is triggered or if the UDP socket returns a fatal error.
    pub fn spawn_udp_listener(&mut self, addr: &str) -> std::io::Result<()> {
        let socket = UdpSocket::bind(addr)?;
        let mut shutdown_rx = {
            let (tx, rx) = oneshot::channel();
            self.shutdown_tx = Some(tx);
            rx
        };
        let mut sender = self.sender.clone();

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            loop {
                tokio::select! {
                    res = socket.recv_from(&mut buf) => {
                        match res {
                            Ok((n, _peer)) => {
                                let mut dat = Vec::with_capacity(n);
                                dat.extend_from_slice(&buf[..n]);
                                // ignore send errors (receiver closed)
                                if let Err(_e) = sender.send(dat).await {
                                    // receiver side closed, stop the task
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("udp recv error: {e}");
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        // graceful shutdown requested
                        break;
                    }
                }
            }
        });

        self.udp_handle = Some(handle);
        Ok(())
    }

    /// Spawn a handler around an abstract gRPC stream.
    ///
    /// The stream must produce Result<Vec<u8>, E> items (where Vec<u8> contains the payload bytes).
    /// Typical usage with tonic:
    ///   let mut stream = client.subscribe_entries(request).await?.into_inner();
    ///   receiver.spawn_grpc_handler(stream);
    pub fn spawn_grpc_handler<S, E>(&mut self, stream: S)
    where
        S: futures::Stream<Item = Result<Vec<u8>, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut stream = stream;
        let mut sender = self.sender.clone();

        let handle = tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(bytes) => {
                        if let Err(_e) = sender.send(bytes).await {
                            // channel closed, stop processing
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("grpc stream error: {}", e);
                        // decide to break on first error; caller can recreate stream if needed
                        break;
                    }
                }
            }
        });

        self.grpc_handle = Some(handle);
    }

    /// Request a graceful shutdown of spawned tasks and wait for them to finish.
    pub async fn shutdown(mut self) {
        // trigger shutdown signal if present
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.udp_handle.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.grpc_handle.take() {
            let _ = handle.await;
        }
    }

    /// Convenience: push raw bytes into the pipeline (useful for tests or local feeding).
    pub async fn feed_bytes(&self, bytes: Vec<u8>) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<u8>>> {
        self.sender.send(bytes).await
    }
}

// Note: we intentionally do not implement Drop to block on async joins; call `shutdown().await` from async context.