use crate::shredstream::shred_stream_client::ShredStreamClient;
use crate::shredstream::Empty;
use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc::Sender, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::Status;
use anyhow::Context;

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
    ///
    /// Note: use std::net::UdpSocket::bind + tokio::net::UdpSocket::from_std to avoid
    /// ambiguity across runtime versions.
    pub fn spawn_udp_listener(&mut self, addr: &str) -> std::io::Result<()> {
        // bind using std to avoid async/sync ambiguity, then convert to tokio socket
        let std_sock = std::net::UdpSocket::bind(addr)?;
        std_sock.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_sock)?;

        let mut shutdown_rx = {
            let (tx, rx) = oneshot::channel();
            self.shutdown_tx = Some(tx);
            rx
        };
        let sender = self.sender.clone();

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
        S: futures::Stream<Item = Result<Vec<u8>, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut stream = stream;
        let sender = self.sender.clone();

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

/// GrpcReceiver connects to a ShredStream gRPC endpoint and forwards Entry.payload
/// into the provided mpsc::Sender<Vec<u8>> for the Decoder layer to consume.
///
/// Usage:
///   let (tx, rx) = tokio::sync::mpsc::channel(1024);
///   let receiver = GrpcReceiver::spawn("http://127.0.0.1:9000", tx).await?;
pub struct GrpcReceiver {
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl GrpcReceiver {
    /// Connect to `endpoint` (e.g. "http://127.0.0.1:9000") and start reading the
    /// SubscribeEntries stream, forwarding each Entry.payload to `sender`.
    pub async fn spawn(endpoint: &str, sender: Sender<Vec<u8>>) -> anyhow::Result<Self> {
        // Create shutdown channel for the background task
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        // Connect to tonic channel
        let channel = Channel::from_shared(endpoint.to_string())
            .context("invalid GRPC endpoint URL")?
            .connect()
            .await
            .context("failed to connect to gRPC endpoint")?;

        let mut client = ShredStreamClient::new(channel);

        // create request and subscribe
        let req = tonic::Request::new(Empty {});

        let mut stream = client
            .subscribe_entries(req)
            .await
            .context("subscribe_entries RPC failed")?
            .into_inner();

        // clone sender into task
        let task_sender = sender.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown_rx => {
                        // shutdown requested
                        break;
                    }

                    msg = stream.message() => {
                        match msg {
                            Ok(Some(entry)) => {
                                // forward payload bytes (best-effort)
                                if let Err(_e) = task_sender.send(entry.payload).await {
                                    // downstream closed -> stop
                                    break;
                                }
                            }
                            Ok(None) => {
                                // server closed stream
                                break;
                            }
                            Err(e) => {
                                // log error and stop
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