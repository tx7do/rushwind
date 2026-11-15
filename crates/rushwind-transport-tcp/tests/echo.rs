//! The shared test corpus: an echo server builder (the raw TCP session
//! echoes every byte back) and a blocking client helper.

use std::net::SocketAddr;

use rushwind_transport_tcp::TcpServer;

/// Builds an echo server bound to `addr`: every admitted session reads
/// bytes and echoes them back until the peer closes.
pub fn echo_server(addr: SocketAddr) -> TcpServer {
    TcpServer::builder(addr)
        .session_handler(|mut session| async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buffer = [0u8; 256];
            loop {
                let read = match session.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if session.write_all(&buffer[..read]).await.is_err() {
                    return;
                }
            }
        })
        .build()
        .expect("bind works")
}

/// Connects to `addr`, sends `payload`, and expects the echo back.
pub async fn echo_round_trip(addr: SocketAddr, payload: &[u8]) -> std::io::Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await?;
    socket.write_all(payload).await?;
    let mut buffer = vec![0u8; payload.len() + 64];
    let read = socket.read(&mut buffer).await?;
    Ok(buffer[..read.min(payload.len())].to_vec())
}
