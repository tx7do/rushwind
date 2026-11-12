//! The shared test corpus: a hand-written unary echo service (the
//! shape tonic-build would generate, narrowed to one raw-bytes
//! method) and the raw h2 client that talks to it — no proto corpus,
//! no codegen, the wire framing written out so the pins can see every
//! byte.

#![allow(dead_code)]

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use tonic::body::Body;
use tonic::service::Routes;
use tower::Service;

/// The service name the routes table matches against.
pub const ECHO_NAME: &str = "test.echo.Echo";

/// A unary echo service: the request message's bytes come back as the
/// response message. A request whose body is not a well-formed gRPC
/// frame answers `grpc-status: 13` (INTERNAL) with no message.
#[derive(Debug, Clone, Default)]
pub struct EchoService;

impl tonic::server::NamedService for EchoService {
    const NAME: &'static str = ECHO_NAME;
}

impl Service<http::Request<Body>> for EchoService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            let message = match extract_message(request.into_body()).await {
                Ok(message) => message,
                Err(status) => return Ok(error_response(status)),
            };
            let mut framed = Vec::with_capacity(5 + message.len());
            framed.push(0u8);
            framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
            framed.extend_from_slice(&message);
            Ok(ok_response(Bytes::from(framed)))
        })
    }
}

/// Builds the routes table carrying the echo service.
pub fn echo_routes() -> Routes {
    Routes::new(EchoService)
}

/// The request body's gRPC frame minus its 5-byte prefix.
async fn extract_message(body: Body) -> Result<Bytes, &'static str> {
    let bytes = BodyExt::collect(body)
        .await
        .map_err(|_| "failed to read the request body")?
        .to_bytes();
    if bytes.len() < 5 {
        return Err("request body is not a gRPC frame");
    }
    let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    if bytes.len() != 5 + length {
        return Err("request frame length disagrees with the body");
    }
    Ok(Bytes::copy_from_slice(&bytes[5..]))
}

fn ok_response(framed: Bytes) -> http::Response<Body> {
    let stream = stream_of([
        Ok(futures_core_frame::data(framed)),
        Ok(futures_core_frame::trailers(grpc_status(0))),
    ]);
    respond(stream)
}

fn error_response(message: &'static str) -> http::Response<Body> {
    let trailers = {
        let mut map = http::HeaderMap::new();
        map.insert("grpc-status", http::HeaderValue::from_static("13"));
        map.insert("grpc-message", http::HeaderValue::from_static(message));
        map
    };
    let stream = stream_of([Ok(futures_core_frame::trailers(trailers))]);
    respond(stream)
}

fn grpc_status(status: u8) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    map.insert(
        "grpc-status",
        http::HeaderValue::from_str(&status.to_string()).expect("numeric status"),
    );
    map
}

fn respond(
    stream: impl futures::Stream<Item = Result<http_body::Frame<Bytes>, Infallible>> + Send + 'static,
) -> http::Response<Body> {
    http::Response::builder()
        .status(http::StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(Body::new(StreamBody::new(stream)))
        .expect("static response shape")
}

/// Narrow re-exports so the stream literals above stay one-liners.
mod futures_core_frame {
    use super::Bytes;

    pub fn data(bytes: Bytes) -> http_body::Frame<Bytes> {
        http_body::Frame::data(bytes)
    }
    pub fn trailers(map: http::HeaderMap) -> http_body::Frame<Bytes> {
        http_body::Frame::trailers(map)
    }
}

fn stream_of<I>(items: I) -> impl futures::Stream<Item = I::Item>
where
    I: IntoIterator + Send + 'static,
{
    futures::stream::iter(items)
}

/// The raw h2 client: sends one framed unary request, returns the
/// response message bytes and the grpc-status trailer value.
pub async fn echo_call(
    address: std::net::SocketAddr,
    message: &[u8],
) -> Result<(Vec<u8>, String), String> {
    let tcp = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|e| e.to_string())?;
    let (mut client, connection) = h2::client::handshake(tcp)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{address}/{ECHO_NAME}/Echo"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .expect("static request");
    let (response, mut send) = client
        .send_request(request, false)
        .map_err(|e| e.to_string())?;

    let mut frame = Vec::with_capacity(5 + message.len());
    frame.push(0u8);
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend_from_slice(message);
    send.send_data(Bytes::from(frame), true)
        .map_err(|e| format!("send_data: {e}"))?;

    let response = response.await.map_err(|e| e.to_string())?;
    let status = response
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        let size = chunk.len();
        bytes.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(size)
            .map_err(|e| e.to_string())?;
    }
    let trailers = body
        .trailers()
        .await
        .map_err(|e| e.to_string())?
        .ok_or("no trailers")?;
    let grpc_status = trailers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let _ = status;

    if bytes.len() < 5 {
        return Err("response is not a gRPC frame".to_string());
    }
    let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    Ok((bytes[5..5 + length].to_vec(), grpc_status))
}
