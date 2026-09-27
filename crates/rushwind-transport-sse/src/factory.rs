//! The bootstrap factory: the events router (handler + preflight under
//! the wire's address and path) assembled into a lifecycle server.

use std::sync::Arc;

use serde::Deserialize;

use rushwind_bootstrap::{BindWire, BootstrapError, BoxFuture, RouteInput};
use rushwind_transport::Server;

use crate::handler::{events, events_preflight, Gate, SseApp};

/// The sse transport wire (the factory's settings node): the listener
/// address (the standard form or the host-any `":port"` form) and the
/// events route path. Missing fields fall back to the deployment's
/// default wire.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct SseWire {
    #[serde(default)]
    pub addr: Option<BindWire>,
    #[serde(default)]
    pub path: Option<String>,
}

/// The sse transport factory: assembles the events router (the handler
/// and its preflight under the wire's address and path — an empty path
/// mounts `/`, exact match only; the reference mux's `/` default is a
/// catch-all prefix, unreachable while the embedded document pins
/// `/events`) into a lifecycle server. `default` carries the
/// deployment's listener address and path for the settings node's
/// missing fields.
pub fn factory<A: Gate>(
    app: Arc<SseApp<A>>,
    default: SseWire,
) -> impl Fn(
    serde_json::Value,
    RouteInput,
) -> BoxFuture<'static, Result<Arc<dyn Server>, BootstrapError>>
       + Send
       + Sync
       + 'static {
    move |settings, _input| {
        let app = Arc::clone(&app);
        let default = default.clone();
        Box::pin(async move {
            let wire = match settings_or_wire(settings)? {
                Some(wire) => wire,
                None => default.clone(),
            };
            let addr = wire
                .addr
                .or(default.addr)
                .ok_or_else(|| BootstrapError::Config("sse wire: no listener address".into()))?;
            let path = wire
                .path
                .or(default.path.clone())
                .unwrap_or_else(|| "/events".into());
            let path = if path.is_empty() { "/" } else { path.as_str() };
            let router = axum::Router::new().route(
                path,
                axum::routing::get(events::<A>)
                    .options(events_preflight)
                    .with_state(app),
            );
            let server = rushwind_transport_axum::AxumServer::new(addr.0, router)?;
            Ok(Arc::new(server) as Arc<dyn Server>)
        })
    }
}

/// The settings node, `None` when the node itself is absent (the
/// deployment's default wire answers); a present node's missing fields
/// ride [`SseWire`]'s own `None`s.
fn settings_or_wire(settings: serde_json::Value) -> Result<Option<SseWire>, BootstrapError> {
    if settings.is_null() {
        return Ok(None);
    }
    serde_json::from_value(settings)
        .map(Some)
        .map_err(|e| BootstrapError::Config(format!("sse wire: {e}")))
}
