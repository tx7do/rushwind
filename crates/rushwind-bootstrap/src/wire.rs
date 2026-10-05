//! The wire face: the serde shapes the built-in server kinds parse
//! (per-server `edge` toggles, the CORS policy, the domain mounts,
//! bind addresses, duration strings) and their projection onto the
//! HTTP edge's options. Crate-internal: the assembler reads them,
//! the deployment document names them.

use std::net::SocketAddr;
use std::time::Duration;

use rushwind_http::CorsOptions;
use serde::Deserialize;

use super::RoutePackRef;

/// The default for the edge toggles: every default-on middleware on.
pub(crate) fn wire_true() -> bool {
    true
}

/// The per-server HTTP edge wire: the middleware toggles (defaults on),
/// the optional request budget (a second count or a duration string),
/// and the optional CORS policy. Absent fields leave the HTTP edge's
/// defaults in place.
#[derive(Debug, Deserialize)]
pub(crate) struct EdgeWire {
    #[serde(default = "wire_true")]
    pub(crate) request_id: bool,
    #[serde(default = "wire_true")]
    pub(crate) logging: bool,
    #[serde(default = "wire_true")]
    pub(crate) recovery: bool,
    #[serde(default)]
    pub(crate) timeout: Option<DurationWire>,
    #[serde(default)]
    pub(crate) cors: Option<CorsWire>,
}

impl Default for EdgeWire {
    fn default() -> Self {
        Self {
            request_id: true,
            logging: true,
            recovery: true,
            timeout: None,
            cors: None,
        }
    }
}

/// The CORS policy wire: origin, method, header, and expose lists, the
/// credentials flag, the preflight cache duration, and the compat
/// switch choosing the gorilla-parity layer over tower-http. Empty or
/// absent lists fall back to the HTTP edge's documented defaults.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct CorsWire {
    pub(crate) origins: Vec<String>,
    pub(crate) credentials: bool,
    pub(crate) methods: Vec<String>,
    pub(crate) headers: Vec<String>,
    pub(crate) expose: Vec<String>,
    pub(crate) max_age_secs: Option<u64>,
    pub(crate) compat: bool,
}

/// The per-server domain-mount wire: whether the health probes and the
/// Prometheus scrape endpoint mount. Each flag rides its cargo feature
/// and fails the assembly loudly when the feature is compiled out.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct MountsWire {
    pub(crate) health: bool,
    pub(crate) metrics: bool,
}

/// Settings of the built-in `http` server kind.
#[derive(Debug, Deserialize)]
pub(crate) struct HttpServerConfig {
    pub(crate) bind: BindWire,
    #[serde(default)]
    pub(crate) route_packs: Vec<RoutePackRef>,
    #[serde(default)]
    pub(crate) edge: EdgeWire,
    #[serde(default)]
    pub(crate) mounts: MountsWire,
}

/// Settings of the built-in `cron` server kind: the endpoint string the
/// server reports (and the announcement carries), plus the registered
/// jobs to mount.
#[derive(Debug, Deserialize)]
pub(crate) struct CronServerConfig {
    pub(crate) endpoint: String,
    #[serde(default)]
    pub(crate) jobs: Vec<String>,
}

/// Maps the CORS wire onto the HTTP edge's options builder.
pub(crate) fn cors_options_from(wire: &CorsWire) -> CorsOptions {
    let mut options = CorsOptions::default();
    for origin in &wire.origins {
        options = options.with_allow_origin(origin.clone());
    }
    options = options.with_allow_credentials(wire.credentials);
    for method in &wire.methods {
        options = options.with_allow_method(method.clone());
    }
    for header in &wire.headers {
        options = options.with_allow_header(header.clone());
    }
    for header in &wire.expose {
        options = options.with_expose_header(header.clone());
    }
    if let Some(secs) = wire.max_age_secs {
        options = options.with_max_age(Duration::from_secs(secs));
    }
    options
}

/// A listener address wire: the standard `host:port` socket-address
/// form, or the host-any `":port"` form, whose omitted host binds
/// every interface.
#[derive(Debug, Clone, Copy)]
pub struct BindWire(pub SocketAddr);

impl<'de> Deserialize<'de> for BindWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        parse_bind(&text)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom(format!("bind address: {text}")))
            .map(BindWire)
    }
}

/// Parses a listener address: the standard socket-address form, or the
/// host-any `":port"` form (the address `0.0.0.0:port`).
pub(crate) fn parse_bind(text: &str) -> Option<SocketAddr> {
    let text = text.trim();
    if let Some(port_text) = text.strip_prefix(':') {
        let port = port_text.parse::<u16>().ok()?;
        return Some(SocketAddr::from(([0, 0, 0, 0], port)));
    }
    text.parse::<SocketAddr>().ok()
}

/// A duration wire: a plain second count or a duration string
/// (`"300s"`, `"1.5h"`). The string grammar is the `ns`, `us`, `µs`,
/// `ms`, `s`, `m`, `h` unit set of the Go standard library's duration
/// form.
#[derive(Debug, Clone, Copy)]
pub struct DurationWire(pub Duration);

impl<'de> Deserialize<'de> for DurationWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_any(DurationWireVisitor)
            .map(DurationWire)
    }
}

/// The duration wire's visitor: second counts or duration strings.
struct DurationWireVisitor;

impl<'de> serde::de::Visitor<'de> for DurationWireVisitor {
    type Value = Duration;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a second count or a duration string")
    }

    fn visit_u64<E>(self, secs: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Duration::try_from_secs_f64(secs as f64)
            .map_err(|_| E::custom(format!("duration: {secs}s overflows")))
    }

    fn visit_i64<E>(self, secs: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if secs < 0 {
            return Err(E::custom(format!("duration: {secs}s is negative")));
        }
        self.visit_u64(secs as u64)
    }

    fn visit_f64<E>(self, secs: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Duration::try_from_secs_f64(secs)
            .map_err(|_| E::custom(format!("duration: {secs}s is not representable")))
    }

    fn visit_str<E>(self, text: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        parse_duration_string(text)
            .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
            .ok_or_else(|| E::custom(format!("duration string: {text}")))
    }
}

/// Parses a duration string (`"300s"`, `"1.5h"`) into seconds.
pub(crate) fn parse_duration_string(text: &str) -> Option<f64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let (value, unit) = text.split_at(text.find(|c: char| c.is_alphabetic())?);
    let value: f64 = value.parse().ok()?;
    let secs = match unit {
        "ns" => value / 1e9,
        "us" | "µs" => value / 1e6,
        "ms" => value / 1e3,
        "s" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        _ => return None,
    };
    Some(secs)
}
