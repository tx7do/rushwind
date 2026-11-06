//! Tracing contract for RushWind: an OTLP-backed
//! [`TracerProviderBuilder`] doing one-call
//! provider setup, plus W3C trace-context carrier helpers for the
//! server/client span lifecycle.
//!
//! # The core shapes
//!
//! The crate is a thin configuration wrapper over
//! the `opentelemetry` SDK: it configures an OTLP exporter (gRPC
//! or HTTP), a ratio sampler, resource attributes, a batch processor,
//! and exposes the provider plus the W3C `TraceContext` /
//! Baggage propagator.
//!
//! There is no process-global provider registration — the provider
//! is an explicit value the caller hands to the layers that need it
//! (it is `Clone`). [`TracerProviderBuilder`]'s options surface:
//! transport choice, sample ratio, batch and export
//! timeouts, headers, service name/version.
//!
//! Propagation is the W3C `TraceContext` format over a string-map
//! carrier. [`MapCarrier`] adapts a string map to
//! both the extractor and injector halves; [`inject`] and [`extract`]
//! are the two lifecycle halves of span handling for
//! client/outbound and server/inbound spans respectively.
//!
//! # Design notes
//!
//! - No global registration: the provider is returned, not installed.
//! - Baggage propagation is not wired — RushWind callers exchange only
//!   trace context today.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry::global;

/// The std result alias for the builder return.
type StdResult<T, E> = std::result::Result<T, E>;
use opentelemetry::Context;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::Resource;

/// Re-exported so bootstrap-side assembly can name the provider type
/// the builder yields without depending on the SDK crate directly.
pub use opentelemetry_sdk::trace::SdkTracerProvider;

/// The default OTLP export batch timeout.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(5);
/// The default export request timeout.
const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// The default sample ratio (sample everything).
const DEFAULT_SAMPLE_RATIO: f64 = 1.0;
/// The default tracer name.
pub const DEFAULT_TRACER_NAME: &str = "rush-wind";

/// The OTLP transport the exporter uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    /// OTLP over gRPC (the default).
    #[default]
    Grpc,
    /// OTLP over HTTP protobuf.
    Http,
}

/// The OTLP tracer-provider settings.
#[derive(Debug, Clone)]
pub struct OtlpOptions {
    /// The OTLP endpoint as `host:port`. No default — the caller
    /// always names the collector.
    pub endpoint: String,
    /// The transport. Default gRPC.
    pub transport: Transport,
    /// Whether to skip TLS. Default false (TLS on).
    pub insecure: bool,
    /// The trace sample ratio (0.0–1.0). Default 1.0.
    pub sample_ratio: f64,
    /// The batch-export timeout. Default 5 s.
    pub batch_timeout: Duration,
    /// The export request timeout. Default 10 s.
    pub export_timeout: Duration,
    /// The service name resource attribute.
    pub service_name: String,
    /// The service version resource attribute.
    pub service_version: String,
    /// Additional headers for OTLP requests.
    pub headers: Vec<(String, String)>,
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            transport: Transport::Grpc,
            insecure: false,
            sample_ratio: DEFAULT_SAMPLE_RATIO,
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            export_timeout: DEFAULT_EXPORT_TIMEOUT,
            service_name: String::new(),
            service_version: String::new(),
            headers: Vec::new(),
        }
    }
}

/// One call that builds the configured tracing
/// provider. There is no global registration —
/// the caller owns the provider (it is `Clone`) and passes it to the
/// layers that need tracing.
#[derive(Debug, Clone)]
pub struct TracerProviderBuilder {
    options: OtlpOptions,
    /// Overrides the tracer name scopes are created under.
    tracer_name: Option<String>,
}

impl TracerProviderBuilder {
    /// Starts a builder from the OTLP options.
    pub fn new(options: OtlpOptions) -> Self {
        Self {
            options,
            tracer_name: None,
        }
    }

    /// Overrides the tracer name scopes are created under.
    pub fn tracer_name(mut self, name: impl Into<String>) -> Self {
        self.tracer_name = Some(name.into());
        self
    }

    /// The tracer name scopes are created under — the
    /// default unless overridden.
    pub fn resolved_tracer_name(&self) -> String {
        self.tracer_name
            .clone()
            .unwrap_or_else(|| DEFAULT_TRACER_NAME.to_string())
    }

    /// Constructs from the bootstrap factory's settings wire shape: the
    /// OTLP endpoint, the transport (`"http"` the HTTP-protobuf
    /// exporter, anything else the gRPC default), the TLS skip, the
    /// sample ratio, the batch and export timeouts, the service
    /// name/version, and the exporter headers — every field optional,
    /// an absent field the builder default. The only failure is the
    /// settings parse; every network side effect stays with
    /// [`TracerProviderBuilder::build`].
    pub fn from_settings(settings: serde_json::Value) -> StdResult<Self, serde_json::Error> {
        #[derive(Debug, Default, serde::Deserialize)]
        #[serde(default)]
        struct TracerWire {
            endpoint: Option<String>,
            transport: Option<String>,
            insecure: Option<bool>,
            sample_ratio: Option<f64>,
            batch_timeout_ms: Option<u64>,
            export_timeout_ms: Option<u64>,
            service_name: Option<String>,
            service_version: Option<String>,
            headers: Option<HashMap<String, String>>,
        }
        let wire: TracerWire = serde_json::from_value(settings)?;
        let mut options = OtlpOptions::default();
        if let Some(endpoint) = wire.endpoint {
            options.endpoint = endpoint;
        }
        if let Some(transport) = wire.transport {
            options.transport = match transport.as_str() {
                "http" => Transport::Http,
                _ => Transport::Grpc,
            };
        }
        if let Some(insecure) = wire.insecure {
            options.insecure = insecure;
        }
        if let Some(sample_ratio) = wire.sample_ratio {
            options.sample_ratio = sample_ratio;
        }
        if let Some(ms) = wire.batch_timeout_ms {
            options.batch_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = wire.export_timeout_ms {
            options.export_timeout = Duration::from_millis(ms);
        }
        if let Some(name) = wire.service_name {
            options.service_name = name;
        }
        if let Some(version) = wire.service_version {
            options.service_version = version;
        }
        if let Some(headers) = wire.headers {
            options.headers = headers.into_iter().collect();
        }
        Ok(Self::new(options))
    }

    /// Builds the [`SdkTracerProvider`]. Sampler, resource, and batch
    /// processor follow the configured options.
    pub fn build(self) -> StdResult<SdkTracerProvider, opentelemetry_otlp::ExporterBuildError> {
        let options = self.options;
        let sample_ratio = if (0.0..=1.0).contains(&options.sample_ratio) {
            options.sample_ratio
        } else {
            DEFAULT_SAMPLE_RATIO
        };
        let scheme = if options.insecure { "http" } else { "https" };
        let endpoint = format!("{scheme}://{}", options.endpoint);

        let header_map: HashMap<String, String> = options.headers.iter().cloned().collect();
        let exporter = match options.transport {
            Transport::Http => SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint.clone())
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_timeout(options.export_timeout)
                .with_headers(header_map)
                .build()?,
            Transport::Grpc => SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(options.export_timeout)
                .with_metadata(headers_to_metadata(&options.headers)?)
                .build()?,
        };

        let resource = Resource::builder()
            .with_service_name(options.service_name.clone())
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                options.service_version.clone(),
            ))
            .build();

        Ok(SdkTracerProvider::builder()
            .with_sampler(Sampler::TraceIdRatioBased(sample_ratio))
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build())
    }
}

/// The string-map carrier trace context is exchanged through — the
/// wire level of every text-map propagator.
#[derive(Debug, Default, Clone)]
pub struct MapCarrier(pub HashMap<String, String>);

impl MapCarrier {
    /// Builds a carrier from a starter map.
    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    /// Reads one carried value — for tests and callers that need a
    /// specific header after injection.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }
}

impl opentelemetry::propagation::Extractor for MapCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

impl opentelemetry::propagation::Injector for MapCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Converts string header pairs into a tonic metadata map — the
/// gRPC exporter's header carrier. A key or value outside metadata
/// ASCII is a typed build error, not a panic: the pairs come from the
/// deployment's configuration document.
fn headers_to_metadata(
    headers: &[(String, String)],
) -> StdResult<tonic::metadata::MetadataMap, opentelemetry_otlp::ExporterBuildError> {
    let mut metadata = tonic::metadata::MetadataMap::new();
    for (key, value) in headers {
        let parsed: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = value
            .parse()
            .map_err(|_| opentelemetry_otlp::ExporterBuildError::InvalidConfig {
                name: "headers".to_string(),
                reason: format!("header value {value:?} is not valid metadata ASCII"),
            })?;
        let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> =
            key.parse()
                .map_err(|_| opentelemetry_otlp::ExporterBuildError::InvalidConfig {
                    name: "headers".to_string(),
                    reason: format!("header key {key:?} is not valid metadata ASCII"),
                })?;
        metadata.insert(key, parsed);
    }
    Ok(metadata)
}

/// The extract half of the span lifecycle for server/consumer
/// spans: pulls the remote trace context out of `carrier`.
pub fn extract(carrier: &MapCarrier) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// The inject half of the span lifecycle for client/producer
/// spans: writes `context`'s trace context into `carrier`.
pub fn inject(context: &opentelemetry::Context, carrier: &mut MapCarrier) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, carrier);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_wire_parses_and_rejects() {
        // The builder is inert until build(); parsing touches no
        // network and spins no runtime.
        let builder = TracerProviderBuilder::from_settings(serde_json::json!({
            "endpoint": "collector:4317",
            "transport": "http"
        }))
        .expect("wire must parse");
        assert_eq!(builder.resolved_tracer_name(), DEFAULT_TRACER_NAME);
        assert!(
            TracerProviderBuilder::from_settings(serde_json::json!({ "endpoint": 7 })).is_err(),
            "a non-string endpoint must fail the parse"
        );
    }
}
