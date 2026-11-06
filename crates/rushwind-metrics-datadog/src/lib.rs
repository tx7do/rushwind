//! Datadog engine for the Rust metrics contract.
//!
//! Metrics are rendered as **DogStatsD line protocol** and sent over
//! UDP to a local Datadog Agent, which forwards them to Datadog's
//! backend:
//!
//! ```text
//! <namespace>.<name>:<value>|<type>|#<tag:k:v,...>|@<rate>
//! ```
//!
//! Counter lines carry `|c` with the value cast to integer —
//! `int64(value)` truncation, kept. Histograms carry `|h`, gauges
//! `|g`, both with the full float. Tags follow the contract's
//! canonical (sorted) label order. The sample-rate suffix `|@rate`
//! appears only when the rate is below 1.0.
//!
//! UDP is fire-and-forget: a missing or dead agent loses samples
//! silently, which is the DogStatsD contract, not a bug. Buffering
//! ([`DogStatsDOptions::with_buffer_size`]) batches up to N lines into
//! one datagram, flushed on overflow, on the flush period, and on
//! drop.
//!
//! # Design notes
//!
//! - the line protocol is hand-rolled — a UDP socket and string framing
//!   is the whole driver surface
//! - tags are sorted, the contract's canonical order
//! - shutdown rides `Drop`, which flushes and closes

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_metrics::{canonical_labels, Metrics};

/// The default DogStatsD agent address.
pub const DEFAULT_ADDRESS: &str = "127.0.0.1:8125";

/// The default flush period for buffered sends.
pub const DEFAULT_FLUSH_PERIOD: Duration = Duration::from_millis(100);

/// Builder for [`DatadogMetrics`].
pub struct DogStatsDOptions {
    address: String,
    namespace: Option<String>,
    buffer_size: Option<usize>,
    flush_period: Option<Duration>,
    rate: f64,
}

impl Default for DogStatsDOptions {
    /// The defaults: loopback agent, no namespace, no buffering,
    /// every sample sent.
    fn default() -> Self {
        Self {
            address: DEFAULT_ADDRESS.to_string(),
            namespace: None,
            buffer_size: None,
            flush_period: None,
            rate: 1.0,
        }
    }
}

impl DogStatsDOptions {
    /// Options pointing at the loopback agent with no buffering.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the DogStatsD agent address (`host:port`).
    pub fn with_address(mut self, address: &str) -> Self {
        self.address = address.to_string();
        self
    }

    /// Sets a prefix prepended to every metric name
    /// (`<namespace>.<name>`).
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }

    /// Enables buffering: up to `size` lines batch into one datagram.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Sets the buffered-send flush interval.
    pub fn with_flush_period(mut self, period: Duration) -> Self {
        self.flush_period = Some(period);
        self
    }

    /// Sets the sample rate (0.0 to 1.0); rates below 1.0 append the
    /// DogStatsD `|@rate` suffix.
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.rate = rate.clamp(0.0, 1.0);
        self
    }
}

/// The shared buffer behind buffered sends, plus the flush thread's
/// stop flag.
struct Buffer {
    lines: Mutex<Vec<String>>,
    stop: AtomicBool,
}

/// The DogStatsD-backed metrics provider.
pub struct DatadogMetrics {
    socket: UdpSocket,
    namespace: Option<String>,
    rate: f64,
    buffer: Option<(Arc<Buffer>, usize)>,
    /// Set when buffering is on; Drop flushes and stops the flush
    /// thread through it.
    flush_thread: Option<Arc<Buffer>>,
}

impl DatadogMetrics {
    /// Builds the provider from its options. A UDP "connection" does
    /// not reach the agent, so an unreachable address surfaces only as
    /// lost samples, never as a construction failure — the only
    /// fallible step is binding a local socket.
    pub fn new(options: DogStatsDOptions) -> std::io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect(&options.address)?;

        // The flush thread gets its own handle before the provider
        // takes the original socket.
        let thread_socket = socket.try_clone()?;
        let mut provider = Self {
            socket,
            namespace: options.namespace,
            rate: options.rate,
            buffer: None,
            flush_thread: None,
        };

        if let Some(size) = options.buffer_size {
            if size > 0 {
                let buffer = Arc::new(Buffer {
                    lines: Mutex::new(Vec::with_capacity(size)),
                    stop: AtomicBool::new(false),
                });
                let period = options.flush_period.unwrap_or(DEFAULT_FLUSH_PERIOD);
                let thread_buffer = Arc::clone(&buffer);
                std::thread::spawn(move || {
                    // The plain sleep-loop flusher: a missed beat just
                    // delays a batch, it cannot lose one.
                    while !thread_buffer.stop.load(Ordering::Relaxed) {
                        std::thread::sleep(period);
                        flush_buffer(&thread_buffer, |payload| {
                            let _ = thread_socket.send(payload.as_bytes());
                        });
                    }
                    // The final tick on stop: drain whatever is left.
                    flush_buffer(&thread_buffer, |payload| {
                        let _ = thread_socket.send(payload.as_bytes());
                    });
                });
                provider.buffer = Some((Arc::clone(&buffer), size));
                provider.flush_thread = Some(buffer);
            }
        }

        Ok(provider)
    }

    /// Renders one DogStatsD line: `name:value|type` plus the tag and
    /// sample-rate suffixes.
    fn render(&self, name: &str, value: &str, kind: &str, labels: &[(&str, &str)]) -> String {
        let full = match &self.namespace {
            Some(namespace) => format!("{namespace}.{name}"),
            None => name.to_string(),
        };
        let mut line = format!("{full}:{value}|{kind}");
        let tags = canonical_labels(labels);
        if !tags.is_empty() {
            // Tag keys/values pass through the DogStatsD sanitizers:
            // a `\n` in an attacker-influenced value would forge a
            // second metric line, and `|`/`,`/`#` would split the tag
            // section of the packet.
            let joined: Vec<String> = tags
                .iter()
                .map(|(k, v)| format!("{}:{}", sanitize_tag(k), sanitize_tag(v)))
                .collect();
            line.push_str(&format!("|#{}", joined.join(",")));
        }
        if self.rate < 1.0 {
            line.push_str(&format!("|@{}", self.rate));
        }
        line
    }

    /// Sends one rendered line — buffered when buffering is on,
    /// straight to the socket otherwise.
    fn send(&self, line: String) {
        match &self.buffer {
            None => {
                let _ = self.socket.send(line.as_bytes());
            }
            Some((buffer, size)) => {
                let flush_now = {
                    let mut lines = buffer.lines.lock().expect("datadog buffer lock");
                    lines.push(line);
                    lines.len() >= *size
                };
                if flush_now {
                    flush_buffer(buffer, |payload| {
                        let _ = self.socket.send(payload.as_bytes());
                    });
                }
            }
        }
    }
}

/// The soft cap on one DogStatsD datagram: a UDP payload over this
/// size risks fragmentation or silent drop at the agent. The flush
/// splits oversized batches into several datagrams instead of joining
/// everything into one.
const MAX_DATAGRAM_BYTES: usize = 8 * 1024;

/// Joins the buffered lines into datagrams under [`MAX_DATAGRAM_BYTES`]
/// and hands each to `send`.
fn flush_buffer(buffer: &Buffer, mut send: impl FnMut(String)) {
    let mut lines = buffer.lines.lock().expect("datadog buffer lock");
    if lines.is_empty() {
        return;
    }
    let mut payload = String::new();
    for line in lines.drain(..) {
        if !payload.is_empty() && payload.len() + 1 + line.len() > MAX_DATAGRAM_BYTES {
            send(std::mem::take(&mut payload));
        }
        if !payload.is_empty() {
            payload.push('\n');
        }
        payload.push_str(&line);
    }
    drop(lines);
    if !payload.is_empty() {
        send(payload);
    }
}

/// Replaces the DogStatsD delimiters in a tag key or value.
fn sanitize_tag(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '\n' | '\r' | '|' | ',' | '#' => '_',
            _ => c,
        })
        .collect()
}

impl Drop for DatadogMetrics {
    fn drop(&mut self) {
        // Final flush, then release the flush thread.
        if let Some((buffer, _)) = self.buffer.take() {
            let socket = &self.socket;
            flush_buffer(&buffer, |payload| {
                let _ = socket.send(payload.as_bytes());
            });
        }
        if let Some(buffer) = &self.flush_thread {
            buffer.stop.store(true, Ordering::Relaxed);
        }
    }
}

impl Metrics for DatadogMetrics {
    fn counter(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        // Counters carry integer values: truncated.
        let integer = value as i64;
        self.send(self.render(name, &integer.to_string(), "c", labels));
    }

    fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.send(self.render(name, &format_float(value), "h", labels));
    }

    fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.send(self.render(name, &format_float(value), "g", labels));
    }
}

/// Float formatting: shortest representation
/// that round-trips (`42`, `42.5`, `0.042`).
fn format_float(value: f64) -> String {
    let mut formatted = format!("{value}");
    if formatted.ends_with(".0") {
        formatted.truncate(formatted.len() - 2);
    }
    formatted
}

#[cfg(test)]
mod tests {
    use super::sanitize_tag;
    use super::*;
    use std::str::from_utf8;

    /// A bound UDP socket playing the Datadog agent; receives the
    /// datagrams the provider sends.
    struct Agent(UdpSocket);

    impl Agent {
        fn bind() -> (Self, String) {
            let socket = UdpSocket::bind("127.0.0.1:0").expect("agent binds");
            let address = socket.local_addr().expect("agent address").to_string();
            (Self(socket), address)
        }

        fn recv(&self) -> String {
            let mut buf = [0u8; 4096];
            let (n, _) = self.0.recv_from(&mut buf).expect("datagram arrives");
            from_utf8(&buf[..n]).expect("utf-8 payload").to_string()
        }
    }

    fn provider(address: &str) -> DatadogMetrics {
        DatadogMetrics::new(DogStatsDOptions::new().with_address(address)).expect("provider binds")
    }

    #[test]
    fn counters_render_integer_lines_with_sorted_tags() {
        let (agent, address) = Agent::bind();
        let provider = provider(&address);
        provider.counter("requests_total", 5.0, &[("z", "1"), ("a", "2")]);
        assert_eq!(agent.recv(), "requests_total:5|c|#a:2,z:1");
    }

    #[test]
    fn gauges_and_histograms_carry_floats() {
        let (agent, address) = Agent::bind();
        let provider = provider(&address);
        provider.gauge("queue_depth", 42.0, &[]);
        assert_eq!(agent.recv(), "queue_depth:42|g");
        provider.histogram("request_duration_seconds", 0.042, &[]);
        assert_eq!(agent.recv(), "request_duration_seconds:0.042|h");
    }

    #[test]
    fn the_namespace_prefixes_the_name() {
        let (agent, address) = Agent::bind();
        let provider = DatadogMetrics::new(
            DogStatsDOptions::new()
                .with_address(&address)
                .with_namespace("myapp"),
        )
        .expect("provider binds");
        provider.counter("requests_total", 1.0, &[]);
        assert_eq!(agent.recv(), "myapp.requests_total:1|c");
    }

    #[test]
    fn sub_one_sample_rates_append_the_rate_suffix() {
        let (agent, address) = Agent::bind();
        let provider = DatadogMetrics::new(
            DogStatsDOptions::new()
                .with_address(&address)
                .with_sample_rate(0.5),
        )
        .expect("provider binds");
        provider.gauge("queue_depth", 1.0, &[]);
        assert_eq!(agent.recv(), "queue_depth:1|g|@0.5");
    }

    #[test]
    fn buffered_sends_batch_lines_into_one_datagram() {
        let (agent, address) = Agent::bind();
        let provider = DatadogMetrics::new(
            DogStatsDOptions::new()
                .with_address(&address)
                .with_buffer_size(2),
        )
        .expect("provider binds");
        provider.counter("first", 1.0, &[]);
        // The first line sits in the buffer; the second fills it and
        // flushes both as one datagram.
        provider.counter("second", 2.0, &[]);
        let payload = agent.recv();
        assert_eq!(payload, "first:1|c\nsecond:2|c");
        // And the buffer is empty afterwards: the next send is its own
        // datagram.
        provider.gauge("third", 3.0, &[]);
        assert_eq!(agent.recv(), "third:3|g");
    }

    #[test]
    fn dropping_the_provider_flushes_the_buffer() {
        let (agent, address) = Agent::bind();
        let provider = DatadogMetrics::new(
            DogStatsDOptions::new()
                .with_address(&address)
                .with_buffer_size(8),
        )
        .expect("provider binds");
        provider.gauge("pending", 9.0, &[]);
        drop(provider);
        assert_eq!(agent.recv(), "pending:9|g");
    }

    #[test]
    fn float_formatting_drops_trailing_zeros() {
        assert_eq!(format_float(42.0), "42");
        assert_eq!(format_float(42.5), "42.5");
        assert_eq!(format_float(0.042), "0.042");
    }
    #[test]
    fn tag_delimiters_are_neutralized() {
        assert_eq!(sanitize_tag("tenant\nfake:1|c"), "tenant_fake:1_c");
        assert_eq!(sanitize_tag("a,b|c#d"), "a_b_c_d");
        assert_eq!(sanitize_tag("plain"), "plain");
    }
}
