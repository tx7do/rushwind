//! The per-request context bag services receive — the port of the
//! reference's `context.Context` propagation (its generated handlers hand
//! every service method a ctx carrying the Transport operation id and the
//! auth middleware's injected identity).
//!
//! `claims` stays the raw verified bag: the reference tokens carry custom
//! claims (user/tenant/role fields) the services read by name.

/// The context handed to every service trait method as its first
/// parameter.
#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    /// The verified JWT claim bag; `None` on the auth-free subtree where
    /// no authentication ran.
    pub claims: Option<serde_json::Map<String, serde_json::Value>>,
    /// The operation id — `/<pkg>.<Svc>/<Method>`, the reference's
    /// `Transport.Operation`.
    pub operation: &'static str,
    /// The HTTP method of the matched binding.
    pub method: String,
    /// The request path.
    pub path: String,
    /// The best-effort client IP: `X-Forwarded-For` (first hop), else
    /// `X-Real-IP`, else empty.
    pub ip: String,
    /// The `User-Agent` header, empty when absent.
    pub user_agent: String,
    /// Lowercased header names → first value. Services read the select
    /// headers the reference reads off its transport ctx (captcha pairs,
    /// X-Forwarded-Proto, …).
    pub headers: std::collections::HashMap<String, String>,
    /// Request cookies (parsed `Cookie` header).
    pub cookies: std::collections::HashMap<String, String>,
    /// The response-header vehicle — the reference's `ReplyHeader`: the
    /// service layer appends (name, value) pairs (Set-Cookie pairs) that
    /// the lifecycle tail merges into the outgoing response.
    pub reply_headers: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl RequestContext {
    /// Appends a response header (the ReplyHeader.Add path).
    pub fn add_reply_header(&self, name: &str, value: impl Into<String>) {
        self.reply_headers
            .lock()
            .expect("reply headers poisoned")
            .push((name.to_string(), value.into()));
    }
}

impl RequestContext {
    /// A string claim by name.
    pub fn claim_str(&self, name: &str) -> Option<&str> {
        self.claims.as_ref()?.get(name).and_then(|v| v.as_str())
    }

    /// An unsigned claim by name; accepts integral JSON numbers and
    /// numeric strings (JWT libraries encode uint32s either way).
    pub fn claim_u32(&self, name: &str) -> Option<u32> {
        let value = self.claims.as_ref()?.get(name)?;
        match value {
            serde_json::Value::Number(n) => n.as_u64().map(|v| v as u32),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// A string-list claim by name (JSON array of strings).
    pub fn claim_str_list(&self, name: &str) -> Vec<String> {
        match self.claims.as_ref().and_then(|c| c.get(name)) {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// The bearer token carried by the Authorization header (`Bearer` /
/// `bearer` prefix), `None` when the header is absent or another
/// scheme rides.
pub fn bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(|t| t.to_owned())
}

/// Which header leads the client-IP probe — the two read orders the
/// middleware layers actually use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpSource {
    /// `X-Real-IP` (whole trimmed value), else the first
    /// `X-Forwarded-For` hop — the authorization/evaluation trail.
    RealIpFirst,
    /// The first `X-Forwarded-For` hop, else the first `X-Real-IP`
    /// hop — the audit trail and the context bag.
    ForwardedFirst,
}

/// Best-effort client IP: the leading header's value per the source
/// policy, else the other header's first comma hop. The socket peer
/// stays unavailable at this layer.
pub fn client_ip(headers: &axum::http::HeaderMap, source: IpSource) -> String {
    fn whole_trimmed(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(String::from)
    }

    fn first_hop(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(String::from)
    }

    match source {
        IpSource::RealIpFirst => {
            whole_trimmed(headers, "x-real-ip").or_else(|| first_hop(headers, "x-forwarded-for"))
        }
        IpSource::ForwardedFirst => {
            first_hop(headers, "x-forwarded-for").or_else(|| first_hop(headers, "x-real-ip"))
        }
    }
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{bearer_token, client_ip, IpSource};
    use axum::http::HeaderMap;

    fn h(name: &'static str, value: &str) -> HeaderMap {
        let mut m = HeaderMap::new();
        m.insert(name, value.parse().unwrap());
        m
    }

    #[test]
    fn bearer_token_strips_both_prefix_spellings() {
        assert_eq!(bearer_token(&HeaderMap::new()), None);
        assert_eq!(
            bearer_token(&h("authorization", "Bearer abc.def")),
            Some("abc.def".to_string())
        );
        assert_eq!(
            bearer_token(&h("authorization", "bearer abc.def")),
            Some("abc.def".to_string())
        );
        assert_eq!(bearer_token(&h("authorization", "Basic abc")), None);
    }

    #[test]
    fn client_ip_follows_the_source_policy() {
        let both = h("x-forwarded-for", "203.0.113.7, 10.0.0.1");
        assert_eq!(client_ip(&both, IpSource::RealIpFirst), "203.0.113.7");
        assert_eq!(client_ip(&both, IpSource::ForwardedFirst), "203.0.113.7");

        let real = h("x-real-ip", "198.51.100.9");
        assert_eq!(client_ip(&real, IpSource::RealIpFirst), "198.51.100.9");
        assert_eq!(client_ip(&real, IpSource::ForwardedFirst), "198.51.100.9");

        // Both present: the leading header wins per policy.
        let mut m = both.clone();
        m.insert("x-real-ip", "198.51.100.9".parse().unwrap());
        assert_eq!(client_ip(&m, IpSource::RealIpFirst), "198.51.100.9");
        assert_eq!(client_ip(&m, IpSource::ForwardedFirst), "203.0.113.7");

        assert_eq!(client_ip(&HeaderMap::new(), IpSource::RealIpFirst), "");
        let blank = h("x-real-ip", "   ");
        assert_eq!(client_ip(&blank, IpSource::RealIpFirst), "");
    }
}
