//! Content-Type codec resolution — the `CodecForRequest`/`ContentSubtype`
//! pair as `DefaultRequestDecoder`
//! applies it to body routes.
//!
//! Resolution walks EVERY Content-Type header value (the reference's
//! `for _, accept := range r.Header[name]`); the first value whose subtype
//! resolves to a registered codec wins. `Header.Get` — the FIRST value
//! only, empty when absent — is what the failure message echoes. The
//! subtype slice is verbatim upstream: the raw span between the first `/`
//! and the first `;`, with NO case folding — the registry lookup
//! is case-sensitive, so `application/JSON` fails where
//! `application/json` resolves. The `;` search starts at byte 0 (upstream
//! `strings.Index` over the whole value), so a `;` before the `/` makes
//! the subtype — and thus the codec match — empty.

use axum::extract::Request;
use axum::http::header;

/// The codecs the reference deployment can resolve, matching the packages
/// its binary imports (kratos `encoding/json`, `encoding/proto`, and the
/// `encoding/form` urlencoded codec — empirically pinned against the
/// reference 2026-09-14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedCodec {
    /// protojson.
    Json,
    /// The urlencoded form codec — pairs fed through the same populate
    /// machinery as the query binder.
    Form,
    /// Binary protobuf wire format (`application/proto`), decoded via
    /// prost-reflect's `DynamicMessage::decode` in the bind layer.
    Proto,
}

impl ResolvedCodec {
    fn subtype_name(self) -> &'static str {
        match self {
            ResolvedCodec::Json => "json",
            ResolvedCodec::Form => "x-www-form-urlencoded",
            ResolvedCodec::Proto => "proto",
        }
    }
}

/// The `internal/httputil.ContentSubtype` semantics: the raw slice between the
/// first `/` and the first `;`, where the `;` is searched from the START of
/// the value (verbatim upstream — a `;` positioned before the `/` yields
/// the empty string). No case folding. Byte-indexed like
/// `strings.Index`; a slice landing inside a multi-byte character is
/// dropped to empty rather than panicking.
pub fn content_subtype(content_type: &str) -> &str {
    let bytes = content_type.as_bytes();
    let Some(left) = bytes.iter().position(|&b| b == b'/') else {
        return "";
    };
    let right = match bytes.iter().position(|&b| b == b';') {
        Some(right) if right < left => return "",
        Some(right) => right,
        None => bytes.len(),
    };
    std::str::from_utf8(&bytes[left + 1..right]).unwrap_or("")
}

/// Resolves the request's codec over its Content-Type header values,
/// restricted to the given registered subtype names. The first value whose
/// subtype is both recognized and registered wins; `None` mirrors the
/// reference's `(GetCodec("json"), false)` fall-through, which the decoder
/// turns into the unregister error.
pub fn resolve_codec(req: &Request, registered: &[&str]) -> Option<ResolvedCodec> {
    for value in req.headers().get_all(header::CONTENT_TYPE) {
        let Ok(text) = value.to_str() else {
            continue;
        };
        let candidate = match content_subtype(text) {
            "json" => ResolvedCodec::Json,
            "x-www-form-urlencoded" => ResolvedCodec::Form,
            "proto" => ResolvedCodec::Proto,
            _ => continue,
        };
        if registered.contains(&candidate.subtype_name()) {
            return Some(candidate);
        }
    }
    None
}

/// The value the failure message echoes: `Header.Get` semantics — the
/// first Content-Type header value, or the empty string when absent.
pub fn first_content_type(req: &Request) -> &str {
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtype_slicing_is_verbatim_upstream() {
        assert_eq!(content_subtype("application/json"), "json");
        assert_eq!(content_subtype("application/json; charset=utf-8"), "json");
        // No case folding — the registry lookup is case-sensitive.
        assert_eq!(content_subtype("application/JSON"), "JSON");
        assert_eq!(
            content_subtype("application/x-www-form-urlencoded"),
            "x-www-form-urlencoded"
        );
        // Missing or degenerate boundaries.
        assert_eq!(content_subtype("json"), "");
        assert_eq!(content_subtype(""), "");
        // Semicolon before slash: right < left+1 → empty.
        assert_eq!(content_subtype(";application/json"), "");
        assert_eq!(content_subtype("a;b/c"), "");
    }
}
