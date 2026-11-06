//! The pre-middleware bind layer — the section of the
//! Kratos-generated leaf handler that runs BEFORE `ctx.Middleware`
//! executes the auth chain: `ctx.Bind` (body decode via the resolved
//! codec) and `ctx.BindQuery` (query parameters). The assembly mounts
//! this layer OUTERMOST on every route, so the observable order matches
//! the reference: codec-resolution and binding failures answer 400/CODEC
//! ahead of any 401 from the auth layer.
//!
//! Body handling per resolved codec (see [`crate::content_type`]):
//!
//! * json — protojson parse (DiscardUnknown); the parsed message becomes
//!   the bound message (the reference's `Unmarshal(data, v)` populates
//!   `v`); parse failures → 400 `body unmarshal %s` (each side's own
//!   parser prose — the differential rig shape-compares this class).
//! * x-www-form-urlencoded — the form codec: urlencoded pairs fed through
//!   the same populate machinery as the query binder.
//! * proto — pass-through, unvalidated and unbound: no wire-format decoder
//!   in this crate (the dormant divergence the caller's compat spec
//!   registers).
//!
//! A body route whose Content-Type resolves to nothing answers 400
//! `unregister Content-Type: %s` with `%s` = `Header.Get` (the first
//! value, empty when absent) — empirically pinned against the reference:
//! this check fires even for an empty body, ahead of the auth layer.
//!
//! One deliberate divergence from the reference: the body is read under
//! a ceiling ([`DEFAULT_BODY_LIMIT`], override via
//! [`bind_run_with_limit`]) and an over-limit body answers 413 — the
//! reference buffers unconditionally, which turns every pre-auth body
//! route into an unbounded memory commitment.
//!
//! The bound message rides to the handler as a [`BoundMessage`] request
//! extension. Path-variable binding CANNOT run here — axum middleware
//! never sees the router's captures — so it runs in the handler, after
//! the auth layer: the one ordering divergence the caller's exemption set
//! registers.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use prost_reflect::{DescriptorPool, DeserializeOptions, DynamicMessage};

use crate::binder::bind_form;
use crate::content_type::{first_content_type, resolve_codec, ResolvedCodec};
use crate::envelope::{codec_error, error_response, internal_error};

/// The pre-bound request message, inserted by [`bind_run`] and consumed by
/// the route handler. `Clone` because `http::Extensions` requires it.
#[derive(Clone)]
pub struct BoundMessage(pub DynamicMessage);

/// The codec/binding failure: 400/CODEC with the reference's message
/// shapes.
fn codec_failure(message: String) -> Response {
    error_response(codec_error(message))
}

/// The over-limit body: 413 with an empty reason — the ceiling the
/// reference lacks, so there is no reference prose to mirror.
fn payload_too_large() -> crate::envelope::StatusError {
    crate::envelope::StatusError::new(413, "", "request body exceeds the configured limit")
}

/// Collects key→values pairs from a urlencoded string (query string or
/// form body), accumulating repeated keys — the shape `url.Values` gives
/// the reference's populate machinery.
fn collect_pairs(input: &[u8]) -> Vec<(String, Vec<String>)> {
    let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
    for (k, v) in form_urlencoded::parse(input) {
        if let Some(entry) = pairs.iter_mut().find(|(ek, _)| ek == &k) {
            entry.1.push(v.into_owned());
        } else {
            pairs.push((k.into_owned(), vec![v.into_owned()]));
        }
    }
    pairs
}

/// The default request-body ceiling: 2 MiB, matching axum's own
/// `DefaultBodyLimit` so every buffering edge in one process shares the
/// same bound. The reference decoder buffers unconditionally; this limit
/// is a deliberate rushwind divergence (the assembly's compat spec should
/// register it) — the pre-auth layer must not turn unauthenticated
/// request bodies into unbounded memory. [`bind_run_with_limit`] accepts
/// a custom ceiling.
pub const DEFAULT_BODY_LIMIT: usize = 2 * 1024 * 1024;

/// The bind layer body with the default body limit. `pool` supplies the
/// input message descriptor; `registered` is the assembly's registered
/// codec-subtype set.
pub async fn bind_run(
    pool: &'static DescriptorPool,
    input_fq: &'static str,
    body_star: bool,
    registered: &[&str],
    req: Request,
    next: Next,
) -> Response {
    bind_run_with_limit(
        pool,
        input_fq,
        body_star,
        registered,
        DEFAULT_BODY_LIMIT,
        req,
        next,
    )
    .await
}

/// The bind layer body with an explicit body ceiling. `pool` supplies the
/// input message descriptor; `registered` is the assembly's registered
/// codec-subtype set; `body_limit` caps how many bytes of the request
/// body are buffered (a larger body answers 413 before any decode).
pub async fn bind_run_with_limit(
    pool: &'static DescriptorPool,
    input_fq: &'static str,
    body_star: bool,
    registered: &[&str],
    body_limit: usize,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(desc) = pool.get_message_by_name(input_fq) else {
        return error_response(internal_error("input type missing from pool"));
    };
    let mut dyn_msg = DynamicMessage::new(desc.clone());

    // 1. Body binding (body routes only). Codec resolution happens FIRST —
    //    an unresolvable Content-Type answers 400 even for an empty body —
    //    then the body is read under the ceiling, and an empty body skips
    //    binding (the reference decoder's exact sequence).
    if body_star {
        let Some(codec) = resolve_codec(&req, registered) else {
            return codec_failure(format!(
                "unregister Content-Type: {}",
                first_content_type(&req)
            ));
        };
        let (parts, body) = req.into_parts();
        let bytes = match axum::body::to_bytes(body, body_limit).await {
            Ok(b) => b,
            Err(_) => return error_response(payload_too_large()),
        };
        req = Request::from_parts(parts, axum::body::Body::empty());
        if !bytes.is_empty() {
            match codec {
                ResolvedCodec::Json => {
                    let mut de = serde_json::Deserializer::from_slice(&bytes);
                    let opts = DeserializeOptions::new();
                    match DynamicMessage::deserialize_with_options(desc, &mut de, &opts)
                        .and_then(|parsed| de.end().map(|_| parsed))
                    {
                        Ok(parsed) => dyn_msg = parsed,
                        Err(e) => return codec_failure(format!("body unmarshal {e}")),
                    }
                }
                ResolvedCodec::Form => {
                    let pairs = collect_pairs(&bytes);
                    if let Err(e) = bind_form(&mut dyn_msg, &pairs) {
                        return codec_failure(format!("body unmarshal {e}"));
                    }
                }
                ResolvedCodec::Proto => {
                    // No wire-format port: the body passes unbound.
                }
            }
        }
    }

    // 2. Query binding — every route, exactly as the reference's
    //    BindQuery section. Failures carry the raw binding error (the
    //    reference's bind.go wraps nothing here — unlike the body path's
    //    "body unmarshal" prefix).
    if let Some(query) = req.uri().query() {
        let pairs = collect_pairs(query.as_bytes());
        if !pairs.is_empty() {
            if let Err(e) = bind_form(&mut dyn_msg, &pairs) {
                return error_response(e);
            }
        }
    }

    req.extensions_mut().insert(BoundMessage(dyn_msg));
    next.run(req).await
}
