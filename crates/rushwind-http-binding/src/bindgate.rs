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
//! * proto — the binary wire-format decode: `DynamicMessage::decode` over
//!   the buffered body; failures → 400 `body unmarshal %s` with the
//!   decoder's own prose (the same failure class the json branch raises).
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
                ResolvedCodec::Proto => match DynamicMessage::decode(desc.clone(), &bytes[..]) {
                    Ok(parsed) => dyn_msg = parsed,
                    Err(e) => return codec_failure(format!("body unmarshal {e}")),
                },
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use axum::middleware::from_fn;
    use axum::routing::post;
    use axum::{Extension, Router};
    use prost::Message as _;
    use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    use prost_reflect::Value;
    use tower::ServiceExt;

    const INPUT_FQ: &str = "bind.test.v1.BindInput";
    const REGISTERED: &[&str] = &["json", "x-www-form-urlencoded", "proto"];

    /// A leaked single-message pool: one proto3 message with a string and
    /// an int32 scalar — the minimal corpus the bind layer needs.
    fn test_pool() -> &'static DescriptorPool {
        let msg = DescriptorProto {
            name: Some("BindInput".to_string()),
            field: [("name", 1, Type::String), ("count", 2, Type::Int32)]
                .map(|(name, number, ty)| FieldDescriptorProto {
                    name: Some(name.to_string()),
                    number: Some(number),
                    label: Some(Label::Optional as i32),
                    r#type: Some(ty as i32),
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("bind_test.proto".to_string()),
            package: Some("bind.test.v1".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![msg],
            ..Default::default()
        };
        let set = FileDescriptorSet { file: vec![file] };
        Box::leak(Box::new(
            DescriptorPool::from_file_descriptor_set(set).unwrap(),
        ))
    }

    async fn echo(Extension(bound): Extension<BoundMessage>) -> String {
        let name = bound
            .0
            .get_field_by_name("name")
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        let count = bound
            .0
            .get_field_by_name("count")
            .and_then(|v| v.as_i32())
            .unwrap_or_default();
        format!("{name}/{count}")
    }

    fn app(pool: &'static DescriptorPool) -> Router {
        Router::new()
            .route("/echo", post(echo))
            .layer(from_fn(move |req, next| async move {
                bind_run(pool, INPUT_FQ, true, REGISTERED, req, next).await
            }))
    }

    fn wire_input(name: &str, count: i32) -> Vec<u8> {
        let pool = test_pool();
        let desc = pool.get_message_by_name(INPUT_FQ).unwrap();
        let mut msg = DynamicMessage::new(desc);
        msg.set_field_by_name("name", Value::String(name.to_string()));
        msg.set_field_by_name("count", Value::I32(count));
        msg.encode_to_vec()
    }

    async fn run(app: Router, content_type: &str, body: Vec<u8>) -> (StatusCode, String) {
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/echo")
            .header("content-type", content_type)
            .body(Body::from(body))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn binary_proto_body_binds() {
        let pool = test_pool();
        let (status, body) = run(app(pool), "application/proto", wire_input("bolt", 7)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "bolt/7");
    }

    #[tokio::test]
    async fn binary_proto_body_with_parameters_resolves() {
        let pool = test_pool();
        let (status, body) = run(
            app(pool),
            "application/proto; codec=proto",
            wire_input("silt", 3),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "silt/3");
    }

    #[tokio::test]
    async fn empty_proto_body_skips_binding() {
        let pool = test_pool();
        let (status, body) = run(app(pool), "application/proto", Vec::new()).await;
        assert_eq!(status, StatusCode::OK);
        // The reference decoder's exact sequence: empty body → no binding —
        // the handler sees an all-default message.
        assert_eq!(body, "/0");
    }

    #[tokio::test]
    async fn malformed_proto_wire_format_is_a_codec_failure() {
        let pool = test_pool();
        let (status, body) = run(app(pool), "application/proto", vec![0x0A]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("\"code\":400"), "{body}");
        assert!(body.contains("\"reason\":\"CODEC\""), "{body}");
        assert!(body.contains("\"message\":\"body unmarshal "), "{body}");
    }

    #[tokio::test]
    async fn truncated_proto_wire_format_is_a_codec_failure() {
        let pool = test_pool();
        let mut truncated = wire_input("bolt", 7);
        truncated.pop(); // cut the int32 payload off its tag
        let (status, body) = run(app(pool), "application/proto", truncated).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("\"message\":\"body unmarshal "), "{body}");
    }

    #[tokio::test]
    async fn json_branch_still_binds() {
        let pool = test_pool();
        let (status, body) = run(
            app(pool),
            "application/json",
            br#"{"name":"clay","count":4}"#.to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "clay/4");
    }
}
