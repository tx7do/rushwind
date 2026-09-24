//! The per-route lifecycle tail — the port of the section of the
//! reference's generated leaf handler that runs AFTER the middleware
//! chain: retrieve the pre-bound message the bind layer
//! ([`crate::bindgate`]) inserted, bind the path variables, convert to
//! the static contract type, invoke the typed service method, and
//! serialize the response.
//!
//! Ordering note: the reference binds path variables BEFORE the auth
//! middleware (its `BindVars` runs in the pre-middleware section); axum
//! middleware never sees the router's captures, so this bind runs after
//! the auth layer — an ordering divergence the embedding service's
//! exemption set registers (class `path-bind-post-auth`).

use std::collections::HashMap;
use std::future::Future;

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use prost_reflect::DescriptorPool;

use crate::binder::bind_form;
use crate::bindgate::BoundMessage;
use crate::codec::serialize_response;
use crate::ctx::RequestContext;
use crate::envelope::{codec_error, error_response, internal_error, StatusError};
use crate::wire::RouteWire;
use rushwind_redact::RedactPlan;

/// The per-route handler tail. `call` invokes the typed service trait
/// method for this route; the assembled [`RequestContext`] rides as its
/// first argument (the reference hands every service method a ctx with
/// the operation id and the auth middleware's identity). The verified
/// claim bag reaches the context through the [`rushwind_authn::AuthClaims`]
/// extension the auth middleware inserts.
pub async fn handle<T, R, F, Fut>(
    pool: &DescriptorPool,
    redact: Option<&RedactPlan>,
    wire: &RouteWire,
    call: F,
    mut req: axum::extract::Request,
    params: HashMap<String, String>,
) -> Response
where
    T: prost::Message + Default,
    R: prost::Message,
    F: FnOnce(RequestContext, T) -> Fut,
    Fut: Future<Output = Result<R, StatusError>>,
{
    let claims = req
        .extensions()
        .get::<rushwind_authn::AuthClaims>()
        .map(|c| c.0.clone());
    let forwarded = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty());
    let ip = forwarded.or_else(|| {
        req.headers()
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_owned())
    });
    let reply_headers = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut headers = std::collections::HashMap::new();
    for (name, value) in req.headers() {
        if let Ok(text) = value.to_str() {
            headers
                .entry(name.as_str().to_ascii_lowercase())
                .or_insert_with(|| text.to_owned());
        }
    }
    let mut cookies = std::collections::HashMap::new();
    if let Some(cookie_header) = req
        .headers()
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
    {
        for pair in cookie_header.split(';') {
            if let Some((name, value)) = pair.split_once('=') {
                cookies.insert(name.trim().to_string(), value.trim().to_string());
            }
        }
    }
    let ctx = RequestContext {
        reply_headers,
        claims,
        operation: wire.operation_id,
        method: req.method().as_str().to_owned(),
        path: req.uri().path().to_owned(),
        ip: ip.unwrap_or_default(),
        user_agent: req
            .headers()
            .get(header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
        headers,
        cookies,
    };

    let Some(bound) = req.extensions_mut().remove::<BoundMessage>() else {
        return error_response(internal_error("bound message missing"));
    };
    let mut dyn_msg = bound.0;

    // 1. Path variables: only the variables the route template declares,
    //    from the router's captures.
    if !wire.path_vars.is_empty() {
        let pairs: Vec<(String, Vec<String>)> = wire
            .path_vars
            .iter()
            .filter_map(|name| {
                params
                    .get(*name)
                    .map(|v| ((*name).to_string(), vec![v.clone()]))
            })
            .collect();
        if !pairs.is_empty() {
            if let Err(e) = bind_form(&mut dyn_msg, &pairs) {
                return error_response(e);
            }
        }
    }

    // 2. Static conversion of the bound request.
    let req_msg: T = match dyn_msg.transcode_to::<T>() {
        Ok(v) => v,
        Err(e) => return error_response(codec_error(format!("body unmarshal {e}"))),
    };

    // 3. Service call. The reply-header bag is cloned out first: the
    //    context itself is consumed.
    let reply_bag = std::sync::Arc::clone(&ctx.reply_headers);
    let reply = match call(ctx, req_msg).await {
        Ok(r) => r,
        Err(e) => return error_response(e),
    };

    // 4. Response serialization: 200 + application/json, EmitUnpopulated.
    //    The deployment's redaction plan rides along (the reference's
    //    redacted server wrappers sit between the handler and the
    //    encoder); `None` leaves the responses untouched.
    let bytes = match serialize_response(
        pool,
        wire.output_fq,
        &reply,
        redact.map(|plan| (plan, wire.operation_id)),
    ) {
        Ok(b) => b,
        Err(e) => return error_response(e),
    };
    let mut response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response();
    // The ReplyHeader vehicle: whatever the service appended rides out.
    for (name, value) in reply_bag.lock().expect("reply headers").drain(..) {
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            axum::http::HeaderValue::from_str(&value),
        ) {
            response.headers_mut().append(name, value);
        }
    }
    response
}
