//! The proto-HTTP wire contract, parameterized by a caller-supplied
//! [`prost_reflect::DescriptorPool`] so the crate stays corpus-free.
//!
//! Request side:
//!
//! * [`binder`] — the form binder: the populate machinery behind both the
//!   query/path-parameter binding and the urlencoded form-body decode.
//!   Semantics anchored to the reference implementation's
//!   `encoding/form/proto_decode.go` (the compatibility spec documents the
//!   behavior table).
//! * [`content_type`] — the Content-Type→codec resolution the reference
//!   applies to body routes (the `internal/httputil` subtype slice plus
//!   the codec registry lookup).
//! * [`bindgate`] — the pre-auth bind layer: the section of the reference
//!   generated handlers that runs body/query binding BEFORE the auth
//!   middleware. Mounted outermost, it reproduces the observable order:
//!   codec/binding failures answer 400/CODEC ahead of any 401. The bound
//!   message rides to the handler in a request extension.
//!
//! Response side:
//!
//! * [`codec`] — the protojson response codec with the reference's
//!   `EmitUnpopulated` semantics, plus the static-redaction hook: a
//!   caller-supplied [`rushwind_redact::RedactPlan`] (built from the
//!   same pool's `(redact.v1)` options) mutates the response before
//!   encoding, the port of the reference's redacted server wrappers.
//! * [`glue`] — the lifecycle tail after the middleware chain: path
//!   variables, static conversion, the typed service call, response
//!   serialization, and the reply-header merge.
//! * [`envelope`] — the four-field status error envelope and its carrying
//!   type, as the reference's `Status`/`DefaultErrorEncoder` emit it.
//! * [`wire`] — the per-route wire facts the bind layer and the glue
//!   consume without referencing generated types.
//! * [`ctx`] — the per-request context bag every service method receives
//!   (the reference's `context.Context` propagation: operation id plus
//!   the auth middleware's verified claim bag).
//!
//! The reference deployment's registered codec subtypes are NOT baked in:
//! the assembly passes its registered set to [`bindgate`], matching
//! whichever codec packages the reference binary imports.
//!
//! Known dormant divergences (the caller's compatibility spec owns the
//! register): the binary wire-format body decode is not implemented here
//! (pass-through), and `form_urlencoded` parses lossily where the
//! reference's `url.ParseQuery` errors on malformed escapes.
//!
//! Note: this crate carries no versioned dependency on the reference
//! implementation — its sources are cited purely as semantic anchors for
//! the compatibility contract.

pub mod binder;
pub mod bindgate;
pub mod codec;
pub mod content_type;
pub mod ctx;
pub mod envelope;
pub mod glue;
pub mod wire;
