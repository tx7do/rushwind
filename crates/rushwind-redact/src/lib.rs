//! The static response-redaction runtime — the port of the reference
//! deployment's `protoc-gen-go-redact` behavior, reinterpreted for the
//! descriptor-driven stack: no generated per-message `Redact()` methods,
//! no protoc plugin — the `(redact.v1)` options already ride the
//! annotated descriptor set (the same bytes the reference's plugin read
//! at generation time), and the plan turns them into a per-message rule
//! table applied to the response's dynamic message before protojson
//! serialization.
//!
//! Semantics are anchored to the reference's GENERATED OUTPUT (the
//! deployment's `*.pb.redact.go` files), not the plugin's internals:
//!
//! * rules apply only where a field carries an explicit `(redact.value)`
//!   option — no recursion by field name or type (the reference's
//!   `// Safe field:` lines); absent fields are never touched (the
//!   reference's nil-pointer guards), so under `EmitUnpopulated` an
//!   absent field still serializes its default;
//! * `(redact.value).element = { nested: true }` on a repeated message
//!   field applies each item's own type rules (`redact.Apply(item)`);
//! * `(redact.method_skip)` drops the redaction for that operation's
//!   responses entirely (the reference's `// Redaction skipped`
//!   wrappers).
//!
//! Fail-closed: the plan build refuses rule vocabulary it does not
//! implement (regex, hash, uuid, ip, url, custom, condition, the
//! message/file-level nil/empty/auto-detect machinery, internal
//! services/methods) instead of silently degrading a security control —
//! the same refusal posture the generator takes for annotation shapes
//! it cannot reproduce. Supported value rules beyond the corpus's
//! (mask, email, fixed string, element-nested, method-skip): fixed
//! scalars of every kind, mask/truncate/fixed-length string rules, and
//! element empty/item rules.
//!
//! One documented divergence from the reference: the generated Go
//! helpers slice strings by BYTE (`len(s)`, `s[:keepFirst]`), which
//! corrupts multi-byte UTF-8; this runtime masks by CHAR, which is
//! byte-identical on the ASCII fields (phone/email) the corpus
//! annotates and safe where the reference is not.

mod apply;
mod plan;
mod rules;

pub use plan::{PlanError, RedactPlan};
