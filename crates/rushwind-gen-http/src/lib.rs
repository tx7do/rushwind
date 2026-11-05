//! Descriptor-driven Rust code generator for the proto-HTTP route surface.
//!
//! Consumes a full proto compile closure (a `FileDescriptorSet` carrying
//! the custom-option bytes — produced by `protoc`, not protox, whose
//! serializer drops them) and emits one Rust source module containing:
//!
//! * `error_tables` — (package, reason) → HTTP status, extracted from the
//!   `(errors.code)` annotations in the `*_error.proto` files.
//! * `routes` — one `RouteSpec` per annotated method binding (including
//!   `additional_bindings`), carrying the axum-syntax path (identical to
//!   the reference registration string for shape-compatible corpora), the
//!   operation id, the path-variable list, the body mode, and the
//!   form-binding plan (every leaf field the form codec would bind from
//!   query/path input, with both name spellings, kinds, oneof membership
//!   and map/list structure).
//! * `services` — one `#[async_trait]` trait per annotated proto service,
//!   with one method per annotated proto method, typed against the
//!   caller's generated contract types (addressed through
//!   [`CodegenConfig::proto_module_path`]).
//! * `nulls` — placeholder impls of every service trait answering the
//!   Unknown error shape; replaced one service at a time as the caller's
//!   real modules land.
//! * `mounts` — per-service `mount_<service>` functions threading TWO
//!   routers: auth-free operations ([`CodegenConfig::auth_free`], the
//!   deployment's whitelist registration set) onto the public router,
//!   everything else onto the gated router that the assembly wraps with
//!   the auth middleware and then merges.
//!
//! Fail-closed rules (the generator refuses rather than silently
//! degrading): regex-constrained or wildcard path variables, named
//! (non-`*`) bodies, and path templates that are not plain
//! literal/`{var}` segments — none of which occur in a
//! shape-compatible corpus; if an upstream contract ever grows them, the
//! build breaks instead of serving a divergent route set.

use std::collections::BTreeMap;

use prost_reflect::{DescriptorPool, FieldDescriptor, Kind};

/// Extension names, per the vendored declarations the caller compiles in.
const EXT_HTTP_RULE: &str = "google.api.http";
const EXT_ERRORS_CODE: &str = "errors.code";

/// The generator's deployment-specific knobs: where the caller's contract
/// types live, which operations ride the auth-free subtree, and how the
/// generated mounts reach the descriptor pool.
pub struct CodegenConfig<'a> {
    /// The Rust module path the caller's generated proto types resolve
    /// against (e.g. `admin_proto::proto`). Well-known types always
    /// extern to `pbjson_types`, the standard pairing.
    pub proto_module_path: &'a str,
    /// The Rust expression yielding the `&'static DescriptorPool` the
    /// generated mounts hand to the lifecycle glue (e.g.
    /// `admin_proto::pool()`). Emitted verbatim at the call sites.
    pub pool_expr: &'a str,
    /// The auth-free operations as (service full name, proto method name)
    /// pairs — the deployment's whitelist registration set. Classification
    /// is per OPERATION, not per binding: `additional_bindings` of a
    /// whitelisted method are exempt together with the primary binding.
    pub auth_free: &'a [(&'a str, &'a str)],
    /// The Rust expression yielding the deployment's `Option<&'static
    /// RedactPlan>` (e.g. `Some(fixture_proto::redact_plan())`) — the
    /// `(redact.v1)` static redaction every mounted handler threads to
    /// the lifecycle glue. `None` emits a literal `None`: no plan, no
    /// redaction (a pool without the vendored redact schema would build
    /// an empty plan anyway).
    pub redact_plan_expr: Option<&'a str>,
}

impl CodegenConfig<'_> {
    /// Whether an operation's owning route bindings are auth-free —
    /// mounted onto the public router rather than the gated one.
    fn auth_free(&self, service_fq: &str, method_name: &str) -> bool {
        self.auth_free
            .iter()
            .any(|(s, m)| *s == service_fq && *m == method_name)
    }
}

/// The redaction argument emitted at every `glue::handle` call site.
pub(crate) fn redact_arg(cfg: &CodegenConfig<'_>) -> String {
    match cfg.redact_plan_expr {
        Some(expr) => format!("Some({expr})"),
        None => "None".to_owned(),
    }
}

/// Well-known messages the form codec binds as leaves, mapped to their
/// wire-relevant leaf kind. Wrappers reduce to the wrapped scalar kind —
/// exactly the codec's behavior.
pub(crate) fn well_known_leaf(full_name: &str) -> Option<LeafKind> {
    match full_name {
        "google.protobuf.Timestamp" => Some(LeafKind::Timestamp),
        "google.protobuf.Duration" => Some(LeafKind::Duration),
        "google.protobuf.FieldMask" => Some(LeafKind::FieldMask),
        "google.protobuf.Struct" => Some(LeafKind::Struct),
        "google.protobuf.Value" => Some(LeafKind::Value),
        "google.protobuf.DoubleValue" => Some(LeafKind::F64),
        "google.protobuf.FloatValue" => Some(LeafKind::F32),
        "google.protobuf.Int64Value" => Some(LeafKind::I64),
        "google.protobuf.Int32Value" => Some(LeafKind::I32),
        "google.protobuf.UInt64Value" => Some(LeafKind::U64),
        "google.protobuf.UInt32Value" => Some(LeafKind::U32),
        "google.protobuf.BoolValue" => Some(LeafKind::Bool),
        "google.protobuf.StringValue" => Some(LeafKind::Str),
        "google.protobuf.BytesValue" => Some(LeafKind::Bytes),
        _ => None,
    }
}

/// The leaf kinds the runtime binder distinguishes. Grouped scalars mirror
/// the form codec's switch arms: e.g. `int32`, `sint32` and `sfixed32`
/// all land on [`LeafKind::I32`].
#[derive(Clone, Copy)]
enum LeafKind {
    Bool,
    I32,
    I64,
    U32,
    U64,
    F32,
    F64,
    Str,
    Bytes,
    Enum(&'static str),
    Timestamp,
    Duration,
    FieldMask,
    Struct,
    Value,
    /// Message-typed leaf the codec refuses (`unsupported message type`).
    Unsupported,
}

impl LeafKind {
    fn from_field(field: &FieldDescriptor) -> Option<LeafKind> {
        match field.kind() {
            Kind::Double => Some(LeafKind::F64),
            Kind::Float => Some(LeafKind::F32),
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Some(LeafKind::I32),
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Some(LeafKind::I64),
            Kind::Uint32 | Kind::Fixed32 => Some(LeafKind::U32),
            Kind::Uint64 | Kind::Fixed64 => Some(LeafKind::U64),
            Kind::Bool => Some(LeafKind::Bool),
            Kind::String => Some(LeafKind::Str),
            Kind::Bytes => Some(LeafKind::Bytes),
            Kind::Enum(ed) => Some(LeafKind::Enum(leak(ed.full_name()))),
            Kind::Message(md) => well_known_leaf(md.full_name()),
        }
    }

    fn tag(&self) -> &'static str {
        match self {
            LeafKind::Bool => "Bool",
            LeafKind::I32 => "I32",
            LeafKind::I64 => "I64",
            LeafKind::U32 => "U32",
            LeafKind::U64 => "U64",
            LeafKind::F32 => "F32",
            LeafKind::F64 => "F64",
            LeafKind::Str => "Str",
            LeafKind::Bytes => "Bytes",
            LeafKind::Enum(_) => "Enum",
            LeafKind::Timestamp => "Timestamp",
            LeafKind::Duration => "Duration",
            LeafKind::FieldMask => "FieldMask",
            LeafKind::Struct => "Struct",
            LeafKind::Value => "Value",
            LeafKind::Unsupported => "Unsupported",
        }
    }
}

/// Leaked FQ enum names — the generated tables are `static` and live for the
/// process lifetime anyway.
pub(crate) fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// A bindable leaf as extracted from the descriptor schema.
struct LeafData {
    path_json: String,
    path_proto: String,
    kind: LeafKind,
    repeated: bool,
    map: Option<(LeafKind, LeafKind)>,
    oneof: Option<&'static str>,
}

struct RouteData {
    method: String,
    path: String,
    operation_id: String,
    service_name: String,
    service_fq: String,
    method_name: String,
    input_fq: String,
    output_fq: String,
    path_vars: Vec<String>,
    body_star: bool,
    leaves: Vec<LeafData>,
}

struct TraitData {
    service_name: String,
    methods: Vec<TraitMethod>,
}

struct TraitMethod {
    rust_name: String,
    input_fq: String,
    output_fq: String,
    operation_id: String,
}

/// The generator entry point: descriptor-set bytes → Rust source.
pub fn generate_from_bytes(bytes: &[u8], cfg: &CodegenConfig<'_>) -> Result<String, String> {
    let pool = DescriptorPool::decode(bytes).map_err(|e| format!("decode descriptor set: {e}"))?;
    generate(&pool, cfg)
}

/// The generator core over a live pool.
pub fn generate(pool: &DescriptorPool, cfg: &CodegenConfig<'_>) -> Result<String, String> {
    let http_ext = pool
        .get_extension_by_name(EXT_HTTP_RULE)
        .ok_or("google.api.http extension not registered in pool (missing third_party)")?;
    let errors_code_ext = pool.get_extension_by_name(EXT_ERRORS_CODE);

    let mut routes: Vec<RouteData> = Vec::new();
    let mut traits: BTreeMap<String, TraitData> = BTreeMap::new();
    let mut error_tables: Vec<(String, String, i32)> = Vec::new();

    for file in pool.files() {
        extract_error_table(&file, &errors_code_ext, &mut error_tables);

        for service in file.services() {
            let mut trait_methods: Vec<TraitMethod> = Vec::new();
            for method in service.methods() {
                let Some(rule) = http_rule_of(&method.options(), &http_ext) else {
                    continue;
                };
                for binding in rule.flatten() {
                    let route = build_route(
                        &service,
                        &method,
                        &binding,
                        &method.input(),
                        &method.output(),
                    )?;
                    routes.push(route);
                }
                trait_methods.push(TraitMethod {
                    rust_name: to_snake(method.name()),
                    input_fq: method.input().full_name().to_string(),
                    output_fq: method.output().full_name().to_string(),
                    operation_id: format!("/{}/{}", service.full_name(), method.name()),
                });
            }
            if !trait_methods.is_empty()
                && traits
                    .insert(
                        service.full_name().to_string(),
                        TraitData {
                            service_name: service.name().to_string(),
                            methods: trait_methods,
                        },
                    )
                    .is_some()
            {
                return Err(format!(
                    "duplicate annotated service full name: {}",
                    service.full_name()
                ));
            }
        }
    }

    if routes.is_empty() {
        return Err("no annotated routes found in the descriptor set".into());
    }

    // Group route indices per service fq for the mount emitters.
    let mut routes_by_service: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, r) in routes.iter().enumerate() {
        routes_by_service
            .entry(r.service_fq.clone())
            .or_default()
            .push(idx);
    }

    let mut out = String::new();
    let shadow = shadow_flags(&routes);
    emit_header(&mut out, routes.len(), error_tables.len());
    emit_error_tables(&mut out, &error_tables);
    emit_routes(&mut out, &routes, &shadow);
    emit_traits(&mut out, &traits, cfg);
    emit_nulls(&mut out, &traits, cfg);
    emit_mounts(&mut out, &routes_by_service, &routes, &shadow, cfg);
    Ok(out)
}

/// Reference-mux route matching: a `{var}` pattern segment matches any path
/// segment, literals match literally, and the segment counts must be
/// equal.
fn pattern_matches(pattern: &[&str], path: &[&str]) -> bool {
    pattern.len() == path.len()
        && pattern
            .iter()
            .zip(path)
            .all(|(p, s)| p.starts_with('{') || p == s)
}

/// The first-match mux shadow analysis. The reference mux answers
/// first-registered-first-matched: an earlier-registered route with the
/// same method whose pattern (with `{var}` wildcards) matches a later
/// route's literal path makes that later route UNREACHABLE on the
/// reference. axum's router prefers static segments, so mounting such a
/// route would route its path to itself instead of the shadowing
/// pattern route — a structural divergence. The `shadowed` flag marks
/// these routes; the mounts skip them, replicating the reference's
/// effective surface.
fn shadow_flags(routes: &[RouteData]) -> Vec<bool> {
    let mut flags = vec![false; routes.len()];
    let segments: Vec<Vec<&str>> = routes.iter().map(|r| r.path.split('/').collect()).collect();
    for i in 0..routes.len() {
        for j in 0..i {
            if routes[j].method != routes[i].method {
                continue;
            }
            if pattern_matches(&segments[j], &segments[i]) {
                flags[i] = true;
                break;
            }
        }
    }
    flags
}

mod emit;
mod extract;

use emit::{emit_error_tables, emit_header, emit_mounts, emit_nulls, emit_routes, emit_traits};
use extract::{build_route, extract_error_table, http_rule_of, to_snake};
