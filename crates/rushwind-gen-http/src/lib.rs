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
use std::fmt::Write as _;

use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, FieldDescriptor, FileDescriptor, Kind,
    MessageDescriptor, Value,
};

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
fn redact_arg(cfg: &CodegenConfig<'_>) -> String {
    match cfg.redact_plan_expr {
        Some(expr) => format!("Some({expr})"),
        None => "None".to_owned(),
    }
}

/// Well-known messages the form codec binds as leaves, mapped to their
/// wire-relevant leaf kind. Wrappers reduce to the wrapped scalar kind —
/// exactly the codec's behavior.
fn well_known_leaf(full_name: &str) -> Option<LeafKind> {
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
fn leak(s: &str) -> &'static str {
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

// ---------------------------------------------------------------------------
// HttpRule extraction
// ---------------------------------------------------------------------------

struct HttpBinding {
    method: String,
    path: String,
    body_star: bool,
}

struct HttpRuleData {
    bindings: Vec<HttpBinding>,
}

impl HttpRuleData {
    fn flatten(self) -> Vec<HttpBinding> {
        self.bindings
    }
}

fn http_rule_of(
    options: &DynamicMessage,
    ext: &prost_reflect::ExtensionDescriptor,
) -> Option<HttpRuleData> {
    let value = options.get_extension(ext);
    let Value::Message(rule) = &*value else {
        return None;
    };
    let mut bindings = Vec::new();
    if let Some(b) = primary_binding(rule) {
        bindings.push(b);
    }
    // additional_bindings → one route per entry, same as the reference
    // generator's sd.Methods expansion.
    if let Some(additional) = rule.get_field_by_name("additional_bindings") {
        if let Value::List(items) = &*additional {
            for item in items {
                if let Value::Message(sub) = item {
                    if let Some(b) = primary_binding(sub) {
                        bindings.push(b);
                    }
                }
            }
        }
    }
    if bindings.is_empty() {
        None
    } else {
        Some(HttpRuleData { bindings })
    }
}

fn primary_binding(rule: &DynamicMessage) -> Option<HttpBinding> {
    const PATTER_FIELDS: &[&str] = &["get", "post", "put", "delete", "patch"];
    let mut method = None;
    let mut path = None;
    for f in PATTER_FIELDS {
        if let Some(v) = rule.get_field_by_name(f) {
            if let Value::String(s) = &*v {
                if !s.is_empty() {
                    method = Some(f.to_ascii_uppercase());
                    path = Some(s.clone());
                    break;
                }
            }
        }
    }
    if method.is_none() {
        // custom { kind, path }
        if let Some(v) = rule.get_field_by_name("custom") {
            if let Value::Message(cust) = &*v {
                let kind = cust.get_field_by_name("kind");
                let p = cust.get_field_by_name("path");
                if let (Some(kind), Some(p)) = (kind, p) {
                    if let (Value::String(k), Value::String(p)) = (&*kind, &*p) {
                        if !k.is_empty() && !p.is_empty() {
                            method = Some(k.to_ascii_uppercase());
                            path = Some(p.clone());
                        }
                    }
                }
            }
        }
    }
    let body = rule
        .get_field_by_name("body")
        .and_then(|cow| match &*cow {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let (method, path) = (method?, path?);
    if body != "*" && !body.is_empty() {
        return None; // named bodies: fail-closed in build_route
    }
    Some(HttpBinding {
        method,
        path,
        body_star: body == "*",
    })
}

// ---------------------------------------------------------------------------
// Route + binding-plan extraction
// ---------------------------------------------------------------------------

fn build_route(
    service: &prost_reflect::ServiceDescriptor,
    method: &prost_reflect::MethodDescriptor,
    binding: &HttpBinding,
    input: &MessageDescriptor,
    output: &MessageDescriptor,
) -> Result<RouteData, String> {
    if !binding.path.starts_with('/') {
        return Err(format!(
            "path template does not start with '/': {}",
            binding.path
        ));
    }
    let path_vars = extract_path_vars(&binding.path)?;
    // Leaves are only relevant for query binding; body="*" methods still
    // run the query bind in the reference handlers, so the plan is
    // computed for every route regardless of body mode.
    let mut leaves = Vec::new();
    walk_leaves(input, &mut Vec::new(), &mut leaves);
    Ok(RouteData {
        method: binding.method.clone(),
        path: binding.path.clone(),
        operation_id: format!("/{}/{}", service.full_name(), method.name()),
        service_name: service.name().to_string(),
        service_fq: service.full_name().to_string(),
        method_name: method.name().to_string(),
        input_fq: input.full_name().to_string(),
        output_fq: output.full_name().to_string(),
        path_vars,
        body_star: binding.body_star,
        leaves,
    })
}

/// Extracts `{name}` variables, rejecting regex-constrained variables and
/// wildcard segments — a shape-compatible corpus contains neither, and the
/// Rust router could not express them identically.
fn extract_path_vars(path: &str) -> Result<Vec<String>, String> {
    let mut vars = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        let end = rest[start..]
            .find('}')
            .map(|i| start + i)
            .ok_or_else(|| format!("unterminated '{{' in path template: {path}"))?;
        let name = &rest[start + 1..end];
        if name.is_empty()
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.chars().next().map_or(true, |c| c.is_ascii_digit())
        {
            return Err(format!(
                "unsupported path variable '{{{name}}}' in {path} (regex-constrained or malformed variables are not supported)"
            ));
        }
        vars.push(name.to_string());
        rest = &rest[end + 1..];
    }
    for seg in path.split('/') {
        if seg == "*" || seg == "**" {
            return Err(format!("wildcard segment in path template: {path}"));
        }
    }
    Ok(vars)
}

/// The form-binding leaf walk: scalars/enums/bytes are leaves; well-known
/// messages from the codec's whitelist are leaves; other singular messages
/// are traversed; maps and lists are leaves with their element kinds. Oneof
/// membership is recorded so the binder can enforce the codec's
/// `field already set for oneof` error.
fn walk_leaves(msg: &MessageDescriptor, path: &mut Vec<(String, String)>, out: &mut Vec<LeafData>) {
    for field in msg.fields() {
        let seg = (field.json_name().to_string(), field.name().to_string());
        let (kind, map) = if field.is_map() {
            (LeafKind::Unsupported, Some(map_kinds(&field)))
        } else if field.is_list() {
            (
                LeafKind::from_field(&field).unwrap_or(LeafKind::Unsupported),
                None,
            )
        } else {
            match field.kind() {
                Kind::Message(md) => {
                    if let Some(wk) = well_known_leaf(md.full_name()) {
                        (wk, None)
                    } else {
                        path.push(seg);
                        walk_leaves(&md, path, out);
                        path.pop();
                        continue;
                    }
                }
                _ => (
                    LeafKind::from_field(&field).unwrap_or(LeafKind::Unsupported),
                    None,
                ),
            }
        };
        path.push(seg);
        out.push(LeafData {
            path_json: path
                .iter()
                .map(|(j, _)| j.as_str())
                .collect::<Vec<_>>()
                .join("."),
            path_proto: path
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>()
                .join("."),
            kind,
            repeated: field.is_list(),
            map,
            oneof: field.containing_oneof().map(|o| leak(o.name())),
        });
        path.pop();
    }
}

/// Map field kinds: (key kind, value kind). Keys are scalar by proto rules;
/// values reduce through the same leaf-kind mapping, with non-whitelisted
/// message values becoming `Unsupported` (the codec's error path).
fn map_kinds(field: &FieldDescriptor) -> (LeafKind, LeafKind) {
    let Kind::Message(entry) = field.kind() else {
        return (LeafKind::Unsupported, LeafKind::Unsupported);
    };
    let mut key = LeafKind::Unsupported;
    let mut value = LeafKind::Unsupported;
    for ef in entry.fields() {
        let k = LeafKind::from_field(&ef).unwrap_or(LeafKind::Unsupported);
        match ef.name() {
            "key" => key = k,
            "value" => value = k,
            _ => {}
        }
    }
    (key, value)
}

fn extract_error_table(
    file: &FileDescriptor,
    ext: &Option<prost_reflect::ExtensionDescriptor>,
    out: &mut Vec<(String, String, i32)>,
) {
    let Some(ext) = ext else { return };
    let package = file.package_name().to_string();
    // The *_error.proto reason enums are top-level; nested enums are not
    // walked (none exist in shape-compatible corpora — extend here if that
    // ever changes).
    for enm in file.enums() {
        collect_enum_values(&enm, &package, ext, out);
    }
}

fn collect_enum_values(
    enm: &EnumDescriptor,
    package: &str,
    ext: &prost_reflect::ExtensionDescriptor,
    out: &mut Vec<(String, String, i32)>,
) {
    for value in enm.values() {
        let opts = value.options();
        let v = opts.get_extension(ext);
        if let Value::I32(status) = *v {
            if (100..=599).contains(&status) {
                out.push((package.to_string(), value.name().to_string(), status));
            }
        }
    }
}

fn to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

/// Ports prost-build ident.rs `sanitize_identifier`: Rust keywords become raw
/// identifiers or get suffixed; numeric-leading identifiers get prefixed.
fn sanitize_ident(name: &str) -> String {
    match name {
        "as" | "break" | "const" | "continue" | "else" | "enum" | "false" | "fn" | "for" | "if"
        | "impl" | "in" | "let" | "loop" | "match" | "mod" | "move" | "mut" | "pub" | "ref"
        | "return" | "static" | "struct" | "trait" | "true" | "type" | "unsafe" | "use"
        | "where" | "while" | "dyn" | "abstract" | "become" | "box" | "do" | "final" | "macro"
        | "override" | "priv" | "typeof" | "unsized" | "virtual" | "yield" | "async" | "await"
        | "try" | "gen" => format!("r#{name}"),
        "_" | "super" | "self" | "Self" | "extern" | "crate" => format!("{name}_"),
        s if s.starts_with(|c: char| c.is_numeric()) => format!("_{name}"),
        _ => name.to_string(),
    }
}

/// Maps a fully-qualified proto message name to the Rust type path in the
/// caller's generated modules. Naming mirrors prost-build ident.rs: type
/// names are heck upper-camel-cased then keyword-sanitized; well-known
/// types are externed to pbjson_types (the standard prost-build config
/// pairing).
fn rust_type_path(fq: &str, proto_module_path: &str) -> String {
    use heck::ToUpperCamelCase as _;
    let (package, name) = fq.rsplit_once('.').unwrap_or(("", fq));
    let type_name = sanitize_ident(&name.to_upper_camel_case());
    if package == "google.protobuf" {
        return format!("pbjson_types::{type_name}");
    }
    let mut path = String::from(proto_module_path);
    for seg in package.split('.') {
        path.push_str("::");
        path.push_str(&sanitize_ident(seg));
    }
    path.push_str("::");
    path.push_str(&type_name);
    path
}

fn rust_str(s: &str) -> String {
    format!("{:?}", s)
}

fn emit_header(out: &mut String, route_count: usize, error_entries: usize) {
    let _ = writeln!(
        out,
        "// @generated by rushwind-gen-http from the embedding deployment's contract tree. DO NOT EDIT."
    );
    let _ = writeln!(
        out,
        "// Route, binding-plan, service-trait and error-table surface for the proto-HTTP annotation semantics"
    );
    let _ = writeln!(
        out,
        "// semantics; the deployment's compatibility spec pins the wire contract."
    );
    let _ = writeln!(
        out,
        "// Corpus: {route_count} route bindings, {error_entries} error-status entries."
    );
}

fn emit_error_tables(out: &mut String, tables: &[(String, String, i32)]) {
    let mut sorted = tables.to_vec();
    sorted.sort();
    sorted.dedup();
    let mut out_local = String::new();
    let _ = writeln!(out_local, "\npub mod error_tables {{");
    let _ = writeln!(
        out_local,
        "    /// (proto package, reason enum value name) → HTTP status, from the"
    );
    let _ = writeln!(
        out_local,
        "    /// (errors.code) annotations in the *_error.proto files. Status values"
    );
    let _ = writeln!(
        out_local,
        "    /// outside 100..=599 were dropped at generation time."
    );
    let _ = writeln!(
        out_local,
        "    pub static ERROR_STATUS: &[(&str, &str, i32)] = &["
    );
    for (pkg, reason, status) in &sorted {
        let _ = writeln!(
            out_local,
            "        ({}, {}, {}i32),",
            rust_str(pkg),
            rust_str(reason),
            status
        );
    }
    let _ = writeln!(out_local, "    ];");
    let _ = writeln!(out_local, "}}");
    out.push_str(&out_local);
}

fn emit_routes(out: &mut String, routes: &[RouteData], shadow: &[bool]) {
    let _ = writeln!(out, "\npub mod routes {{");
    let _ = writeln!(out, "    /// Leaf kinds of the form-binding surface.");
    let _ = writeln!(
        out,
        "    /// Scalars are grouped exactly like the codec's switch arms (e.g. int32/sint32/sfixed32 → I32)."
    );
    let _ = writeln!(out, "    #[derive(Clone, Copy, Debug, PartialEq, Eq)]");
    let _ = writeln!(out, "    pub enum LeafKind {{");
    for v in [
        "Bool",
        "I32",
        "I64",
        "U32",
        "U64",
        "F32",
        "F64",
        "Str",
        "Bytes",
        "Enum(&'static str)",
        "Timestamp",
        "Duration",
        "FieldMask",
        "Struct",
        "Value",
        "Unsupported",
    ] {
        let _ = writeln!(out, "        {v},");
    }
    let _ = writeln!(out, "    }}");
    let _ = writeln!(
        out,
        "    /// A bindable leaf field per the form-codec walk."
    );
    let _ = writeln!(
        out,
        "    /// `path_json`/`path_proto`: dotted paths in both spellings the decoder resolves."
    );
    let _ = writeln!(
        out,
        "    /// `map`: Some((key kind, value kind)) for map fields."
    );
    let _ = writeln!(
        out,
        "    /// `oneof`: owning oneof name — the binder errors when two leaves of one oneof receive values."
    );
    let _ = writeln!(
        out,
        "    /// Note: proto3-`optional` fields surface their synthetic single-member oneof"
    );
    let _ = writeln!(
        out,
        "    /// (`_fieldname`); those can never collide by construction."
    );
    let _ = writeln!(out, "    pub struct BindLeaf {{");
    let _ = writeln!(out, "        pub path_json: &'static str,");
    let _ = writeln!(out, "        pub path_proto: &'static str,");
    let _ = writeln!(out, "        pub kind: LeafKind,");
    let _ = writeln!(out, "        pub repeated: bool,");
    let _ = writeln!(out, "        pub map: Option<(LeafKind, LeafKind)>,");
    let _ = writeln!(out, "        pub oneof: Option<&'static str>,");
    let _ = writeln!(out, "    }}");
    let _ = writeln!(
        out,
        "    /// One route binding as registered by the reference generator (r.<METHOD>(path, handler))."
    );
    let _ = writeln!(
        out,
        "    /// `shadowed`: the first-match-mux shadow analysis marked this binding unreachable on the"
    );
    let _ = writeln!(
        out,
        "    /// reference (an earlier same-method pattern matches its path); the mounts skip it."
    );
    let _ = writeln!(out, "    pub struct RouteSpec {{");
    for f in [
        ("method", "&'static str"),
        ("path", "&'static str"),
        ("operation_id", "&'static str"),
        ("service_name", "&'static str"),
        ("service_fq", "&'static str"),
        ("method_name", "&'static str"),
        ("input_fq", "&'static str"),
        ("output_fq", "&'static str"),
        ("path_vars", "&'static [&'static str]"),
        ("body_star", "bool"),
        ("shadowed", "bool"),
        ("query_leaves", "&'static [BindLeaf]"),
    ] {
        let _ = writeln!(out, "        pub {}: {},", f.0, f.1);
    }
    let _ = writeln!(out, "    }}");
    let _ = writeln!(out, "    pub static ROUTES: &[RouteSpec] = &[");
    for (idx, r) in routes.iter().enumerate() {
        let _ = writeln!(out, "        RouteSpec {{");
        let _ = writeln!(out, "            method: {},", rust_str(&r.method));
        let _ = writeln!(out, "            path: {},", rust_str(&r.path));
        let _ = writeln!(
            out,
            "            operation_id: {},",
            rust_str(&r.operation_id)
        );
        let _ = writeln!(
            out,
            "            service_name: {},",
            rust_str(&r.service_name)
        );
        let _ = writeln!(out, "            service_fq: {},", rust_str(&r.service_fq));
        let _ = writeln!(
            out,
            "            method_name: {},",
            rust_str(&r.method_name)
        );
        let _ = writeln!(out, "            input_fq: {},", rust_str(&r.input_fq));
        let _ = writeln!(out, "            output_fq: {},", rust_str(&r.output_fq));
        let path_vars_lit = format!(
            "&[{}]",
            r.path_vars
                .iter()
                .map(|v| rust_str(v))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let _ = writeln!(out, "            path_vars: {path_vars_lit},",);
        let _ = writeln!(out, "            body_star: {},", r.body_star);
        let _ = writeln!(out, "            shadowed: {},", shadow[idx]);
        let _ = writeln!(out, "            query_leaves: &[");
        for leaf in &r.leaves {
            let map = match leaf.map {
                None => "None".to_string(),
                Some((k, v)) => format!("Some((LeafKind::{}, LeafKind::{}))", k.tag(), v.tag()),
            };
            let kind = if let LeafKind::Enum(fq) = leaf.kind {
                format!("LeafKind::Enum({})", rust_str(fq))
            } else {
                format!("LeafKind::{}", leaf.kind.tag())
            };
            let oneof = match leaf.oneof {
                None => "None".to_string(),
                Some(o) => format!("Some({})", rust_str(o)),
            };
            let _ = writeln!(
                out,
                "                BindLeaf {{ path_json: {}, path_proto: {}, kind: {}, repeated: {}, map: {}, oneof: {} }},",
                rust_str(&leaf.path_json),
                rust_str(&leaf.path_proto),
                kind,
                leaf.repeated,
                map,
                oneof
            );
        }
        let _ = writeln!(out, "            ],");
        let _ = writeln!(out, "        }},");
    }
    let _ = writeln!(out, "    ];");
    let _ = writeln!(out, "}}");
}

fn emit_traits(out: &mut String, traits: &BTreeMap<String, TraitData>, cfg: &CodegenConfig<'_>) {
    let _ = writeln!(out, "\npub mod services {{");
    for (fq, t) in traits {
        let _ = writeln!(
            out,
            "    /// Service `{fq}` — one trait method per annotated proto method,"
        );
        let _ = writeln!(
            out,
            "    /// typed against the deployment's contract types. The trait mirrors"
        );
        let _ = writeln!(out, "    /// the reference's server interface 1:1.");
        let _ = writeln!(out, "    #[async_trait::async_trait]");
        let _ = writeln!(
            out,
            "    pub trait {}Handlers: Send + Sync {{",
            t.service_name
        );
        for m in &t.methods {
            let _ = writeln!(out, "        /// Operation `{}`.", m.operation_id);
            let _ = writeln!(
                out,
                "        async fn {}(&self, ctx: rushwind_http_binding::ctx::RequestContext, req: {}) -> Result<{}, rushwind_http_binding::envelope::StatusError>;",
                m.rust_name,
                rust_type_path(&m.input_fq, cfg.proto_module_path),
                rust_type_path(&m.output_fq, cfg.proto_module_path)
            );
        }
        let _ = writeln!(out, "    }}");
    }
    let _ = writeln!(out, "}}");
}

/// Emits `null_<service>` placeholder impls: one struct per service trait
/// whose every method answers the Unknown error shape via
/// `rushwind_http_binding::envelope::internal_error`. The full mounted
/// surface is exercised end-to-end (binding → envelope) while the caller's
/// real modules are still landing; each service's null is retired when its
/// module arrives.
fn emit_nulls(out: &mut String, traits: &BTreeMap<String, TraitData>, cfg: &CodegenConfig<'_>) {
    use heck::ToSnakeCase as _;
    let _ = writeln!(out, "\npub mod nulls {{");
    let _ = writeln!(out, "    use std::sync::Arc;");
    for (fq, t) in traits {
        let struct_name = format!("Null{}", t.service_name);
        let null_name = t.service_name.to_snake_case();
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "    /// Placeholder `{fq}` impl; every method answers the Unknown"
        );
        let _ = writeln!(out, "    /// error shape until the real module lands.");
        let _ = writeln!(out, "    pub struct {struct_name};");
        let _ = writeln!(out, "    #[async_trait::async_trait]");
        let _ = writeln!(
            out,
            "    impl crate::gen::services::{}Handlers for {} {{",
            t.service_name, struct_name
        );
        for m in &t.methods {
            let _ = writeln!(
                out,
                "        async fn {}(&self, _ctx: rushwind_http_binding::ctx::RequestContext, _req: {}) -> Result<{}, rushwind_http_binding::envelope::StatusError> {{",
                m.rust_name,
                rust_type_path(&m.input_fq, cfg.proto_module_path),
                rust_type_path(&m.output_fq, cfg.proto_module_path)
            );
            let _ = writeln!(
                out,
                "            Err(rushwind_http_binding::envelope::internal_error(\"not implemented\"))"
            );
            let _ = writeln!(out, "        }}");
        }
        let _ = writeln!(out, "    }}");
        let _ = writeln!(
            out,
            "    pub fn null_{null_name}() -> Arc<dyn crate::gen::services::{}Handlers> {{",
            t.service_name
        );
        let _ = writeln!(out, "        Arc::new({struct_name})");
        let _ = writeln!(out, "    }}");
    }
    let _ = writeln!(out, "}}");
}

/// Emits `mount_<service>` per annotated service, plus one private
/// `mount_route_<idx>` per route binding. Each route registers an axum
/// handler whose closure funnels through `rushwind_http_binding::glue::handle`,
/// which implements the request lifecycle tail.
///
/// The mount splits its routes between the two threaded routers per the
/// config's auth-free set — the deployment's whitelist registrations — and
/// hands every route's method router to the assembly-provided `wrap`
/// closure together with the route's wire facts and its gated
/// classification. The closure composes the layer stack — the framework
/// bind layer outermost on every route, the auth gate inside it on gated
/// routes only — reproducing the reference's pre-middleware-bind /
/// middleware order.
fn emit_mounts(
    out: &mut String,
    routes_by_service: &BTreeMap<String, Vec<usize>>,
    routes: &[RouteData],
    shadow: &[bool],
    cfg: &CodegenConfig<'_>,
) {
    use heck::ToSnakeCase as _;
    let _ = writeln!(
        out,
        "
pub mod mounts {{"
    );
    let _ = writeln!(out, "    use axum::routing::Router;");
    let _ = writeln!(out, "    use std::sync::Arc;");
    let wrap_ty = "wrap: &dyn Fn(axum::routing::MethodRouter, &rushwind_http_binding::wire::RouteWire, bool) -> axum::routing::MethodRouter";
    for (fq, indices) in routes_by_service {
        let trait_name = format!("{}Handlers", fq.rsplit('.').next().unwrap_or(fq));
        let mount_name = fq.rsplit('.').next().unwrap_or(fq).to_snake_case();
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "    /// Mounts every route of `{fq}`, each onto the public router"
        );
        let _ = writeln!(
            out,
            "    /// when its owning operation is auth-free, else the gated one."
        );
        let _ = writeln!(out, "    #[allow(clippy::too_many_lines)]");
        let _ = writeln!(out, "    pub fn mount_{mount_name}(router_pub: Router, router_gate: Router, svc: Arc<dyn crate::gen::services::{trait_name}>, {wrap_ty}) -> (Router, Router) {{");
        // `mut` only on the routers that actually receive routes from this
        // service, so the emitted tree builds warning-free.
        let (has_pub, has_gate) = indices.iter().fold((false, false), |acc, idx| {
            if cfg.auth_free(&routes[*idx].service_fq, &routes[*idx].method_name) {
                (true, acc.1)
            } else {
                (acc.0, true)
            }
        });
        let (pub_mut, gate_mut) = (
            if has_pub { "mut " } else { "" },
            if has_gate { "mut " } else { "" },
        );
        let _ = writeln!(out, "        let {pub_mut}r_pub = router_pub;");
        let _ = writeln!(out, "        let {gate_mut}r_gate = router_gate;");
        for idx in indices {
            if shadow[*idx] {
                let _ = writeln!(
                    out,
                    "        // route {idx} shadowed by an earlier same-method pattern — first-match mux unreachable; not mounted."
                );
                continue;
            }
            let target = if cfg.auth_free(&routes[*idx].service_fq, &routes[*idx].method_name) {
                "r_pub"
            } else {
                "r_gate"
            };
            let _ = writeln!(
                out,
                "        {target} = mount_route_{idx}({target}, Arc::clone(&svc), wrap);"
            );
        }
        let _ = writeln!(out, "        (r_pub, r_gate)");
        let _ = writeln!(out, "    }}");
    }
    for (idx, route) in routes.iter().enumerate() {
        if shadow[idx] {
            // Shadowed bindings: unreachable on the reference; no mount
            // emitter, and the table row carries shadowed=true.
            continue;
        }
        let fq = &route.service_fq;
        let trait_name = format!("{}Handlers", fq.rsplit('.').next().unwrap_or(fq));
        let call = to_snake(&route.method_name);
        let gated = !cfg.auth_free(&route.service_fq, &route.method_name);
        let _ = writeln!(out);
        let _ = writeln!(out, "    fn mount_route_{idx}(r: Router, svc: Arc<dyn crate::gen::services::{trait_name}>, {wrap_ty}) -> Router {{");
        let _ = writeln!(out, "        let spec = &super::routes::ROUTES[{idx}];");
        let _ = writeln!(out, "        let m = match spec.method {{");
        for (k, v) in [
            ("GET", "GET"),
            ("POST", "POST"),
            ("PUT", "PUT"),
            ("DELETE", "DELETE"),
            ("PATCH", "PATCH"),
        ] {
            let _ = writeln!(
                out,
                "            {k:?} => axum::routing::MethodFilter::{v},"
            );
        }
        let _ = writeln!(
            out,
            "            _ => panic!(\"unsupported route method {{}}\", spec.method),"
        );
        let _ = writeln!(out, "        }};");
        // The handler signature: the path-variable extractor when the
        // template declares variables, then the request itself (the
        // pre-bound message rides in its extensions).
        if route.path_vars.is_empty() {
            let _ = writeln!(out, "        let h = move |req: axum::extract::Request| {{");
        } else {
            let _ = writeln!(out, "        let h = move |params: axum::extract::Path<std::collections::HashMap<String, String>>, req: axum::extract::Request| {{");
        }
        let _ = writeln!(out, "            let svc = Arc::clone(&svc);");
        let _ = writeln!(out, "            async move {{");
        let _ = writeln!(
            out,
            "                let wire = rushwind_http_binding::wire::RouteWire {{"
        );
        let _ = writeln!(out, "                    operation_id: spec.operation_id,");
        let _ = writeln!(out, "                    input_fq: spec.input_fq,");
        let _ = writeln!(out, "                    output_fq: spec.output_fq,");
        let _ = writeln!(out, "                    body_star: spec.body_star,");
        let _ = writeln!(out, "                    path_vars: spec.path_vars,");
        let _ = writeln!(out, "                }};");
        let _ = writeln!(out, "                rushwind_http_binding::glue::handle(");
        let _ = writeln!(out, "                    {},", cfg.pool_expr);
        let _ = writeln!(out, "                    {},", redact_arg(cfg));
        let _ = writeln!(out, "                    &wire,");
        let _ = writeln!(out, "                    move |ctx, req| {{");
        let _ = writeln!(out, "                        let svc = Arc::clone(&svc);");
        let _ = writeln!(
            out,
            "                        async move {{ svc.{call}(ctx, req).await }}"
        );
        let _ = writeln!(out, "                    }},");
        let _ = writeln!(out, "                    req,");
        if route.path_vars.is_empty() {
            let _ = writeln!(out, "                    std::collections::HashMap::new(),");
        } else {
            let _ = writeln!(out, "                    params.0,");
        }
        let _ = writeln!(out, "                )");
        let _ = writeln!(out, "                .await");
        let _ = writeln!(out, "            }}");
        let _ = writeln!(out, "        }};");
        // The bind layer's wire facts (the framework layer binds body and
        // query pre-auth from these; the gated flag tells the assembly's
        // wrap whether to compose the auth gate inside).
        let _ = writeln!(
            out,
            "        let bind_wire = rushwind_http_binding::wire::RouteWire {{"
        );
        let _ = writeln!(out, "            operation_id: spec.operation_id,");
        let _ = writeln!(out, "            input_fq: spec.input_fq,");
        let _ = writeln!(out, "            output_fq: spec.output_fq,");
        let _ = writeln!(out, "            body_star: spec.body_star,");
        let _ = writeln!(out, "            path_vars: spec.path_vars,");
        let _ = writeln!(out, "        }};");
        let _ = writeln!(
            out,
            "        let mr = wrap(axum::routing::on(m, h), &bind_wire, {gated});"
        );
        let _ = writeln!(out, "        r.route(spec.path, mr)");
        let _ = writeln!(out, "    }}");
    }
    let _ = writeln!(out, "}}");
}
