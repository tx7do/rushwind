//! Route-data extraction from annotated descriptors: the proto `HttpRule`
//! options and the binding plan each route carries, plus the error-table
//! sweep — everything `generate` needs before a line of Rust is emitted.

use crate::{leak, well_known_leaf, LeafData, LeafKind, RouteData};
use prost_reflect::{
    DynamicMessage, EnumDescriptor, FieldDescriptor, FileDescriptor, Kind, MessageDescriptor, Value,
};

// ---------------------------------------------------------------------------
// HttpRule extraction
// ---------------------------------------------------------------------------

pub(crate) struct HttpBinding {
    pub(crate) method: String,
    path: String,
    body_star: bool,
}

pub(crate) struct HttpRuleData {
    bindings: Vec<HttpBinding>,
}

impl HttpRuleData {
    pub(crate) fn flatten(self) -> Vec<HttpBinding> {
        self.bindings
    }
}

pub(crate) fn http_rule_of(
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

pub(crate) fn build_route(
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

pub(crate) fn extract_error_table(
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

pub(crate) fn to_snake(name: &str) -> String {
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
