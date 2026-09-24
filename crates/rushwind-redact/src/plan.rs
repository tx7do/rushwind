//! The plan builder — one walk over the caller's descriptor pool that
//! resolves every `(redact.v1)` option into the per-message rule table
//! and the operation skip set, refusing anything the runtime does not
//! implement.

use std::collections::{HashMap, HashSet};

use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, ExtensionDescriptor, FieldDescriptor, Kind,
    ReflectMessage, Value,
};

use crate::rules::{FieldRule, ScalarRule};

/// A plan-build failure. The message names the offending descriptor —
/// the deployment fixes its proto, not the code.
#[derive(Debug, Clone)]
pub struct PlanError {
    message: String,
}

impl PlanError {
    fn new(context: &str, reason: impl std::fmt::Display) -> Self {
        Self {
            message: format!("redact plan: {context}: {reason}"),
        }
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlanError {}

/// The static redaction plan: per-message field rules plus the
/// operations whose responses redaction skips. Built once over the
/// annotated descriptor pool (see [`RedactPlan::build`]); immutable and
/// `Send + Sync` thereafter.
#[derive(Debug, Default)]
pub struct RedactPlan {
    /// `pkg.Svc/Method` operations whose generated reference wrappers
    /// carry `// Redaction skipped` — the `(redact.method_skip)` set.
    skipped_operations: HashSet<String>,
    /// Message fully-qualified name → (field number → rule). Messages
    /// without any annotated field have no entry, and a lookup miss is
    /// the reference's all-`// Safe field` no-op.
    pub(crate) messages: HashMap<String, HashMap<u32, FieldRule>>,
}

impl RedactPlan {
    /// Walks the pool and resolves every `(redact.v1)` option. The
    /// extension descriptors come from the pool itself, so a compile
    /// closure without the vendored `redact/v1/redact.proto` yields an
    /// empty plan — nothing annotated, nothing to do. Every unsupported
    /// option shape FAILS the build (see the crate docs for the
    /// supported set); the error names the descriptor.
    pub fn build(pool: &DescriptorPool) -> Result<RedactPlan, PlanError> {
        let ext = |name: &str| pool.get_extension_by_name(name);
        let field_value = ext("redact.value");
        let file_skip = ext("redact.file_skip");
        let auto_detect = ext("redact.auto_detect");
        let service_skip = ext("redact.service_skip");
        let internal_service = ext("redact.internal_service");
        let internal_service_code = ext("redact.internal_service_code");
        let internal_service_err = ext("redact.internal_service_err_message");
        let method_skip = ext("redact.method_skip");
        let internal_method = ext("redact.internal_method");
        let internal_method_code = ext("redact.internal_method_code");
        let internal_method_err = ext("redact.internal_method_err_message");
        let message_nil = ext("redact.nil");
        let message_empty = ext("redact.empty");
        let message_ignored = ext("redact.ignored");

        let mut plan = RedactPlan::default();

        for file in pool.files() {
            let options = file.options();
            if bool_ext(&options, &file_skip)? {
                continue;
            }
            if has_ext(&options, &auto_detect)? {
                return Err(PlanError::new(
                    file.name(),
                    "(redact.auto_detect) is not supported; annotate the fields explicitly",
                ));
            }
        }

        for service in pool.services() {
            let options = service.options();
            if has_ext(&options, &internal_service)?
                || has_ext(&options, &internal_service_code)?
                || has_ext(&options, &internal_service_err)?
            {
                return Err(PlanError::new(
                    service.full_name(),
                    "(redact.internal_service*) is not supported (internal-service denial)",
                ));
            }
            if bool_ext(&options, &service_skip)? {
                continue;
            }
            for method in service.methods() {
                let options = method.options();
                if has_ext(&options, &internal_method)?
                    || has_ext(&options, &internal_method_code)?
                    || has_ext(&options, &internal_method_err)?
                {
                    return Err(PlanError::new(
                        method.full_name(),
                        "(redact.internal_method*) is not supported (internal-method denial)",
                    ));
                }
                if bool_ext(&options, &method_skip)? {
                    plan.skipped_operations.insert(format!(
                        "{}/{}",
                        service.full_name(),
                        method.name()
                    ));
                }
            }
        }

        for message in pool.all_messages() {
            let options = message.options();
            if bool_ext(&options, &message_ignored)? {
                continue;
            }
            if bool_ext(&options, &message_nil)? || bool_ext(&options, &message_empty)? {
                return Err(PlanError::new(
                    message.full_name(),
                    "(redact.nil)/(redact.empty) message-level redaction is not supported",
                ));
            }
            let Some(value_ext) = &field_value else {
                continue;
            };
            let mut rules = HashMap::new();
            for field in message.fields() {
                let field_options = field.options();
                if !field_options.has_extension(value_ext) {
                    continue;
                }
                let Value::Message(field_rules) =
                    field_options.get_extension(value_ext).into_owned()
                else {
                    return Err(PlanError::new(
                        field.full_name(),
                        "(redact.value) did not decode to FieldRules",
                    ));
                };
                if let Some(rule) = resolve_field_rule(&field, &field_rules)? {
                    rules.insert(field.number(), rule);
                }
            }
            if !rules.is_empty() {
                plan.messages.insert(message.full_name().to_string(), rules);
            }
        }

        Ok(plan)
    }

    /// Whether the operation's responses skip redaction. Accepts the
    /// operation id in either spelling — the route table's
    /// `/pkg.Svc/Method` or the bare `pkg.Svc/Method`.
    pub fn operation_skipped(&self, operation_id: &str) -> bool {
        self.skipped_operations
            .contains(operation_id.trim_start_matches('/'))
    }

    /// The plan's rule table size for one message — exposed for
    /// diagnostics and tests.
    pub fn message_rule_count(&self, message_fq: &str) -> usize {
        self.messages.get(message_fq).map_or(0, HashMap::len)
    }
}

/// `has_extension` for an optional descriptor — absent descriptors (no
/// vendored schema) read as unset.
fn has_ext(options: &DynamicMessage, ext: &Option<ExtensionDescriptor>) -> Result<bool, PlanError> {
    Ok(match ext {
        Some(ext) => options.has_extension(ext),
        None => false,
    })
}

/// Reads a boolean extension value off an options message; absent
/// extension descriptors (no vendored schema) read as unset.
fn bool_ext(
    options: &DynamicMessage,
    ext: &Option<ExtensionDescriptor>,
) -> Result<bool, PlanError> {
    if !has_ext(options, ext)? {
        return Ok(false);
    }
    let ext = ext.as_ref().expect("checked above");
    match options.get_extension(ext).as_ref() {
        Value::Bool(value) => Ok(*value),
        _ => Err(PlanError::new(
            ext.full_name(),
            "boolean option did not decode to a bool",
        )),
    }
}

/// Resolves one field's `FieldRules` into a [`FieldRule`], validated
/// against the field's kind and cardinality. `Ok(None)` = the rules
/// request no redaction for this field (`message: { skip: true }`).
fn resolve_field_rule(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
) -> Result<Option<FieldRule>, PlanError> {
    // The `values` oneof is FieldRules' entire body: find its set arm
    // (prost-reflect 0.16 has no which_oneof accessor).
    let arm_field = rules
        .descriptor()
        .fields()
        .find(|arm| rules.has_field(arm))
        .ok_or_else(|| PlanError::new(field.full_name(), "(redact.value) sets no rule"))?;
    let arm = arm_field.name().to_owned();

    match arm.as_str() {
        "message" => {
            // MessageRules: only `skip` — the explicit no-op — is
            // supported; nil/empty/apply change the field's shape.
            let message_rules = message_arm(field, rules, "message")?;
            let flag = |name: &str| {
                bool_field(&message_rules, name).map_err(|e| PlanError::new(field.full_name(), e))
            };
            if flag("skip")? {
                Ok(None)
            } else if flag("nil")? || flag("empty")? || flag("apply")? {
                Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).message nil/empty/apply is not supported; \
                     recurse via (redact.value).element = { nested: true } on the \
                     repeated field, or annotate the leaf fields",
                ))
            } else {
                Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).message with no flags is not supported",
                ))
            }
        }
        "element" => {
            if field.cardinality() != Cardinality::Repeated {
                return Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).element on a non-repeated field",
                ));
            }
            let element_rules = message_arm(field, rules, "element")?;
            let element_field = element_field_desc(field);
            let empty_set = bool_field(&element_rules, "empty")
                .map_err(|e| PlanError::new(field.full_name(), e))?;
            let nested_set = bool_field(&element_rules, "nested")
                .map_err(|e| PlanError::new(field.full_name(), e))?;
            let item_field = element_rules
                .descriptor()
                .get_field_by_name("item")
                .expect("ElementRules.item is declared");
            let item_set = element_rules.has_field(&item_field);
            if i32::from(empty_set) + i32::from(nested_set) + i32::from(item_set) > 1 {
                return Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).element sets several of empty/nested/item",
                ));
            }
            if empty_set {
                return Ok(Some(FieldRule::ElementEmpty));
            }
            if nested_set {
                let element_kind = element_field.kind();
                let item_type = element_kind
                    .as_message()
                    .ok_or_else(|| {
                        PlanError::new(
                            field.full_name(),
                            "(redact.value).element = { nested: true } on a non-message element",
                        )
                    })?
                    .full_name()
                    .to_owned();
                return Ok(Some(FieldRule::ElementNested { item_type }));
            }
            if !item_set {
                return Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).element sets no rule (expected empty/nested/item)",
                ));
            }
            // `item`: an inner FieldRules applied to each element —
            // scalar transforms only. Resolved against the ELEMENT's
            // descriptor so kind validation sees the item, not the list.
            let Value::Message(item_rules) = element_rules.get_field(&item_field).into_owned()
            else {
                return Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).element.item did not decode to FieldRules",
                ));
            };
            match resolve_field_rule(&element_field, &item_rules)? {
                Some(FieldRule::Scalar(scalar)) => Ok(Some(FieldRule::ElementItem(scalar))),
                Some(_) => Err(PlanError::new(
                    field.full_name(),
                    "(redact.value).element.item supports scalar rules only",
                )),
                None => Ok(None),
            }
        }
        "mask" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = message_arm(field, rules, "mask")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Mask {
                keep_first: u32_field(&m, "keep_first"),
                keep_last: u32_field(&m, "keep_last"),
                mask_char: default_str(&m, "mask_char", "*"),
            })))
        }
        "email" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = message_arm(field, rules, "email")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Email {
                keep_local_first: u32_field(&m, "keep_local_first"),
                mask_domain: bool_field(&m, "mask_domain")
                    .map_err(|e| PlanError::new(field.full_name(), e))?,
                mask_char: default_str(&m, "mask_char", "*"),
            })))
        }
        "truncate" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = message_arm(field, rules, "truncate")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Truncate {
                length: u32_field(&m, "length"),
                suffix: default_str(&m, "suffix", "..."),
            })))
        }
        "fixed_length" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = message_arm(field, rules, "fixed_length")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::FixedLength {
                mask_char: default_str(&m, "char", "X"),
            })))
        }
        "string" => {
            expect_kind(field, Kind::String, &arm)?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(Value::String(
                string_arm(field, rules, "string")?,
            )))))
        }
        "bytes" => {
            expect_kind(field, Kind::Bytes, &arm)?;
            let value = scalar_arm(&rules.descriptor(), "bytes");
            match rules.get_field(&value).as_ref() {
                Value::Bytes(value) => Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(
                    Value::Bytes(value.clone()),
                )))),
                _ => Err(PlanError::new(field.full_name(), "unreadable bytes rule")),
            }
        }
        "bool" => {
            expect_kind(field, Kind::Bool, &arm)?;
            let value = scalar_arm(&rules.descriptor(), "bool");
            match rules.get_field(&value).as_ref() {
                Value::Bool(value) => Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(Value::Bool(
                    *value,
                ))))),
                _ => Err(PlanError::new(field.full_name(), "unreadable bool rule")),
            }
        }
        "float" | "double" | "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64"
        | "fixed32" | "fixed64" | "sfixed32" | "sfixed64" | "enum" => {
            let value = scalar_arm(&rules.descriptor(), &arm);
            let raw = rules.get_field(&value).into_owned();
            let fixed = numeric_fixed(field, &arm, raw)?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(fixed))))
        }
        other => Err(PlanError::new(
            field.full_name(),
            format!("(redact.value).{other} is not supported"),
        )),
    }
}

/// Resolves a numeric/enum rule value into a `Value` variant matching
/// the FIELD's kind (the arm↔kind check runs first, so the variant
/// follows the kind).
fn numeric_fixed(field: &FieldDescriptor, arm: &str, raw: Value) -> Result<Value, PlanError> {
    let err = |reason: &str| PlanError::new(field.full_name(), reason.to_owned());
    match (arm, field.kind(), raw) {
        ("float", Kind::Float, Value::F32(v)) => Ok(Value::F32(v)),
        ("double", Kind::Double, Value::F64(v)) => Ok(Value::F64(v)),
        ("enum", Kind::Enum(_), Value::I32(v)) => Ok(Value::EnumNumber(v)),
        (_, Kind::Int32, Value::I32(v)) => Ok(Value::I32(v)),
        (_, Kind::Int64, Value::I32(v)) => Ok(Value::I64(i64::from(v))),
        (_, Kind::Int64, Value::I64(v)) => Ok(Value::I64(v)),
        (_, Kind::Sint32, Value::I32(v)) => Ok(Value::I32(v)),
        (_, Kind::Sint64, Value::I32(v)) => Ok(Value::I64(i64::from(v))),
        (_, Kind::Sint64, Value::I64(v)) => Ok(Value::I64(v)),
        (_, Kind::Sfixed32, Value::I32(v)) => Ok(Value::I32(v)),
        (_, Kind::Sfixed64, Value::I32(v)) => Ok(Value::I64(i64::from(v))),
        (_, Kind::Sfixed64, Value::I64(v)) => Ok(Value::I64(v)),
        (_, Kind::Uint32, Value::U32(v)) => Ok(Value::U32(v)),
        (_, Kind::Uint64, Value::U32(v)) => Ok(Value::U64(u64::from(v))),
        (_, Kind::Uint64, Value::U64(v)) => Ok(Value::U64(v)),
        (_, Kind::Fixed32, Value::U32(v)) => Ok(Value::U32(v)),
        (_, Kind::Fixed64, Value::U32(v)) => Ok(Value::U64(u64::from(v))),
        (_, Kind::Fixed64, Value::U64(v)) => Ok(Value::U64(v)),
        (arm, kind, _) => Err(err(&format!(
            "(redact.value).{arm} does not match the field kind {kind:?}"
        ))),
    }
}

/// The element descriptor a repeated field's rules resolve against: the
/// field itself for lists, the entry's value field for maps.
fn element_field_desc(field: &FieldDescriptor) -> FieldDescriptor {
    if field.is_map() {
        let kind = field.kind();
        let entry = kind
            .as_message()
            .expect("a map field's kind is its entry message");
        return entry
            .get_field(2)
            .expect("a map entry declares its value as field 2");
    }
    field.clone()
}

/// Kind mismatch check for the scalar-transform arms.
fn expect_kind(field: &FieldDescriptor, kind: Kind, arm: &str) -> Result<(), PlanError> {
    let matches = match (&field.kind(), &kind) {
        // The enum kind carries the target enum's name; a rule on an
        // enum field matches regardless of which enum.
        (Kind::Enum(_), Kind::Enum(_)) => true,
        (a, b) => a == b,
    };
    if matches {
        Ok(())
    } else {
        Err(PlanError::new(
            field.full_name(),
            format!(
                "(redact.value).{arm} does not match the field kind {:?}",
                field.kind()
            ),
        ))
    }
}

/// Reads a message-typed arm of the `values` oneof.
fn message_arm(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
    name: &str,
) -> Result<DynamicMessage, PlanError> {
    match rules
        .get_field(&scalar_arm(&rules.descriptor(), name))
        .as_ref()
    {
        Value::Message(m) => Ok(m.clone()),
        _ => Err(PlanError::new(
            field.full_name(),
            format!("(redact.value).{name} did not decode"),
        )),
    }
}

/// Reads a string-typed arm of the `values` oneof.
fn string_arm(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
    name: &str,
) -> Result<String, PlanError> {
    match rules
        .get_field(&scalar_arm(&rules.descriptor(), name))
        .as_ref()
    {
        Value::String(s) => Ok(s.clone()),
        _ => Err(PlanError::new(
            field.full_name(),
            format!("(redact.value).{name} did not decode"),
        )),
    }
}

fn scalar_arm(
    desc: &prost_reflect::MessageDescriptor,
    name: &str,
) -> prost_reflect::FieldDescriptor {
    desc.get_field_by_name(name)
        .unwrap_or_else(|| panic!("FieldRules.{name} is declared"))
}

fn u32_field(msg: &DynamicMessage, name: &str) -> u32 {
    match msg.get_field(&scalar_arm(&msg.descriptor(), name)).as_ref() {
        Value::U32(v) => *v,
        Value::I32(v) => u32::try_from(*v).unwrap_or(0),
        _ => 0,
    }
}

fn bool_field(msg: &DynamicMessage, name: &str) -> Result<bool, String> {
    match msg.get_field(&scalar_arm(&msg.descriptor(), name)).as_ref() {
        Value::Bool(v) => Ok(*v),
        _ => Err(format!("(redact.{name}) did not decode to a bool")),
    }
}

fn default_str(msg: &DynamicMessage, name: &str, default: &str) -> String {
    match msg.get_field(&scalar_arm(&msg.descriptor(), name)).as_ref() {
        Value::String(s) if !s.is_empty() => s.clone(),
        _ => default.to_owned(),
    }
}
