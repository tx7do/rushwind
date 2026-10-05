//! The plan builder — one walk over the caller's descriptor pool that
//! resolves every `(redact.v1)` option into the per-message rule table
//! and the operation skip set, refusing anything the runtime does not
//! implement.

use std::collections::{HashMap, HashSet};

use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, ExtensionDescriptor, FieldDescriptor, Kind,
    MessageDescriptor, ReflectMessage, Value,
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
        let schema = Schema::resolve(pool);
        let file_skips = collect_file_skips(pool, &schema)?;
        Ok(RedactPlan {
            skipped_operations: collect_operation_skips(pool, &schema)?,
            messages: collect_message_rules(pool, &schema, &file_skips)?,
        })
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

/// Every `(redact.v1)` extension the runtime knows, position-paired
/// with [`EXT_NAMES`].
#[derive(Clone, Copy)]
enum Ext {
    FieldValue,
    FileSkip,
    AutoDetect,
    ServiceSkip,
    ServiceInternal,
    ServiceInternalCode,
    ServiceInternalMessage,
    MethodSkip,
    MethodInternal,
    MethodInternalCode,
    MethodInternalMessage,
    MessageNil,
    MessageEmpty,
    MessageIgnored,
}

/// The `(redact.v1)` extension names, paired with [`Ext`] by position.
const EXT_NAMES: [&str; 14] = [
    "redact.value",
    "redact.file_skip",
    "redact.auto_detect",
    "redact.service_skip",
    "redact.internal_service",
    "redact.internal_service_code",
    "redact.internal_service_err_message",
    "redact.method_skip",
    "redact.internal_method",
    "redact.internal_method_code",
    "redact.internal_method_err_message",
    "redact.nil",
    "redact.empty",
    "redact.ignored",
];

/// The `(redact.v1)` extension descriptors the pool declares, resolved
/// once per build. A `None` slot means the compile closure has no
/// vendored redact schema — every option of that kind reads as unset.
struct Schema([Option<ExtensionDescriptor>; EXT_NAMES.len()]);

impl Schema {
    fn resolve(pool: &DescriptorPool) -> Self {
        Self(EXT_NAMES.map(|name| pool.get_extension_by_name(name)))
    }

    fn get(&self, ext: Ext) -> Option<&ExtensionDescriptor> {
        self.0[ext as usize].as_ref()
    }

    /// Presence check — absent descriptors read as unset.
    fn is_set(&self, options: &DynamicMessage, ext: Ext) -> bool {
        self.get(ext)
            .is_some_and(|desc| options.has_extension(desc))
    }

    /// Boolean option value — absent descriptors or unset options read
    /// false; a non-bool payload is a build failure.
    fn bool_opt(&self, options: &DynamicMessage, ext: Ext) -> Result<bool, PlanError> {
        let Some(desc) = self.get(ext) else {
            return Ok(false);
        };
        if !options.has_extension(desc) {
            return Ok(false);
        }
        match options.get_extension(desc).as_ref() {
            Value::Bool(value) => Ok(*value),
            _ => Err(PlanError::new(
                desc.full_name(),
                "boolean option did not decode to a bool",
            )),
        }
    }
}

/// Refuses the file-level vocabulary the runtime does not implement and
/// collects the `(redact.file_skip)` set — files whose messages never
/// enter the rule table.
fn collect_file_skips(
    pool: &DescriptorPool,
    schema: &Schema,
) -> Result<HashSet<String>, PlanError> {
    let mut skipped = HashSet::new();
    for file in pool.files() {
        let options = file.options();
        if schema.is_set(&options, Ext::AutoDetect) {
            return Err(PlanError::new(
                file.name(),
                "(redact.auto_detect) is not supported; annotate the fields explicitly",
            ));
        }
        if schema.bool_opt(&options, Ext::FileSkip)? {
            skipped.insert(file.name().to_owned());
        }
    }
    Ok(skipped)
}

/// Refuses the internal-service/method denial vocabulary and collects
/// the `(redact.method_skip)` set — the operations whose responses
/// serialize without redaction.
fn collect_operation_skips(
    pool: &DescriptorPool,
    schema: &Schema,
) -> Result<HashSet<String>, PlanError> {
    let mut skipped = HashSet::new();
    for service in pool.services() {
        let options = service.options();
        if schema.is_set(&options, Ext::ServiceInternal)
            || schema.is_set(&options, Ext::ServiceInternalCode)
            || schema.is_set(&options, Ext::ServiceInternalMessage)
        {
            return Err(PlanError::new(
                service.full_name(),
                "(redact.internal_service*) is not supported (internal-service denial)",
            ));
        }
        if schema.bool_opt(&options, Ext::ServiceSkip)? {
            continue;
        }
        for method in service.methods() {
            let options = method.options();
            if schema.is_set(&options, Ext::MethodInternal)
                || schema.is_set(&options, Ext::MethodInternalCode)
                || schema.is_set(&options, Ext::MethodInternalMessage)
            {
                return Err(PlanError::new(
                    method.full_name(),
                    "(redact.internal_method*) is not supported (internal-method denial)",
                ));
            }
            if schema.bool_opt(&options, Ext::MethodSkip)? {
                skipped.insert(format!("{}/{}", service.full_name(), method.name()));
            }
        }
    }
    Ok(skipped)
}

/// Resolves every field-level `(redact.value)` into the per-message
/// rule table, skipping `redact.ignored` messages and refusing the
/// message-level nil/empty vocabulary.
fn collect_message_rules(
    pool: &DescriptorPool,
    schema: &Schema,
    file_skips: &HashSet<String>,
) -> Result<HashMap<String, HashMap<u32, FieldRule>>, PlanError> {
    let Some(value_ext) = schema.get(Ext::FieldValue) else {
        return Ok(HashMap::new());
    };
    let mut messages = HashMap::new();
    for message in pool.all_messages() {
        if file_skips.contains(message.parent_file().name()) {
            continue;
        }
        let options = message.options();
        if schema.bool_opt(&options, Ext::MessageIgnored)? {
            continue;
        }
        if schema.bool_opt(&options, Ext::MessageNil)?
            || schema.bool_opt(&options, Ext::MessageEmpty)?
        {
            return Err(PlanError::new(
                message.full_name(),
                "(redact.nil)/(redact.empty) message-level redaction is not supported",
            ));
        }
        let mut rules = HashMap::new();
        for field in message.fields() {
            let field_options = field.options();
            if !field_options.has_extension(value_ext) {
                continue;
            }
            let Value::Message(field_rules) = field_options.get_extension(value_ext).into_owned()
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
            messages.insert(message.full_name().to_string(), rules);
        }
    }
    Ok(messages)
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
    let arm = rules
        .descriptor()
        .fields()
        .find(|arm| rules.has_field(arm))
        .ok_or_else(|| PlanError::new(field.full_name(), "(redact.value) sets no rule"))?
        .name()
        .to_owned();

    match arm.as_str() {
        "message" => resolve_message_rules(field, rules),
        "element" => resolve_element_rules(field, rules),
        "mask" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = arm_message(field, rules, "mask")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Mask {
                keep_first: u32_field(&m, "keep_first"),
                keep_last: u32_field(&m, "keep_last"),
                mask_char: default_str(&m, "mask_char", "*"),
            })))
        }
        "email" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = arm_message(field, rules, "email")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Email {
                keep_local_first: u32_field(&m, "keep_local_first"),
                mask_domain: bool_field(&m, "mask_domain")
                    .map_err(|e| PlanError::new(field.full_name(), e))?,
                mask_char: default_str(&m, "mask_char", "*"),
            })))
        }
        "truncate" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = arm_message(field, rules, "truncate")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Truncate {
                length: u32_field(&m, "length"),
                suffix: default_str(&m, "suffix", "..."),
            })))
        }
        "fixed_length" => {
            expect_kind(field, Kind::String, &arm)?;
            let m = arm_message(field, rules, "fixed_length")?;
            Ok(Some(FieldRule::Scalar(ScalarRule::FixedLength {
                mask_char: default_str(&m, "char", "X"),
            })))
        }
        "string" => fixed_scalar(field, rules, &arm, Kind::String),
        "bytes" => fixed_scalar(field, rules, &arm, Kind::Bytes),
        "bool" => fixed_scalar(field, rules, &arm, Kind::Bool),
        "float" | "double" | "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64"
        | "fixed32" | "fixed64" | "sfixed32" | "sfixed64" | "enum" => {
            let fixed = numeric_fixed(field, &arm, arm_value(rules, &arm))?;
            Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(fixed))))
        }
        other => Err(PlanError::new(
            field.full_name(),
            format!("(redact.value).{other} is not supported"),
        )),
    }
}

/// The `message` arm: only `skip` — the explicit no-op — is supported;
/// nil/empty/apply change the field's shape.
fn resolve_message_rules(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
) -> Result<Option<FieldRule>, PlanError> {
    let message_rules = arm_message(field, rules, "message")?;
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

/// The `element` arm: exactly one of empty/nested/item, resolved
/// against the ELEMENT's descriptor (for maps, the entry's value
/// field) so kind validation sees the item, not the list.
fn resolve_element_rules(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
) -> Result<Option<FieldRule>, PlanError> {
    if field.cardinality() != Cardinality::Repeated {
        return Err(PlanError::new(
            field.full_name(),
            "(redact.value).element on a non-repeated field",
        ));
    }
    let element = arm_message(field, rules, "element")?;
    let element_field = element_field_desc(field);
    let empty_set =
        bool_field(&element, "empty").map_err(|e| PlanError::new(field.full_name(), e))?;
    let nested_set =
        bool_field(&element, "nested").map_err(|e| PlanError::new(field.full_name(), e))?;
    let item_set = element.has_field_by_name("item");
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
    // `item`: an inner FieldRules applied to each element — scalar
    // transforms only.
    let Value::Message(item_rules) = arm_value(&element, "item") else {
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

/// The string/bytes/bool fixed-replacement arms — each carries exactly
/// its own scalar type as the payload.
fn fixed_scalar(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
    arm: &str,
    kind: Kind,
) -> Result<Option<FieldRule>, PlanError> {
    expect_kind(field, kind, arm)?;
    let payload = match arm_value(rules, arm) {
        payload @ (Value::String(_) | Value::Bytes(_) | Value::Bool(_)) => payload,
        _ => {
            return Err(PlanError::new(
                field.full_name(),
                format!("unreadable {arm} rule"),
            ))
        }
    };
    Ok(Some(FieldRule::Scalar(ScalarRule::Fixed(payload))))
}

/// The numeric arms' expected field kind — the arm↔kind contract in one
/// table (the enum arm expects any enum and resolves separately).
fn numeric_arm_kind(arm: &str) -> Option<Kind> {
    match arm {
        "float" => Some(Kind::Float),
        "double" => Some(Kind::Double),
        "int32" | "sint32" | "sfixed32" => Some(Kind::Int32),
        "int64" | "sint64" | "sfixed64" => Some(Kind::Int64),
        "uint32" | "fixed32" => Some(Kind::Uint32),
        "uint64" | "fixed64" => Some(Kind::Uint64),
        _ => None,
    }
}

/// A numeric/enum fixed value. The arm↔kind check first pins the
/// field's kind to the arm's table entry; past that the rule's prost
/// type already IS the target `Value` variant (both follow the arm's
/// declared proto type) — the enum arm alone renumbers its int32 rule
/// value into an `EnumNumber`.
fn numeric_fixed(field: &FieldDescriptor, arm: &str, raw: Value) -> Result<Value, PlanError> {
    let mismatch = || {
        PlanError::new(
            field.full_name(),
            format!(
                "(redact.value).{arm} does not match the field kind {:?}",
                field.kind()
            ),
        )
    };
    match arm {
        "enum" => {
            if !matches!(field.kind(), Kind::Enum(_)) {
                return Err(mismatch());
            }
            match raw {
                Value::I32(value) => Ok(Value::EnumNumber(value)),
                _ => Err(mismatch()),
            }
        }
        _ => {
            match numeric_arm_kind(arm) {
                Some(expected) if field.kind() == expected => {}
                _ => return Err(mismatch()),
            }
            match raw {
                raw @ (Value::F32(_)
                | Value::F64(_)
                | Value::I32(_)
                | Value::I64(_)
                | Value::U32(_)
                | Value::U64(_)) => Ok(raw),
                _ => Err(mismatch()),
            }
        }
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

/// Reads one `values`-oneof arm's payload (the arm is guaranteed set —
/// the dispatch found it).
fn arm_value(rules: &DynamicMessage, name: &str) -> Value {
    rules
        .get_field(&declared_field(&rules.descriptor(), name))
        .into_owned()
}

/// Reads a message-typed arm of the `values` oneof.
fn arm_message(
    field: &FieldDescriptor,
    rules: &DynamicMessage,
    name: &str,
) -> Result<DynamicMessage, PlanError> {
    match arm_value(rules, name) {
        Value::Message(m) => Ok(m),
        _ => Err(PlanError::new(
            field.full_name(),
            format!("(redact.value).{name} did not decode"),
        )),
    }
}

/// A field descriptor by name on a rule/option message — every name
/// here is a declared FieldRules/ElementRules/... field.
fn declared_field(desc: &MessageDescriptor, name: &str) -> FieldDescriptor {
    desc.get_field_by_name(name)
        .unwrap_or_else(|| panic!("FieldRules.{name} is declared"))
}

fn u32_field(msg: &DynamicMessage, name: &str) -> u32 {
    match msg
        .get_field(&declared_field(&msg.descriptor(), name))
        .as_ref()
    {
        Value::U32(v) => *v,
        Value::I32(v) => u32::try_from(*v).unwrap_or(0),
        _ => 0,
    }
}

fn bool_field(msg: &DynamicMessage, name: &str) -> Result<bool, String> {
    match msg
        .get_field(&declared_field(&msg.descriptor(), name))
        .as_ref()
    {
        Value::Bool(v) => Ok(*v),
        _ => Err(format!("(redact.{name}) did not decode to a bool")),
    }
}

fn default_str(msg: &DynamicMessage, name: &str, default: &str) -> String {
    match msg
        .get_field(&declared_field(&msg.descriptor(), name))
        .as_ref()
    {
        Value::String(s) if !s.is_empty() => s.clone(),
        _ => default.to_owned(),
    }
}
