//! The plan's application to a response dynamic message — the port of
//! the reference's generated `Redact()` method bodies: present fields
//! only, in-place value replacement, per-item recursion for
//! `element = { nested: true }`.

use prost_reflect::{DynamicMessage, FieldDescriptor, ReflectMessage, Value};

use crate::plan::RedactPlan;
use crate::rules::{FieldRule, ScalarRule};

impl RedactPlan {
    /// Mutates the message in place: every present field carrying a
    /// plan rule is replaced (fixed value or string transform);
    /// repeated/map fields recurse or clear per their element rule.
    /// Absent fields are never touched — under the codec's
    /// `EmitUnpopulated` they serialize as defaults, exactly like the
    /// reference's untouched nil pointers.
    pub fn apply_message(&self, message_fq: &str, msg: &mut DynamicMessage) {
        let Some(rules) = self.messages.get(message_fq) else {
            return;
        };
        for (&number, rule) in rules {
            let Some(field) = msg.descriptor().get_field(number) else {
                continue;
            };
            if !msg.has_field(&field) {
                continue;
            }
            match rule {
                FieldRule::Scalar(scalar) => {
                    let current = msg.get_field(&field).into_owned();
                    msg.set_field(&field, apply_scalar(scalar, current));
                }
                FieldRule::ElementEmpty => msg.clear_field(&field),
                FieldRule::ElementNested { item_type } => {
                    for_each_element(msg, &field, |item| {
                        if let Value::Message(item_msg) = item {
                            self.apply_message(item_type, item_msg);
                        }
                    });
                }
                FieldRule::ElementItem(scalar) => {
                    for_each_element(msg, &field, |item| {
                        *item = apply_scalar(scalar, item.clone());
                    });
                }
            }
        }
    }
}

/// Walks the elements of a present repeated/map field — lists and map
/// values share the element transform, keys never change.
fn for_each_element(
    msg: &mut DynamicMessage,
    field: &FieldDescriptor,
    mut redact: impl FnMut(&mut Value),
) {
    match msg.get_field_mut(field) {
        Value::List(items) => items.iter_mut().for_each(&mut redact),
        Value::Map(entries) => entries.values_mut().for_each(&mut redact),
        _ => {}
    }
}

/// Applies one scalar transform to a value. Fixed rules replace
/// wholesale; the string transforms require `Value::String` (the plan
/// build pins the rule to the field kind, so any other payload cannot
/// reach here) and pass through unchanged on the mismatch.
pub(crate) fn apply_scalar(rule: &ScalarRule, value: Value) -> Value {
    match rule {
        ScalarRule::Fixed(fixed) => fixed.clone(),
        ScalarRule::Mask {
            keep_first,
            keep_last,
            mask_char,
        } => match value {
            Value::String(s) => Value::String(mask(&s, *keep_first, *keep_last, mask_char)),
            other => other,
        },
        ScalarRule::Email {
            keep_local_first,
            mask_domain,
            mask_char,
        } => match value {
            Value::String(s) => {
                Value::String(mask_email(&s, *keep_local_first, *mask_domain, mask_char))
            }
            other => other,
        },
        ScalarRule::Truncate { length, suffix } => match value {
            Value::String(s) => Value::String(truncate(&s, *length, suffix)),
            other => other,
        },
        ScalarRule::FixedLength { mask_char } => match value {
            Value::String(s) => Value::String(mask_char.repeat(s.chars().count())),
            other => other,
        },
    }
}

/// The reference's `_redactMask` by CHAR (see the crate docs for the
/// byte/char note): short values unchanged, else keep-head + mask-run +
/// keep-tail.
fn mask(s: &str, keep_first: u32, keep_last: u32, mask_char: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let keep_first = keep_first as usize;
    let keep_last = keep_last as usize;
    if chars.len() <= keep_first + keep_last {
        return s.to_owned();
    }
    let masked = mask_char.repeat(chars.len() - keep_first - keep_last);
    let head: String = chars[..keep_first].iter().collect();
    let tail: String = chars[chars.len() - keep_last..].iter().collect();
    format!("{head}{masked}{tail}")
}

/// The reference's `_redactEmail`: no `@` leaves the value unchanged;
/// the local part keeps its head only when longer than
/// `keep_local_first`; `mask_domain` replaces the whole domain.
fn mask_email(s: &str, keep_local_first: u32, mask_domain: bool, mask_char: &str) -> String {
    let Some(at) = s.rfind('@') else {
        return s.to_owned();
    };
    let (local, domain) = (&s[..at], &s[at + 1..]);
    let keep_local_first = keep_local_first as usize;
    let masked_local = if local.chars().count() > keep_local_first {
        let head: String = local.chars().take(keep_local_first).collect();
        let run = mask_char.repeat(local.chars().count() - keep_local_first);
        format!("{head}{run}")
    } else {
        local.to_owned()
    };
    let masked_domain = if mask_domain {
        mask_char.repeat(domain.chars().count())
    } else {
        domain.to_owned()
    };
    format!("{masked_local}@{masked_domain}")
}

/// Keep the first `length` chars, then the suffix; short values
/// unchanged.
fn truncate(s: &str, length: u32, suffix: &str) -> String {
    let count = s.chars().count();
    if count <= length as usize {
        return s.to_owned();
    }
    let head: String = s.chars().take(length as usize).collect();
    format!("{head}{suffix}")
}
