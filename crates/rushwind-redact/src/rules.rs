//! The resolved rule vocabulary — what a `(redact.value)` option
//! compiles down to once the plan build has validated it against the
//! field's kind.

use prost_reflect::Value;

/// A per-value transform for scalar fields (and per-element transforms
/// under [`FieldRule::ElementItem`]).
#[derive(Debug, Clone)]
pub(crate) enum ScalarRule {
    /// Fixed replacement — the scalar arms of the `values` oneof
    /// (`(redact.value).string/.int32/.bool/...`). The reference
    /// assigns the literal (`x.Password = &""`).
    Fixed(Value),
    /// Position mask — the reference's `_redactMask(s, keepFirst,
    /// keepLast, maskChar)`: `len(s) <= keep_first + keep_last` leaves
    /// the value unchanged, else keep-head + mask-run + keep-tail.
    Mask {
        keep_first: u32,
        keep_last: u32,
        mask_char: String,
    },
    /// Email mask — the reference's `_redactEmail(s, keepLocalFirst,
    /// maskDomain, maskChar)`: no `@` leaves the value unchanged; the
    /// local part keeps its first `keep_local_first` chars (only when
    /// longer), the domain is fully masked under `mask_domain`.
    Email {
        keep_local_first: u32,
        mask_domain: bool,
        mask_char: String,
    },
    /// Truncation — keep the first `length` chars, then the suffix
    /// (unchanged when already short enough).
    Truncate { length: u32, suffix: String },
    /// Fixed-length mask — the whole value replaced by `mask_char`
    /// repeated to the value's length.
    FixedLength { mask_char: String },
}

/// A validated per-field rule, keyed by field number in the plan's
/// per-message table.
#[derive(Debug, Clone)]
pub(crate) enum FieldRule {
    /// A scalar (or fixed) transform applied to the present value.
    Scalar(ScalarRule),
    /// Repeated/map: the whole container cleared
    /// (`(redact.value).element = { empty: true }`).
    ElementEmpty,
    /// Repeated message/map: each item's own type rules applied
    /// (`(redact.value).element = { nested: true }` — the reference's
    /// `redact.Apply(x.Items[k])` loop). Carries the item type's fully
    /// qualified name, resolved at plan build.
    ElementNested { item_type: String },
    /// Repeated/map scalar: the transform applied to each element
    /// value (`(redact.value).element = { item: {...} }`).
    ElementItem(ScalarRule),
}
