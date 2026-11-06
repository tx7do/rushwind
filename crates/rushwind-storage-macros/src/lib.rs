//! Derive macros for the RushWind storage mapper pair.
//!
//! ```ignore
//! #[derive(ToRecord, FromRecord)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     score: Option<f64>,
//!     active: bool,
//! }
//! ```
//!
//! # Field types
//!
//! The contract's scalar model is the closed [`Value`] enum, and the
//! derives map onto it exactly:
//!
//! | Rust type | encodes as |
//! |:---|:---|
//! | `String` | [`Value::Text`] |
//! | `i8` `i16` `i32` `i64` `u8` `u16` `u32` | [`Value::Int`] (widened to i64 on write, cast back on read) |
//! | `f32` `f64` | [`Value::Real`] |
//! | `bool` | [`Value::Bool`] |
//! | `Option<_>` of any of the above / of an attributed type | `None` ↔ [`Value::Null`] |
//!
//! `u64`/`i128`/`u128` are rejected — they exceed the contract's `Int(i64)`
//! carrier and silent wrap-around is not a trade this crate makes.
//!
//! # Field attributes
//!
//! Three `#[record(…)]` attributes cover everything the scalar model
//! does not:
//!
//! - **`#[record(rename = "column")]`** — the Record key (and therefore
//!   the engine column) instead of the Rust field name.
//! - **`#[record(as_text)]`** — a string enum: writes `Value::Text` via
//!   the type's `as_str(&self) -> &str`, reads back through
//!   `std::str::FromStr` (a parse failure is
//!   [`StorageError::InvalidQuery`]). The usual shape:
//!
//!   ```ignore
//!   #[derive(ToRecord, FromRecord)]
//!   struct Task {
//!       #[record(as_text)]
//!       status: TaskStatus,          // as_str + FromStr
//!   }
//!   ```
//!
//! - **`#[record(with = "path")]`** — the escape hatch for everything
//!   else (timestamps, JSON columns, byte blobs, newtype ids): `path` is
//!   a conversion module the *user's* crate owns, so the macro crate
//!   stays dependency-free. The module must expose:
//!
//!   ```ignore
//!   mod timestamp {
//!       // any custom Rust type ⇄ Value
//!       pub fn to_value(v: &MyType) -> rushwind_storage::Value;
//!       pub fn from_value(v: &rushwind_storage::Value)
//!           -> Result<MyType, rushwind_storage::StorageError>;
//!   }
//!
//!   #[derive(ToRecord, FromRecord)]
//!   struct Row {
//!       #[record(with = "timestamp")]
//!       created_at: MyTimestamp,
//!   }
//!   ```
//!
//! A missing or wrongly-typed field on the read face is
//! [`StorageError::InvalidQuery`](rushwind_storage::StorageError::InvalidQuery).

#![forbid(unsafe_code)]
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, LitStr, Path, Type};

/// One supported field shape: how the Rust type crosses the
/// [`Value`](rushwind_storage::Value) boundary, and the concrete
/// primitive when a cast is involved.
enum Kind {
    /// `String` — no cast.
    Text,
    /// `i8…i64`, `u8…u32` — widened on write, cast back on read.
    Int { ty: Ident },
    /// `f32`/`f64` — widened on write, cast back on read.
    Real { ty: Ident },
    /// `bool`.
    Bool,
    /// `#[record(as_text)]` — a string enum with `as_str` + `FromStr`.
    EnumText,
    /// `#[record(with = "path")]` — user-owned conversion module.
    With { path: Path },
}

/// The parsed `#[record(…)]` field attributes.
#[derive(Default)]
struct FieldAttrs {
    rename: Option<String>,
    as_text: bool,
    with: Option<Path>,
}

fn attrs_of(field: &syn::Field) -> syn::Result<FieldAttrs> {
    let mut attrs = FieldAttrs::default();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("record")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: LitStr = meta.value()?.parse()?;
                attrs.rename = Some(value.value());
                Ok(())
            } else if meta.path.is_ident("as_text") {
                attrs.as_text = true;
                Ok(())
            } else if meta.path.is_ident("with") {
                let value: LitStr = meta.value()?.parse()?;
                attrs.with = Some(syn::parse_str(&value.value()).map_err(|_| {
                    syn::Error::new_spanned(&value, "`with` expects a module path string")
                })?);
                Ok(())
            } else {
                Err(meta.error("unknown `record` attribute; expected rename, as_text or with"))
            }
        })?;
    }
    if attrs.as_text && attrs.with.is_some() {
        return Err(syn::Error::new_spanned(
            field,
            "`as_text` and `with` are mutually exclusive",
        ));
    }
    Ok(attrs)
}

/// Resolves one field's kind: attributes first, then the primitive
/// table. `nullable` is whether an `Option<_>` wraps the type.
fn kind_of(ty: &Type, attrs: &FieldAttrs) -> syn::Result<(Kind, bool)> {
    let Type::Path(path) = ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "unsupported field type: expected a scalar, a string enum with `as_text`, or `with`",
        ));
    };
    let segment = path
        .path
        .segments
        .last()
        .expect("a type path has at least one segment");
    let nullable = segment.ident == "Option";
    if nullable {
        let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
            return Err(syn::Error::new_spanned(
                ty,
                "Option needs an explicit type argument",
            ));
        };
        let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
            return Err(syn::Error::new_spanned(
                ty,
                "Option needs an explicit type argument",
            ));
        };
        if attrs.with.is_some() {
            return Ok((
                Kind::With {
                    path: attrs.with.clone().expect("checked above"),
                },
                true,
            ));
        }
        if attrs.as_text {
            return Ok((Kind::EnumText, true));
        }
        let (kind, nested) = kind_of(inner, &FieldAttrs::default())?;
        if nested {
            return Err(syn::Error::new_spanned(
                ty,
                "Option<Option<_>> is not supported",
            ));
        }
        return Ok((kind, true));
    }
    if let Some(path) = &attrs.with {
        return Ok((Kind::With { path: path.clone() }, false));
    }
    if attrs.as_text {
        return Ok((Kind::EnumText, false));
    }
    let ident = &segment.ident;
    let kind = match ident.to_string().as_str() {
        "String" => Kind::Text,
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" => Kind::Int { ty: ident.clone() },
        "f32" | "f64" => Kind::Real { ty: ident.clone() },
        "bool" => Kind::Bool,
        "u64" | "i128" | "u128" => {
            return Err(syn::Error::new_spanned(
                ty,
                format!(
                    "unsupported field type {ident}: it exceeds the contract's Int(i64) carrier; \
                     use i64, or a `with` conversion that owns the narrowing"
                ),
            ))
        }
        other => {
            return Err(syn::Error::new_spanned(
                ty,
                format!(
                    "unsupported field type {other}: the derives support String, the Int(i64)-fit \
                     integers, f32/f64, bool, and Option<_> of those; enums take `as_text`, \
                     everything else takes `with`"
                ),
            ))
        }
    };
    Ok((kind, false))
}

struct Field {
    name: Ident,
    column: String,
    nullable: bool,
    kind: Kind,
}

fn fields_of(input: &DeriveInput) -> syn::Result<Vec<Field>> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "the storage derives apply only to structs",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            input,
            "the storage derives need named fields",
        ));
    };
    named
        .named
        .iter()
        .map(|field| {
            let Some(name) = &field.ident else {
                return Err(syn::Error::new_spanned(field, "expected a named field"));
            };
            let attrs = attrs_of(field)?;
            let (kind, nullable) = kind_of(&field.ty, &attrs)?;
            let column = attrs.rename.unwrap_or_else(|| name.to_string());
            Ok(Field {
                name: name.clone(),
                column,
                nullable,
                kind,
            })
        })
        .collect()
}

#[proc_macro_derive(ToRecord, attributes(record))]
pub fn derive_to_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match fields_of(&input) {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };
    let writes = fields.iter().map(|field| {
        let column = &field.column;
        let expression = write_expression(field);
        quote! { record.insert(#column, #expression); }
    });
    let expanded = quote! {
        impl ::rushwind_storage::ToRecord for #name {
            fn to_record(&self) -> ::rushwind_storage::Record {
                let mut record = ::rushwind_storage::Record::new();
                #(#writes)*
                record
            }
        }
    };
    expanded.into()
}

/// The `self.#name → Value` expression for one field.
fn write_expression(field: &Field) -> TokenStream2 {
    let name = &field.name;
    match (&field.kind, field.nullable) {
        (Kind::Text, false) => quote! { ::rushwind_storage::Value::Text(self.#name.clone()) },
        (Kind::Int { .. }, false) => {
            quote! { ::rushwind_storage::Value::Int(self.#name as i64) }
        }
        (Kind::Real { .. }, false) => {
            quote! { ::rushwind_storage::Value::Real(self.#name as f64) }
        }
        (Kind::Bool, false) => quote! { ::rushwind_storage::Value::Bool(self.#name) },
        (Kind::EnumText, false) => quote! {
            ::rushwind_storage::Value::Text(self.#name.as_str().to_owned())
        },
        (Kind::With { path }, false) => quote! { #path::to_value(&self.#name) },
        (Kind::Text, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Text(v.clone()),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Kind::Int { .. }, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Int(*v as i64),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Kind::Real { .. }, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Real(*v as f64),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Kind::Bool, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Bool(*v),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Kind::EnumText, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Text(v.as_str().to_owned()),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Kind::With { path }, true) => quote! {
            match &self.#name {
                Some(v) => #path::to_value(v),
                None => ::rushwind_storage::Value::Null,
            }
        },
    }
}

#[proc_macro_derive(FromRecord, attributes(record))]
pub fn derive_from_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match fields_of(&input) {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };
    let reads = fields.iter().map(|field| {
        let name = &field.name;
        let column = &field.column;
        let expected = match &field.kind {
            Kind::Text | Kind::EnumText => "text",
            Kind::Int { .. } => "int",
            Kind::Real { .. } => "real",
            Kind::Bool => "bool",
            Kind::With { .. } => "a convertible value",
        };
        let missing_message = LitStr::new(
            &format!("field {column:?} is missing from the record"),
            name.span(),
        );
        let kind_mismatch_message = LitStr::new(
            &format!("field {column:?} expects {expected}, got {{}}"),
            name.span(),
        );
        let read = read_expression(field, &kind_mismatch_message);
        if field.nullable {
            quote! {
                #name: match record.get(#column) {
                    None | Some(::rushwind_storage::Value::Null) => None,
                    Some(value) => Some(#read),
                }
            }
        } else {
            quote! {
                #name: match record.get(#column) {
                    Some(value) => #read,
                    None => {
                        return Err(::rushwind_storage::StorageError::InvalidQuery(
                            #missing_message.to_owned(),
                        ))
                    }
                }
            }
        }
    });
    let expanded = quote! {
        impl ::rushwind_storage::FromRecord for #name {
            fn from_record(record: &::rushwind_storage::Record) -> ::std::result::Result<Self, ::rushwind_storage::StorageError> {
                Ok(Self {
                    #(#reads,)*
                })
            }
        }
    };
    expanded.into()
}

/// The `&Value → field type` expression for one (present, non-null)
/// record entry. `#mismatch` formats the offending value's kind.
fn read_expression(field: &Field, mismatch: &LitStr) -> TokenStream2 {
    let name = &field.name;
    let match_arms = |success: TokenStream2| {
        quote! {
            match value {
                #success,
                other => {
                    return Err(::rushwind_storage::StorageError::InvalidQuery(format!(
                        #mismatch,
                        other.type_name()
                    )))
                }
            }
        }
    };
    match &field.kind {
        Kind::Text => match_arms(quote! { ::rushwind_storage::Value::Text(s) => s.clone() }),
        Kind::Int { ty } => match_arms(quote! { ::rushwind_storage::Value::Int(i) => *i as #ty }),
        Kind::Real { ty } => match_arms(quote! { ::rushwind_storage::Value::Real(r) => *r as #ty }),
        Kind::Bool => match_arms(quote! { ::rushwind_storage::Value::Bool(b) => *b }),
        Kind::EnumText => {
            let parse_failure = LitStr::new(
                &format!(
                    "field {:?} does not parse from the record text {{}}: {{}}",
                    field.column
                ),
                name.span(),
            );
            quote! {
                match value {
                    ::rushwind_storage::Value::Text(s) => {
                        s.parse().map_err(|error| {
                            ::rushwind_storage::StorageError::InvalidQuery(format!(
                                #parse_failure,
                                s,
                                error
                            ))
                        })?
                    }
                    other => {
                        return Err(::rushwind_storage::StorageError::InvalidQuery(format!(
                            #mismatch,
                            other.type_name()
                        )))
                    }
                }
            }
        }
        Kind::With { path } => quote! {
            #path::from_value(value)?
        },
    }
}
