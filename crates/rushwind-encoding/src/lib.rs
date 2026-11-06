//! Codec contract for the RushWind encoding domain.
//!
//! The core is tiny: a [`Codec`] is anything that
//! can marshal a serde-serializable value to bytes and unmarshal bytes
//! back, plus a name. A process-wide registry maps names
//! (case-insensitively) to codecs, so an application picks wire formats
//! by string.
//!
//! The type-erased surface rides on `erased-serde`: `marshal` takes a
//! `&dyn` erased-serialize and `unmarshal` hands back an erased
//! deserializer over the decoded bytes, which is what makes [`Codec`]
//! object-safe and the registry possible. The [`marshal`] and
//! [`unmarshal`] free functions restore full type inference at the call
//! site.
//!
//! # The Codec contract
//!
//! ```text
//! codec.marshal(&value)          ->  Vec<u8>
//! codec.unmarshal(bytes, decode) ->  ()   (parse + hand the erased
//!                                          deserializer to `decode`)
//! codec.name()                   ->  "json" | "msgpack" | ...
//! ```
//!
//! Engines live in sibling crates, one per format
//! (`rushwind-encoding-json`, `rushwind-encoding-msgpack`, …).
//! Each exposes a codec
//! constructor and a `register()` function that installs it in the
//! registry.
//!
//! # Registry
//!
//! ```no_run
//! use rushwind_encoding::{get_codec, marshal, unmarshal};
//!
//! rushwind_encoding_json::register();
//!
//! let codec = get_codec("JSON").expect("json is registered");
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Ping { seq: u32 }
//! let bytes = marshal(codec.as_ref(), &Ping { seq: 1 }).unwrap();
//! let round: Ping = unmarshal(codec.as_ref(), &bytes).unwrap();
//! # assert_eq!(round.seq, 1);
//! ```
//!
//! Registry semantics: names are lowercased before
//! lookup, a later registration with the same name overwrites the
//! earlier one, and unknown or empty names return `None`.
//!
//! # Design notes
//!
//! - **No `init()` self-registration.** Rust has
//!   no import-time lifecycle hook, so each engine exposes an explicit
//!   `register()` the application calls once at startup.
//! - **`register_codec` returns `Err`** on an empty codec name instead
//!   of panicking.
//! - **gob, thrift, avro and flatbuffers have no engines.** gob has no
//!   cross-language meaning; thrift, avro
//!   and flatbuffers are schema/interface-driven formats whose codecs
//!   lean on runtime type assertions that have no Rust
//!   equivalent. They can join as engines later if a need shows up.
//! - **proto is not a registry member.** A registry codec that
//!   type-asserts `v.(proto.Message)` at runtime has no object-safe
//!   Rust twin: `prost::Message` is bound at compile time, so binary
//!   proto lives in `rushwind-encoding-proto`
//!   as a typed sidecar rather than behind this object-safe trait.
//! - **No MIME/content-type mapping.** That belongs to an
//!   HTTP middleware layer, not the encoding core.
//! - **No broker `Marshal`/`Unmarshal` fallback** (bytes/string
//!   pass-through with a built-in default codec). RushWind's broker contract
//!   carries bytes on the wire and leaves encoding to the edges.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod depth;

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, OnceLock, RwLock},
};

use erased_serde::{Deserializer as ErasedDeserializer, Serialize as ErasedSerialize};

/// The error surface of the encoding domain. Detail rides in the
/// message, per the workspace error taxonomy
/// (`RegistryError`/`BrokerError` style).
#[derive(Debug)]
#[non_exhaustive]
pub enum EncodingError {
    /// The codec could not complete the operation.
    Failed(String),
}

impl fmt::Display for EncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "encoding operation failed: {msg}"),
        }
    }
}

impl std::error::Error for EncodingError {}

/// The contract every concrete codec (json, msgpack, yaml, …)
/// satisfies, with serde's
/// `Serialize`/`Deserialize` as the value model.
///
/// Implementations live in the per-format engine crates; the trait is
/// object-safe so codecs can be traded through the registry as
/// `Arc<dyn Codec>`.
pub trait Codec: Send + Sync {
    /// The codec's registry name (e.g. `"json"`).
    fn name(&self) -> &'static str;

    /// Encodes a serde-serializable value into bytes.
    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError>;

    /// Parses `data` in this codec's format and hands the erased
    /// deserializer to `decode`, which reads a concrete type out of it.
    /// The [`unmarshal`] free function supplies the target type; the
    /// engine only owns the parse. The callback shape is what keeps the
    /// trait object-safe: most serde deserializers implement
    /// `serde::Deserializer` for `&mut self`, so an owned boxed
    /// hand-off cannot borrow the parser.
    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError>;
}

/// The process-wide name-to-codec registry. Keys are lowercased codec
/// names; a later registration under the same name wins.
fn codecs() -> &'static RwLock<HashMap<String, Arc<dyn Codec>>> {
    static CODECS: OnceLock<RwLock<HashMap<String, Arc<dyn Codec>>>> = OnceLock::new();
    CODECS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Installs a codec in the registry under its [`Codec::name`]
/// (lowercased). A later registration with the same name overwrites the
/// earlier one. An empty name is rejected with an error.
pub fn register_codec(codec: impl Codec + 'static) -> Result<(), EncodingError> {
    let name = codec.name().to_lowercase();
    if name.is_empty() {
        return Err(EncodingError::Failed("codec name cannot be empty".into()));
    }
    codecs()
        .write()
        .expect("codec registry poisoned")
        .insert(name, Arc::new(codec));
    Ok(())
}

/// Looks up the codec registered under `name`. Lookup is
/// case-insensitive; unknown or empty names return `None`.
pub fn get_codec(name: &str) -> Option<Arc<dyn Codec>> {
    if name.is_empty() {
        return None;
    }
    codecs()
        .read()
        .expect("codec registry poisoned")
        .get(&name.to_lowercase())
        .cloned()
}

/// Marshals `value` with `codec` — the typed convenience over
/// [`Codec::marshal`].
///
/// ```
/// # use rushwind_encoding::{get_codec, marshal, unmarshal};
/// rushwind_encoding_json::register();
/// let codec = get_codec("json").unwrap();
/// let bytes = marshal(codec.as_ref(), &vec![1u8, 2, 3]).unwrap();
/// let round: Vec<u8> = unmarshal(codec.as_ref(), &bytes).unwrap();
/// assert_eq!(round, vec![1, 2, 3]);
/// ```
pub fn marshal<T>(codec: &dyn Codec, value: &T) -> Result<Vec<u8>, EncodingError>
where
    T: serde::Serialize,
{
    codec.marshal(value)
}

/// Unmarshals `data` into a `T` with `codec` — the typed convenience
/// over [`Codec::unmarshal`].
///
/// ```
/// # use rushwind_encoding::{get_codec, marshal, unmarshal};
/// rushwind_encoding_json::register();
/// let codec = get_codec("json").unwrap();
/// let bytes = marshal(codec.as_ref(), &vec![1u8, 2, 3]).unwrap();
/// let round: Vec<u8> = unmarshal(codec.as_ref(), &bytes).unwrap();
/// assert_eq!(round, vec![1, 2, 3]);
/// ```
pub fn unmarshal<'de, T>(codec: &dyn Codec, data: &'de [u8]) -> Result<T, EncodingError>
where
    T: serde::Deserialize<'de>,
{
    let mut slot: Option<T> = None;
    codec.unmarshal(data, &mut |deserializer| {
        slot = Some(
            erased_serde::deserialize(deserializer)
                .map_err(|e| EncodingError::Failed(format!("{} decode: {e}", codec.name())))?,
        );
        Ok(())
    })?;
    slot.ok_or_else(|| EncodingError::Failed(format!("{} decode filled no value", codec.name())))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal codec for exercising registry semantics.
    struct PassthroughCodec(&'static str);

    impl Codec for PassthroughCodec {
        fn name(&self) -> &'static str {
            self.0
        }

        fn marshal(&self, _value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
            Ok(Vec::new())
        }

        fn unmarshal<'de>(
            &self,
            data: &'de [u8],
            decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
        ) -> Result<(), EncodingError> {
            let mut deserializer = serde_json::Deserializer::from_slice(data);
            decode(&mut <dyn ErasedDeserializer>::erase(&mut deserializer))
        }
    }

    #[test]
    fn registry_is_case_insensitive_and_last_registration_wins() {
        register_codec(PassthroughCodec("CaseProbe")).unwrap();

        let first = get_codec("caseprobe").expect("lowercase lookup");
        let again = get_codec("CASEPROBE").expect("uppercase lookup");
        assert!(Arc::ptr_eq(&first, &again), "same codec under both cases");

        // Overwrite: a second registration under the same name replaces
        // the first.
        register_codec(PassthroughCodec("CASEPROBE")).unwrap();
        let replaced = get_codec("caseprobe").expect("still registered");
        assert_eq!(replaced.name(), "CASEPROBE");
    }

    #[test]
    fn empty_and_unknown_names_miss() {
        assert!(get_codec("").is_none(), "empty name never resolves");
        assert!(get_codec("no-such-codec").is_none());
    }

    #[test]
    fn empty_codec_name_is_rejected() {
        assert!(register_codec(PassthroughCodec("")).is_err());
    }
}
