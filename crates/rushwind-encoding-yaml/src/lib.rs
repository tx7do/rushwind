//! YAML engine for the RushWind encoding contract, over `serde_yaml`.
//!
//! A direct delegate. Payloads are UTF-8 YAML
//! text. Note for the record: `serde_yaml` is upstream-deprecated but
//! remains the most battle-tested YAML serde implementation — an
//! accepted, maintenance-mode choice. Its deserializer enforces its
//! own recursion limit (`RecursionLimitExceeded`) and rejects
//! recursive aliases, so hostile nesting and billion-laughs shapes
//! answer typed errors instead of overflowing the stack — no extra
//! depth guard needed here, unlike the msgpack and cbor codecs.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_yaml::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("yaml").unwrap();
//! # #[derive(serde::Serialize)]
//! # struct Ping { seq: u32 }
//! let bytes = marshal(codec.as_ref(), &Ping { seq: 1 }).unwrap();
//! # let _ = bytes;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use erased_serde::{
    Deserializer as ErasedDeserializer, Serialize as ErasedSerialize,
    Serializer as ErasedSerializer,
};
use rushwind_encoding::{Codec, EncodingError};

/// The codec's registry name.
pub const NAME: &str = "yaml";

/// The YAML codec. State-free; construct one or fetch it from the
/// registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct YamlCodec;

impl YamlCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — registration is explicit, not import-time.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the yaml codec");
    }
}

/// Installs [`YamlCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`YamlCodec::register`].
pub fn register() {
    YamlCodec::register();
}

impl Codec for YamlCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        let mut buffer = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_yaml::Serializer::new(&mut buffer),
            ))
            .map_err(|e| EncodingError::Failed(format!("yaml encode: {e}")))?;
        Ok(buffer)
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        let text = std::str::from_utf8(data)
            .map_err(|e| EncodingError::Failed(format!("yaml payload is not utf-8: {e}")))?;
        let deserializer = serde_yaml::Deserializer::from_str(text);
        decode(&mut <dyn ErasedDeserializer>::erase(deserializer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_encoding::{marshal, unmarshal};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        seq: u32,
        note: String,
    }

    fn payload() -> Payload {
        Payload {
            seq: 7,
            note: "hello".into(),
        }
    }

    #[test]
    fn name_matches_go_constant() {
        assert_eq!(YamlCodec.name(), "yaml");
    }

    #[test]
    fn round_trips_through_yaml_text() {
        let codec = YamlCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("seq: 7"), "yaml mapping shape: {text:?}");
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = YamlCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"seq: [unclosed").unwrap_err();
        assert!(err.to_string().contains("yaml decode"));
    }

    #[test]
    fn non_utf8_input_fails_cleanly() {
        let codec = YamlCodec::codec();
        let err = unmarshal::<Payload>(&codec, &[0xff, 0xfe]).unwrap_err();
        assert!(err.to_string().contains("utf-8"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        YamlCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
