//! Constant-time credential comparison for the engines that validate
//! secret-shaped strings against a static table.
//!
//! A plain `==` on strings short-circuits at the first differing byte,
//! so response timing encodes how much of a presented secret matched —
//! enough of a side channel for an attacker with network-local latency
//! measurements to probe credentials byte by byte. The HMAC engine
//! gets this property from `Mac::verify_slice` for free; the table
//! engines use [`constant_time_eq`].

/// Constant-time equality for credential strings: the XOR of every
/// byte pair folds into one accumulator, so the comparison cost does
/// not vary with the position of the first differing byte. Lengths are
/// compared first and short-circuit — the encoded shape of a
/// credential (base64 width, hex width) is public knowledge, not a
/// secret worth masking.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn equality_matches_and_mismatches() {
        assert!(constant_time_eq("key-1", "key-1"));
        assert!(!constant_time_eq("key-1", "key-2"));
        assert!(!constant_time_eq("key-1", "key-11"));
        assert!(!constant_time_eq("", "key-1"));
        assert!(constant_time_eq("", ""));
        assert!(!constant_time_eq("aesop", "aesop2"));
    }
}
