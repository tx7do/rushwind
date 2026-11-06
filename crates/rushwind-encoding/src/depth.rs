//! Wire-format depth pre-scanners for the binary codecs.
//!
//! The msgpack and CBOR deserializers recurse once per nesting level
//! while driving the target type's visitor, so an unbounded payload
//! overflows the machine stack — an abort, which no `catch_unwind` in
//! the workspace can intercept. The scanners below walk the same bytes
//! with an explicit heap stack and answer one question: does the
//! nesting exceed the limit? Both return `false` on input they cannot
//! parse — the real deserializer produces its own error for malformed
//! bytes, so the guard only ever fires on well-formed input that is
//! too deep.

/// The shared nesting ceiling, matching serde_json's own recursion
/// limit: a depth any honest payload stays far under and a hostile one
/// cannot get past.
pub const MAX_DEPTH: usize = 128;

/// Whether a MessagePack payload nests deeper than `limit`. Malformed
/// bytes answer `false` (the deserializer will report them); a
/// well-formed payload over the limit answers `true`.
pub fn msgpack_exceeds_depth(data: &[u8], limit: usize) -> bool {
    exceeds(data, limit, msgpack_step)
}

/// Whether a CBOR payload nests deeper than `limit`. Malformed bytes
/// answer `false`; a well-formed payload over the limit answers
/// `true`.
pub fn cbor_exceeds_depth(data: &[u8], limit: usize) -> bool {
    exceeds(data, limit, cbor_step)
}

/// The shared frame: a cursor, a heap stack of remaining child counts
/// (`None` = an indefinite container that closes on its break byte),
/// and the nesting depth. One `step` consumes exactly one element.
/// One wire-format step: consume one element, advance the cursor,
/// push containers, adjust the depth.
type StepFn = fn(&[u8], &mut usize, &mut Vec<Option<u64>>, &mut usize) -> Option<()>;

fn exceeds(data: &[u8], limit: usize, step: StepFn) -> bool {
    let mut pos = 0;
    let mut stack = Vec::new();
    let mut depth = 0usize;
    while pos < data.len() {
        // A finished container completes one element in its parent;
        // cascade until a parent still expects children.
        while let Some(top) = stack.last_mut() {
            match top {
                Some(0) => {
                    stack.pop();
                    depth -= 1;
                }
                Some(remaining) => {
                    *remaining -= 1;
                    break;
                }
                None => break,
            }
        }
        // A None from the step means malformed input the real
        // deserializer will report: stop scanning, no violation found.
        if step(data, &mut pos, &mut stack, &mut depth).is_none() {
            break;
        }
        if depth > limit {
            return true;
        }
        pos += 1;
    }
    false
}

/// Reads a big-endian width-byte integer; `None` past the end.
fn read_len(data: &[u8], pos: usize, width: usize) -> Option<u64> {
    let end = pos.checked_add(width)?;
    if end > data.len() {
        return None;
    }
    let mut value = 0u64;
    for byte in &data[pos..end] {
        value = (value << 8) | u64::from(*byte);
    }
    Some(value)
}

/// Consumes one MessagePack element: advances `pos` to the element's
/// last byte (the shared frame's `pos += 1` lands on the next), pushes
/// container child counts, and raises `depth` on entry. Malformed
/// bytes consume what they claim and let the deserializer fail later.
fn msgpack_step(
    data: &[u8],
    pos: &mut usize,
    stack: &mut Vec<Option<u64>>,
    depth: &mut usize,
) -> Option<()> {
    let format = data.get(*pos).copied()?;
    // (child count when a container, bytes after the format byte)
    let (children, payload): (Option<u64>, usize) = match format {
        0x00..=0x7f | 0xe0..=0xff => (None, 0), // fixint leaves
        0xc0 | 0xc2..=0xc3 => (None, 0),        // nil, bools
        0x80..=0x8f => (Some(u64::from(format & 0x0f) * 2), 0), // fixmap
        0x90..=0x9f => (Some(u64::from(format & 0x0f)), 0), // fixarray
        0xa0..=0xbf => (None, usize::from(format & 0x1f)), // fixstr
        0xc1 => (None, 0),                      // the "never" byte: malformed
        0xc4 => (None, read_len(data, *pos + 1, 1)? as usize + 1),
        0xc5 => (None, read_len(data, *pos + 1, 2)? as usize + 2),
        0xc6 => (None, read_len(data, *pos + 1, 4)? as usize + 4),
        0xc7 => (None, read_len(data, *pos + 1, 1)? as usize + 2),
        0xc8 => (None, read_len(data, *pos + 1, 2)? as usize + 3),
        0xc9 => (None, read_len(data, *pos + 1, 4)? as usize + 5),
        0xca => (None, 4),
        0xcb => (None, 8),
        0xcc | 0xd0 | 0xd4 => (None, 1),
        0xcd | 0xd1 | 0xd5 => (None, 2),
        0xce | 0xd2 | 0xd6 => (None, 4),
        0xcf | 0xd3 | 0xd7 => (None, 8),
        0xd8 => (None, 16),
        0xd9 => (None, read_len(data, *pos + 1, 1)? as usize + 1),
        0xda => (None, read_len(data, *pos + 1, 2)? as usize + 2),
        0xdb => (None, read_len(data, *pos + 1, 4)? as usize + 4),
        0xdc => (Some(read_len(data, *pos + 1, 2)?), 2),
        0xdd => (Some(read_len(data, *pos + 1, 4)?), 4),
        0xde => (Some(read_len(data, *pos + 1, 2)? * 2), 2),
        0xdf => (Some(read_len(data, *pos + 1, 4)? * 2), 4),
    };
    let end = (*pos).checked_add(payload).and_then(|p| p.checked_add(1))?;
    if end > data.len() {
        // Claimed length runs past the buffer: malformed; the
        // deserializer reports the truncation.
        return None;
    }
    if let Some(count) = children {
        *depth += 1;
        stack.push(Some(count));
    }
    *pos = end - 1;
    Some(())
}

/// Consumes one CBOR element: advances `pos` to the element's last
/// byte (the shared frame's `pos += 1` lands on the next), pushes
/// container child counts (`None` for indefinite), and raises `depth`
/// on entry.
fn cbor_step(
    data: &[u8],
    pos: &mut usize,
    stack: &mut Vec<Option<u64>>,
    depth: &mut usize,
) -> Option<()> {
    let initial = data.get(*pos).copied()?;
    // A break byte closes the innermost indefinite container; anywhere
    // else it is malformed and the deserializer will say so.
    if initial == 0xff {
        if matches!(stack.last(), Some(None)) {
            stack.pop();
            *depth -= 1;
        }
        return Some(());
    }
    let major = initial >> 5;
    let additional = initial & 0x1f;
    let (children, skip): (Option<Option<u64>>, usize) = match (major, additional) {
        (0 | 1, 0..=23) => (None, 0),
        (0 | 1, 24) => (None, 1),
        (0 | 1, 25) => (None, 2),
        (0 | 1, 26) => (None, 4),
        (0 | 1, 27) => (None, 8),
        (2 | 3, 0..=23) => (None, usize::from(additional)),
        (2 | 3, 24) => (None, read_len(data, *pos + 1, 1)? as usize),
        (2 | 3, 25) => (None, read_len(data, *pos + 1, 2)? as usize),
        (2 | 3, 26) => (None, read_len(data, *pos + 1, 4)? as usize),
        (2 | 3, 27) => (None, read_len(data, *pos + 1, 8)? as usize),
        (2 | 3, 31) => (Some(None), 0), // indefinite string chunks
        (4, 0..=23) => (Some(Some(u64::from(additional))), 0),
        (4, 24) => (Some(Some(read_len(data, *pos + 1, 1)?)), 1),
        (4, 25) => (Some(Some(read_len(data, *pos + 1, 2)?)), 2),
        (4, 26) => (Some(Some(read_len(data, *pos + 1, 4)?)), 4),
        (4, 27) => (Some(Some(read_len(data, *pos + 1, 8)?)), 8),
        (4, 31) => (Some(None), 0),
        (5, 0..=23) => (Some(Some(u64::from(additional) * 2)), 0),
        (5, 24) => (Some(Some(read_len(data, *pos + 1, 1)? * 2)), 1),
        (5, 25) => (Some(Some(read_len(data, *pos + 1, 2)? * 2)), 2),
        (5, 26) => (Some(Some(read_len(data, *pos + 1, 4)? * 2)), 4),
        (5, 27) => (Some(Some(read_len(data, *pos + 1, 8)? * 2)), 8),
        (5, 31) => (Some(None), 0),
        (6, 0..=23) => (Some(Some(1)), 0), // a tag wraps exactly one item
        (6, 24) => (Some(Some(1)), 1),
        (6, 25) => (Some(Some(1)), 2),
        (6, 26) => (Some(Some(1)), 4),
        (6, 27) => (Some(Some(1)), 8),
        (7, 0..=23) => (None, 0), // simple values
        (7, 24) => (None, 1),
        (7, 25) => (None, 2), // f16
        (7, 26) => (None, 4), // f32
        (7, 27) => (None, 8), // f64
        // Reserved additional info: malformed length math.
        _ => return None,
    };
    let end = (*pos).checked_add(skip).and_then(|p| p.checked_add(1))?;
    if end > data.len() {
        return None;
    }
    if let Some(count) = children {
        *depth += 1;
        stack.push(count);
    }
    *pos = end - 1;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::{cbor_exceeds_depth, msgpack_exceeds_depth, MAX_DEPTH};

    #[test]
    fn msgpack_nesting_walks_the_boundary() {
        // d levels of fixarray(1): 0x91 repeated, closed by one fixint.
        let nest = |depth: usize| {
            let mut data = vec![0x91u8; depth];
            data.push(0x01);
            data
        };
        assert!(!msgpack_exceeds_depth(&nest(MAX_DEPTH), MAX_DEPTH));
        assert!(msgpack_exceeds_depth(&nest(MAX_DEPTH + 1), MAX_DEPTH));
        assert!(!msgpack_exceeds_depth(&nest(5), MAX_DEPTH));
    }

    #[test]
    fn msgpack_maps_count_two_children_per_entry() {
        // 0x81 fixmap(1), key 0x01, value 0x81 fixmap(1), key 0x01,
        // value 0x01 — depth 2.
        let data = [0x81, 0x01, 0x81, 0x01, 0x01];
        assert!(!msgpack_exceeds_depth(&data, MAX_DEPTH));
        assert!(msgpack_exceeds_depth(&data, 1));
    }

    #[test]
    fn msgpack_empty_containers_hold_no_cumulative_depth() {
        // Four sibling empty array/maps: each occupies one level while
        // open, and the settle cascade returns to 0 between them.
        let data = [0x90, 0x80, 0x90, 0x90];
        assert!(!msgpack_exceeds_depth(&data, 1));
        // A single empty container still occupies one level.
        assert!(msgpack_exceeds_depth(&[0x90], 0));
    }

    #[test]
    fn msgpack_leaves_and_truncation_stay_quiet() {
        // fixstr "hello", uint8, bin8, f64, negative fixint, fixext1.
        let data = [
            0xa5, b'h', b'e', b'l', b'l', b'o', 0xcc, 0x07, 0xc4, 0x02, 0xde, 0xad, 0xcb, 0, 0, 0,
            0, 0, 0, 0, 0, 0xff, 0xd4, 0x00, 0x2a,
        ];
        assert!(!msgpack_exceeds_depth(&data, MAX_DEPTH));
        // Truncated array16: malformed — the guard stays out of the way.
        let truncated = [0xda, 0x00, 0x10, 0x01];
        assert!(!msgpack_exceeds_depth(&truncated, MAX_DEPTH));
    }

    #[test]
    fn msgpack_str8_consumes_its_length_byte() {
        // 0xd9 str8 len=3 "abc", then a fixint: both parse.
        let data = [0xd9, 0x03, b'a', b'b', b'c', 0x05];
        assert!(!msgpack_exceeds_depth(&data, MAX_DEPTH));
        // With a wrong length the fixint is swallowed: malformed but
        // not an infinite loop, not a false positive.
        let data = [0xd9, 0x04, b'a', b'b', b'c'];
        assert!(!msgpack_exceeds_depth(&data, MAX_DEPTH));
    }

    #[test]
    fn cbor_nesting_walks_the_boundary() {
        let nest = |depth: usize| {
            let mut data = vec![0x81u8; depth]; // array(1)
            data.push(0x01);
            data
        };
        assert!(!cbor_exceeds_depth(&nest(MAX_DEPTH), MAX_DEPTH));
        assert!(cbor_exceeds_depth(&nest(MAX_DEPTH + 1), MAX_DEPTH));
    }

    #[test]
    fn cbor_indefinite_containers_close_on_break() {
        // 0x9f indefinite array holding two ints, closed by 0xff.
        let data = [0x9f, 0x01, 0x02, 0xff];
        assert!(!cbor_exceeds_depth(&data, MAX_DEPTH));
        // Indefinite text inside indefinite array: depth 2.
        let data = [0x9f, 0x7f, 0x61, b'x', 0xff, 0xff];
        assert!(cbor_exceeds_depth(&data, 1));
        assert!(!cbor_exceeds_depth(&data, MAX_DEPTH));
    }

    #[test]
    fn cbor_maps_tags_and_leaves() {
        // 0xa1 map(1): text key "k", tag(1) wrapping uint 1.
        let data = [0xa1, 0x61, b'k', 0xc1, 0x01];
        assert!(!cbor_exceeds_depth(&data, MAX_DEPTH));
        assert!(cbor_exceeds_depth(&data, 1));
        // Bytes chunk, f32, double.
        let data = [
            0x43, 1, 2, 3, 0xfa, 0, 0, 0, 0, 0xfb, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(!cbor_exceeds_depth(&data, MAX_DEPTH));
    }
}
