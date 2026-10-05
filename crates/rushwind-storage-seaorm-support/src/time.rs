//! The entity-time ↔ proto-Timestamp conversions. SeaORM entity
//! columns carry chrono types (`NaiveDateTime` for plain timestamps,
//! `DateTime<FixedOffset>` for timestamptz); the protojson face carries
//! pbjson's WKT — every mapper in every deployment crossed this bridge
//! by hand.

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use pbjson_types::Timestamp;

/// Naive local datetime (the plain timestamp column flavor) → protojson
/// Timestamp, read as UTC.
pub fn naive_to_ts(value: NaiveDateTime) -> Option<Timestamp> {
    use chrono::TimeZone as _;
    let utc = chrono::Utc.from_utc_datetime(&value);
    Some(Timestamp {
        seconds: utc.timestamp(),
        nanos: utc.timestamp_subsec_nanos() as i32,
    })
}

/// protojson Timestamp → naive local datetime (UTC).
pub fn ts_to_naive(value: &Timestamp) -> Option<NaiveDateTime> {
    use chrono::TimeZone as _;
    Some(
        chrono::Utc
            .timestamp_opt(value.seconds, value.nanos.max(0) as u32)
            .single()?
            .naive_utc(),
    )
}

/// Timestamptz (the `DateTime<FixedOffset>` column flavor) → protojson
/// Timestamp.
pub fn datetime_to_ts(value: DateTime<FixedOffset>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    })
}

/// protojson Timestamp → timestamptz (UTC).
pub fn ts_to_datetime(value: &Timestamp) -> Option<DateTime<FixedOffset>> {
    use chrono::TimeZone as _;
    chrono::Utc
        .timestamp_opt(value.seconds, value.nanos.max(0) as u32)
        .single()
        .map(|dt| dt.fixed_offset())
}

/// Now, as a timestamptz value (the schema's timestamp flavor).
pub fn now() -> DateTime<FixedOffset> {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn naive_roundtrip_is_identity() {
        let naive = chrono::NaiveDate::from_ymd_opt(2024, 3, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let ts = naive_to_ts(naive).unwrap();
        assert_eq!(ts_to_naive(&ts).unwrap(), naive);
    }

    #[test]
    fn datetime_roundtrip_is_identity() {
        let dt = chrono::NaiveDate::from_ymd_opt(2024, 3, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap()
            .and_local_timezone(chrono::FixedOffset::east_opt(8 * 3600).unwrap())
            .single()
            .unwrap();
        let ts = datetime_to_ts(dt).unwrap();
        assert_eq!(ts_to_datetime(&ts).unwrap(), dt);
    }

    #[test]
    fn negative_seconds_keep_their_subsecond_nanos() {
        // The pre-1970 spelling: negative seconds carry a POSITIVE nanos
        // field — the max(0) guard keeps timestamp_opt single.
        let ts = Timestamp {
            seconds: -1,
            nanos: 500_000_000,
        };
        let naive = ts_to_naive(&ts).unwrap();
        assert_eq!(naive.and_utc().timestamp_subsec_nanos(), 500_000_000);
    }
}
