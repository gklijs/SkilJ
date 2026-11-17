//! How skilj writes a timestamp on the wire: RFC 3339 in UTC, with 0, 3, 6
//! or 9 fractional digits - what chrono wrote before skilj moved to jiff
//! (docs/architecture.md §254). jiff's own `Display` and serde trim every
//! trailing zero (`.12345` where chrono wrote `.123450`), which is valid
//! RFC 3339 but would change byte for byte what clients already get.
//!
//! GraphQL renders the `+00:00` form ([`rfc3339`], chrono's
//! `to_rfc3339`), REST and serde the `Z` form ([`serialize`], chrono's
//! serde). Both parse back with `str::parse::<jiff::Timestamp>()`.

use jiff::Timestamp;

fn render(t: Timestamp, offset: &str) -> String {
    let dt = jiff::tz::Offset::UTC.to_datetime(t);
    let nanos = dt.subsec_nanosecond();
    let fraction = if nanos == 0 {
        String::new()
    } else if nanos % 1_000_000 == 0 {
        format!(".{:03}", nanos / 1_000_000)
    } else if nanos % 1_000 == 0 {
        format!(".{:06}", nanos / 1_000)
    } else {
        format!(".{nanos:09}")
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{fraction}{offset}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
    )
}

/// `t` with a `+00:00` offset, as GraphQL renders a timestamp.
pub fn rfc3339(t: Timestamp) -> String {
    render(t, "+00:00")
}

/// `t` with a `Z` offset, as REST and serde write a timestamp.
pub fn rfc3339_z(t: Timestamp) -> String {
    render(t, "Z")
}

/// `#[serde(serialize_with = "skilj_core::wire_time::serialize")]`:
/// [`rfc3339_z`] as a string.
pub fn serialize<S: serde::Serializer>(t: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&rfc3339_z(*t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fraction_digits_come_in_threes_like_chrono() {
        let at = |s: &str| s.parse::<Timestamp>().unwrap();
        assert_eq!(
            rfc3339(at("2026-11-17T13:42:00Z")),
            "2026-11-17T13:42:00+00:00"
        );
        assert_eq!(
            rfc3339_z(at("2026-11-17T13:42:00.5Z")),
            "2026-11-17T13:42:00.500Z"
        );
        assert_eq!(
            rfc3339_z(at("2026-11-17T13:42:00.12345Z")),
            "2026-11-17T13:42:00.123450Z"
        );
        assert_eq!(
            rfc3339_z(at("2026-11-17T13:42:00.1234567Z")),
            "2026-11-17T13:42:00.123456700Z"
        );
        assert_eq!(
            rfc3339(at("1969-12-31T23:59:59.999999Z")),
            "1969-12-31T23:59:59.999999+00:00"
        );
    }
}
