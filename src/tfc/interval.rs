use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use thiserror::Error;

use super::datatypes::{
    DateTimeInterval, Duration, INTERVAL_COMPONENT_DATE, INTERVAL_COMPONENT_DATETIME,
    INTERVAL_FLAG_DURATION_END, INTERVAL_FLAG_EXPLICIT, INTERVAL_FLAG_START_DURATION,
};

#[derive(Debug, Error)]
pub enum IntervalParseError {
    #[error("empty interval string")]
    Empty,
    #[error("missing '/' separator in interval: `{0}`")]
    MissingSeparator(String),
    #[error("invalid date component: `{0}`")]
    InvalidDate(String),
    #[error("invalid datetime component: `{0}`")]
    InvalidDateTime(String),
    #[error("invalid duration: `{0}`")]
    InvalidDuration(String),
    #[error("interval has two durations: `{0}`")]
    TwoDurations(String),
    #[error("interval has no date/datetime component: `{0}`")]
    NoDates(String),
}

/// Parse an ISO 8601 interval string into a `DateTimeInterval`.
///
/// Supported forms:
/// - `"start/end"` — explicit dates or datetimes
/// - `"start/duration"` — start date/datetime + ISO 8601 duration
/// - `"duration/end"` — ISO 8601 duration + end date/datetime
///
/// Date components: `YYYY-MM-DD`
/// DateTime components: `YYYY-MM-DDTHH:MM:SS[.fff]Z` (trailing Z required for now)
/// Duration: `PnYnMnDTnHnMnS` (standard ISO 8601 duration)
///
/// A date component denotes the whole day as an interval — the start of the
/// day through its end, in UTC when no timezone is given. A date-only
/// interval like `2020-02-01/2020-02-29` is therefore the inclusive span
/// `2020-02-01T00:00:00Z/2020-03-01T00:00:00Z`: the start date contributes
/// its start, the end date contributes its end. ISO 8601 is silent on this;
/// this is the convention implemented here.
pub fn parse_iso_interval(s: &str) -> Result<DateTimeInterval, IntervalParseError> {
    if s.is_empty() {
        return Err(IntervalParseError::Empty);
    }

    let slash_pos = s
        .find('/')
        .ok_or_else(|| IntervalParseError::MissingSeparator(s.to_string()))?;

    let left = &s[..slash_pos];
    let right = &s[slash_pos + 1..];

    let left_is_duration = left.starts_with('P') || left.starts_with("-P");
    let right_is_duration = right.starts_with('P') || right.starts_with("-P");

    if left_is_duration && right_is_duration {
        return Err(IntervalParseError::TwoDurations(s.to_string()));
    }

    if left_is_duration {
        // duration/end
        let duration = parse_iso_duration(left)?;
        let (end_seconds, end_nanos, end_type) = parse_date_or_datetime(right, true)?;
        let (start_seconds, start_nanos) =
            compute_start_from_duration_and_end(&duration, end_seconds, end_nanos);
        let start_type = end_type;
        Ok(DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag: INTERVAL_FLAG_DURATION_END,
            duration,
        })
    } else if right_is_duration {
        // start/duration
        let (start_seconds, start_nanos, start_type) = parse_date_or_datetime(left, false)?;
        let duration = parse_iso_duration(right)?;
        let (end_seconds, end_nanos) =
            compute_end_from_start_and_duration(&duration, start_seconds, start_nanos);
        let end_type = start_type;
        Ok(DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag: INTERVAL_FLAG_START_DURATION,
            duration,
        })
    } else {
        // start/end (explicit)
        let (start_seconds, start_nanos, start_type) = parse_date_or_datetime(left, false)?;
        let (end_seconds, end_nanos, end_type) = parse_date_or_datetime(right, true)?;
        let duration =
            compute_duration_from_endpoints(start_seconds, start_nanos, end_seconds, end_nanos);
        Ok(DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag: INTERVAL_FLAG_EXPLICIT,
            duration,
        })
    }
}

/// Parse a date (`YYYY-MM-DD`) or datetime (`YYYY-MM-DDTHH:MM:SS[.fff][Z|+hh:mm|-hh:mm]`) string.
/// Returns (unix_seconds, nanos, component_type). All datetimes are converted to UTC.
/// When `is_end` is true and the component is a date, the date is bumped to the
/// next calendar day, matching the half-open interval convention for unqualified
/// ISO 8601 end dates.
fn parse_date_or_datetime(s: &str, is_end: bool) -> Result<(i64, u32, u8), IntervalParseError> {
    if s.contains('T') {
        // DateTime: try RFC 3339 first (handles Z and numeric offsets), then fall
        // back to naive ISO 8601 datetimes which are interpreted as UTC.
        let dt = DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map(|ndt| ndt.and_utc())
            })
            .map_err(|_| IntervalParseError::InvalidDateTime(s.to_string()))?;
        Ok((
            dt.timestamp(),
            dt.timestamp_subsec_nanos(),
            INTERVAL_COMPONENT_DATETIME,
        ))
    } else {
        // Date only
        let mut nd = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| IntervalParseError::InvalidDate(s.to_string()))?;
        if is_end {
            nd = nd.succ_opt().unwrap_or(nd);
        }
        let ndt = nd.and_hms_opt(0, 0, 0).unwrap();
        Ok((ndt.and_utc().timestamp(), 0, INTERVAL_COMPONENT_DATE))
    }
}

/// Designators in the ISO 8601 order used for duration parsing.
/// The order is: Y < M < D < H < Minute < S.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DurationDesignator {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// Ensure `current` appears after the previous designator and is not a duplicate.
fn validate_designator_order(
    last: &Option<DurationDesignator>,
    current: DurationDesignator,
) -> Result<(), ()> {
    if let Some(l) = last {
        if *l >= current {
            return Err(());
        }
    }
    Ok(())
}

/// Parse an ISO 8601 duration string like `P3M`, `P1Y2M3DT4H5M6S`, `-P1D`.
/// Designators must appear in the order Y, M, D, T, H, M, S and may not be repeated.
fn parse_iso_duration(s: &str) -> Result<Duration, IntervalParseError> {
    let err = || IntervalParseError::InvalidDuration(s.to_string());

    let (sign, rest) = if let Some(stripped) = s.strip_prefix('-') {
        (-1i8, stripped)
    } else {
        (1i8, s)
    };

    let rest = rest.strip_prefix('P').ok_or_else(err)?;

    let (date_part, time_part) = if let Some(t_pos) = rest.find('T') {
        (&rest[..t_pos], Some(&rest[t_pos + 1..]))
    } else {
        (rest, None)
    };

    let mut year: i64 = 0;
    let mut month: u8 = 0;
    let mut day: u8 = 0;

    if !date_part.is_empty() {
        let mut num_buf = String::new();
        let mut last_designator: Option<DurationDesignator> = None;
        for ch in date_part.chars() {
            match ch {
                'Y' => {
                    let d = DurationDesignator::Year;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    year = num_buf.parse::<i64>().map_err(|_| err())?;
                    num_buf.clear();
                }
                'M' => {
                    let d = DurationDesignator::Month;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    month = num_buf.parse::<u8>().map_err(|_| err())?;
                    num_buf.clear();
                }
                'D' => {
                    let d = DurationDesignator::Day;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    day = num_buf.parse::<u8>().map_err(|_| err())?;
                    num_buf.clear();
                }
                '0'..='9' => num_buf.push(ch),
                _ => return Err(err()),
            }
        }
        if !num_buf.is_empty() {
            return Err(err());
        }
    }

    let mut hour: u8 = 0;
    let mut minute: u8 = 0;
    let mut second: f64 = 0.0;

    if let Some(tp) = time_part {
        if tp.is_empty() {
            return Err(err());
        }
        let mut num_buf = String::new();
        let mut last_designator: Option<DurationDesignator> = None;
        for ch in tp.chars() {
            match ch {
                'H' => {
                    let d = DurationDesignator::Hour;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    hour = num_buf.parse::<u8>().map_err(|_| err())?;
                    num_buf.clear();
                }
                'M' => {
                    let d = DurationDesignator::Minute;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    minute = num_buf.parse::<u8>().map_err(|_| err())?;
                    num_buf.clear();
                }
                'S' => {
                    let d = DurationDesignator::Second;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    last_designator = Some(d);
                    second = num_buf.parse::<f64>().map_err(|_| err())?;
                    num_buf.clear();
                }
                '0'..='9' | '.' => num_buf.push(ch),
                _ => return Err(err()),
            }
        }
        if !num_buf.is_empty() {
            return Err(err());
        }
    }

    Ok(Duration {
        sign,
        year,
        month,
        day,
        hour,
        minute,
        second,
    })
}

/// Compute an approximate duration from two endpoints.
/// This produces a day-based duration (no month/year component).
fn compute_duration_from_endpoints(
    start_seconds: i64,
    start_nanos: u32,
    end_seconds: i64,
    end_nanos: u32,
) -> Duration {
    // Compute the full elapsed interval in nanoseconds so sub-second explicit
    // intervals get a correct duration.
    let start_total_nanos = start_seconds as i128 * 1_000_000_000 + start_nanos as i128;
    let end_total_nanos = end_seconds as i128 * 1_000_000_000 + end_nanos as i128;
    let total_nanos = end_total_nanos - start_total_nanos;
    let sign = if total_nanos >= 0 { 1 } else { -1 };
    let abs_total_nanos = total_nanos.unsigned_abs();
    let days = (abs_total_nanos / 86_400_000_000_000) as u8;
    let remainder = abs_total_nanos % 86_400_000_000_000;
    let hours = (remainder / 3_600_000_000_000) as u8;
    let remainder = remainder % 3_600_000_000_000;
    let minutes = (remainder / 60_000_000_000) as u8;
    let remainder = remainder % 60_000_000_000;
    let secs = (remainder / 1_000_000_000) as u8;
    let nanos = (remainder % 1_000_000_000) as u32;
    let second = secs as f64 + nanos as f64 / 1_000_000_000.0;
    Duration {
        sign,
        year: 0,
        month: 0,
        day: days,
        hour: hours,
        minute: minutes,
        second,
    }
}

/// Compute end timestamp from start + duration.
/// For year/month durations, this uses chrono calendar arithmetic.
fn compute_end_from_start_and_duration(
    duration: &Duration,
    start_seconds: i64,
    start_nanos: u32,
) -> (i64, u32) {
    let start_dt = DateTime::from_timestamp(start_seconds, start_nanos)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|| {
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        });

    let sign = if duration.sign >= 0 { 1i64 } else { -1i64 };

    // Add year/month via chrono
    let mut dt = start_dt;
    if duration.year != 0 || duration.month != 0 {
        let total_months = sign * (duration.year * 12 + duration.month as i64);
        dt = add_months(dt, total_months as i32);
    }

    // Add day/hour/minute/second as seconds
    let day_seconds = sign
        * (duration.day as i64 * 86400
            + duration.hour as i64 * 3600
            + duration.minute as i64 * 60
            + duration.second as i64);

    let end_seconds = dt.and_utc().timestamp() + day_seconds;
    let frac_nanos = ((duration.second.fract() * 1_000_000_000.0) as u32).wrapping_mul(sign as u32);
    let end_nanos = start_nanos.wrapping_add(frac_nanos);

    (end_seconds, end_nanos)
}

/// Compute start timestamp from duration + end.
fn compute_start_from_duration_and_end(
    duration: &Duration,
    end_seconds: i64,
    end_nanos: u32,
) -> (i64, u32) {
    let end_dt = DateTime::from_timestamp(end_seconds, end_nanos)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|| {
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        });

    let sign = if duration.sign >= 0 { -1i64 } else { 1i64 };

    let mut dt = end_dt;
    if duration.year != 0 || duration.month != 0 {
        let total_months = sign * (duration.year * 12 + duration.month as i64);
        dt = add_months(dt, total_months as i32);
    }

    let day_seconds = sign
        * (duration.day as i64 * 86400
            + duration.hour as i64 * 3600
            + duration.minute as i64 * 60
            + duration.second as i64);

    let start_seconds = dt.and_utc().timestamp() + day_seconds;
    let frac_nanos = ((duration.second.fract() * 1_000_000_000.0) as u32).wrapping_mul(sign as u32);
    let start_nanos = end_nanos.wrapping_add(frac_nanos);

    (start_seconds, start_nanos)
}

/// Add (or subtract) months to a NaiveDateTime using chrono calendar logic.
/// Clamps the day to the last day of the target month if needed.
fn add_months(dt: NaiveDateTime, months: i32) -> NaiveDateTime {
    let total_months = dt.date().year() as i32 * 12 + dt.date().month0() as i32 + months;
    let target_year = total_months.div_euclid(12);
    let target_month0 = total_months.rem_euclid(12) as u32;
    let target_month = target_month0 + 1;
    let max_day = days_in_month(target_year, target_month);
    let day = dt.date().day().min(max_day);
    NaiveDate::from_ymd_opt(target_year, target_month, day)
        .unwrap()
        .and_hms_nano_opt(
            dt.time().hour(),
            dt.time().minute(),
            dt.time().second(),
            dt.time().nanosecond(),
        )
        .unwrap()
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

use chrono::Datelike;
use chrono::Timelike;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tfc::datatypes::{FromLexical, TdbDataType, ToLexical};

    #[test]
    fn parse_explicit_dates() {
        let iv = parse_iso_interval("2025-01-01/2025-04-01").unwrap();
        assert_eq!(iv.flag, INTERVAL_FLAG_EXPLICIT);
        assert_eq!(iv.start_type, INTERVAL_COMPONENT_DATE);
        assert_eq!(iv.end_type, INTERVAL_COMPONENT_DATE);
        assert_eq!(iv.start_seconds, 1735689600); // 2025-01-01T00:00:00Z
        assert_eq!(iv.end_seconds, 1743552000); // 2025-04-02T00:00:00Z (unqualified end bumped)
        assert_eq!(iv.start_nanos, 0);
        assert_eq!(iv.end_nanos, 0);
    }

    #[test]
    fn parse_explicit_datetimes() {
        let iv = parse_iso_interval("2025-01-01T10:30:00Z/2025-04-01T15:45:00Z").unwrap();
        assert_eq!(iv.flag, INTERVAL_FLAG_EXPLICIT);
        assert_eq!(iv.start_type, INTERVAL_COMPONENT_DATETIME);
        assert_eq!(iv.end_type, INTERVAL_COMPONENT_DATETIME);
        assert_eq!(iv.start_seconds, 1735727400);
        assert_eq!(iv.end_seconds, 1743522300);
    }

    #[test]
    fn parse_explicit_datetime_with_nanos() {
        let iv = parse_iso_interval("2025-01-01T00:00:00.500Z/2025-04-01T00:00:00Z").unwrap();
        assert_eq!(iv.start_nanos, 500_000_000);
        assert_eq!(iv.end_nanos, 0);
    }

    #[test]
    fn parse_explicit_datetime_with_timezone_offset_and_nanos() {
        // +02:00 means the local 09:00:00 is 07:00:00Z.
        let s = "2025-01-01T09:00:00.000+02:00/2025-01-01T09:00:00.123456789+02:00";
        let iv = parse_iso_interval(s).unwrap();
        assert_eq!(iv.start_seconds, 1735689600 + 7 * 3600);
        assert_eq!(iv.start_nanos, 0);
        assert_eq!(iv.end_seconds, 1735689600 + 7 * 3600);
        assert_eq!(iv.end_nanos, 123_456_789);
        assert_eq!(iv.duration.second, 0.123456789);
    }

    #[test]
    fn roundtrip_explicit_datetime_with_timezone_offset_and_nanos() {
        let s = "2025-01-01T09:00:00.000+02:00/2025-01-01T09:00:00.123456789+02:00";
        let iv = parse_iso_interval(s).unwrap();
        let round = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!(round, "2025-01-01T07:00:00Z/2025-01-01T07:00:00.123456789Z");
    }

    #[test]
    fn parse_start_duration() {
        let iv = parse_iso_interval("2025-01-01/P3M").unwrap();
        assert_eq!(iv.flag, INTERVAL_FLAG_START_DURATION);
        assert_eq!(iv.start_seconds, 1735689600);
        assert_eq!(iv.duration.month, 3);
        assert_eq!(iv.duration.sign, 1);
        // End should be 2025-04-01
        assert_eq!(iv.end_seconds, 1743465600);
    }

    #[test]
    fn parse_duration_end() {
        let iv = parse_iso_interval("P3M/2025-04-01").unwrap();
        assert_eq!(iv.flag, INTERVAL_FLAG_DURATION_END);
        assert_eq!(iv.end_seconds, 1743552000); // 2025-04-02T00:00:00Z (unqualified end bumped)
        assert_eq!(iv.duration.month, 3);
        // Start is 2025-01-02: P3M back from the bumped end 2025-04-02
        assert_eq!(iv.start_seconds, 1735776000);
    }

    #[test]
    fn parse_duration_with_time_parts() {
        let iv = parse_iso_interval("2025-01-01/PT1H").unwrap();
        assert_eq!(iv.flag, INTERVAL_FLAG_START_DURATION);
        assert_eq!(iv.duration.hour, 1);
        assert_eq!(iv.duration.day, 0);
        assert_eq!(iv.end_seconds, iv.start_seconds + 3600);
    }

    #[test]
    fn parse_complex_duration() {
        let iv = parse_iso_interval("2025-01-01/P1Y2M3DT4H5M6S").unwrap();
        assert_eq!(iv.duration.year, 1);
        assert_eq!(iv.duration.month, 2);
        assert_eq!(iv.duration.day, 3);
        assert_eq!(iv.duration.hour, 4);
        assert_eq!(iv.duration.minute, 5);
        assert_eq!(iv.duration.second, 6.0);
    }

    #[test]
    fn parse_negative_duration() {
        let iv = parse_iso_interval("-P1D/2025-01-02").unwrap();
        assert_eq!(iv.duration.sign, -1);
        assert_eq!(iv.duration.day, 1);
    }

    #[test]
    fn parse_error_empty() {
        assert!(parse_iso_interval("").is_err());
    }

    #[test]
    fn parse_error_no_separator() {
        assert!(parse_iso_interval("2025-01-01").is_err());
    }

    #[test]
    fn parse_error_two_durations() {
        assert!(parse_iso_interval("P1M/P2M").is_err());
    }

    #[test]
    fn parse_error_invalid_date() {
        assert!(parse_iso_interval("not-a-date/2025-01-01").is_err());
    }

    #[test]
    fn roundtrip_parse_to_lexical_to_string_explicit() {
        // Q1 written as inclusive dates: the end of 2025-03-31 renders as the
        // canonical exclusive instant 2025-04-01T00:00:00Z.
        let iv = parse_iso_interval("2025-01-01/2025-03-31").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn roundtrip_parse_to_lexical_to_string_start_duration() {
        let iv = parse_iso_interval("2025-01-01/P3M").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2025-01-01T00:00:00Z/P3M", s);
    }

    #[test]
    fn roundtrip_parse_to_lexical_to_string_duration_end() {
        // Same rule: P3M ending on 2025-03-31 is the quarter, rendered as the
        // exclusive instant 2025-04-01T00:00:00Z.
        let iv = parse_iso_interval("P3M/2025-03-31").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("P3M/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn roundtrip_parse_to_lexical_to_struct() {
        let iv = parse_iso_interval("2025-01-01/2025-04-01").unwrap();
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes.clone());
        let bytes2 = iv2.to_lexical();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn roundtrip_parse_to_lexical_to_struct_start_duration() {
        let iv = parse_iso_interval("2025-01-01/P3M").unwrap();
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes.clone());
        assert_eq!(iv.start_seconds, iv2.start_seconds);
        assert_eq!(iv.end_seconds, iv2.end_seconds);
        assert_eq!(iv.duration.month, iv2.duration.month);
        assert_eq!(iv.flag, iv2.flag);
    }

    #[test]
    fn roundtrip_datetime_with_fractional_seconds() {
        let iv = parse_iso_interval("2025-01-01T10:30:00.500Z/2025-04-01T15:45:00Z").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2025-01-01T10:30:00.500Z/2025-04-01T15:45:00Z", s);
    }

    #[test]
    fn lexical_ordering_chronological() {
        let iv_early = parse_iso_interval("2024-01-01/2024-06-01").unwrap();
        let iv_late = parse_iso_interval("2025-01-01/2025-06-01").unwrap();

        let entry_early = DateTimeInterval::make_entry(&iv_early);
        let entry_late = DateTimeInterval::make_entry(&iv_late);
        assert!(entry_early < entry_late);
    }

    #[test]
    fn lexical_ordering_same_start_different_end() {
        let iv_short = parse_iso_interval("2025-01-01/2025-02-01").unwrap();
        let iv_long = parse_iso_interval("2025-01-01/2025-06-01").unwrap();

        let entry_short = DateTimeInterval::make_entry(&iv_short);
        let entry_long = DateTimeInterval::make_entry(&iv_long);
        assert!(entry_short < entry_long);
    }

    #[test]
    fn lexical_ordering_mixed_formats() {
        // start/duration and start/end for the same interval should sort the same
        // 2025-01-01/P3M ends on 2025-04-01; the explicit date-only end 2025-03-31
        // is bumped to 2025-04-01 so both forms represent the same half-open interval.
        let iv_explicit = parse_iso_interval("2025-01-01/2025-03-31").unwrap();
        let iv_duration = parse_iso_interval("2025-01-01/P3M").unwrap();

        // Same start and end timestamps
        assert_eq!(iv_explicit.start_seconds, iv_duration.start_seconds);
        assert_eq!(iv_explicit.end_seconds, iv_duration.end_seconds);

        // Lexical entries differ only in metadata suffix (flag + duration)
        // but sorting prefix is identical
        let bytes_e = iv_explicit.to_lexical();
        let bytes_d = iv_duration.to_lexical();
        // First 24 bytes (sorting prefix) should be identical
        assert_eq!(&bytes_e[..24], &bytes_d[..24]);
    }

    #[test]
    fn parse_duration_days_only() {
        let iv = parse_iso_interval("2025-01-01/P90D").unwrap();
        assert_eq!(iv.duration.day, 90);
        assert_eq!(iv.duration.month, 0);
    }

    #[test]
    fn parse_duration_fractional_seconds() {
        let iv = parse_iso_interval("2025-01-01/PT0.5S").unwrap();
        assert_eq!(iv.duration.second, 0.5);
    }

    #[test]
    fn leap_year_2020_bumps_feb_29_end_to_march_1() {
        // 2020 is a leap year: 2020-02-29 is a valid date, but the unqualified
        // end date is bumped to the start of the next day.
        let iv = parse_iso_interval("2020-02-28/2020-02-29").unwrap();
        assert_eq!(iv.start_seconds, 1582848000); // 2020-02-28T00:00:00Z
        assert_eq!(iv.end_seconds, 1583020800); // 2020-03-01T00:00:00Z
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2020-02-28T00:00:00Z/2020-03-01T00:00:00Z", s);
    }

    #[test]
    fn leap_year_2000_bumps_feb_29_end_to_march_1() {
        // 2000 is a leap year (divisible by 400).
        let iv = parse_iso_interval("2000-02-28/2000-02-29").unwrap();
        assert_eq!(iv.start_seconds, 951696000); // 2000-02-28T00:00:00Z
        assert_eq!(iv.end_seconds, 951868800); // 2000-03-01T00:00:00Z
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2000-02-28T00:00:00Z/2000-03-01T00:00:00Z", s);
    }

    #[test]
    fn century_non_leap_year_1900_bumps_feb_28_end_to_march_1() {
        // 1900 is not a leap year (divisible by 100 but not by 400).
        // Feb 28 is the last day of February, so the unqualified end date
        // 1900-02-28 is bumped to 1900-03-01.
        let iv = parse_iso_interval("1900-02-27/1900-02-28").unwrap();
        assert_eq!(iv.end_seconds, -2203891200); // 1900-03-01T00:00:00Z
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("1900-02-27T00:00:00Z/1900-03-01T00:00:00Z", s);
    }

    #[test]
    fn date_only_interval_from_start_to_end_of_year() {
        // A calendar year written as inclusive dates becomes the half-open
        // interval [Jan 1, Jan 1 of next year).
        let iv = parse_iso_interval("2020-01-01/2020-12-31").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2020-01-01T00:00:00Z/2021-01-01T00:00:00Z", s);
    }

    #[test]
    fn date_only_q4_interval() {
        // Q4 written as inclusive dates: 2020-10-01 through 2020-12-31.
        // The unqualified end date is bumped to the start of the next day.
        let iv = parse_iso_interval("2020-10-01/2020-12-31").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2020-10-01T00:00:00Z/2021-01-01T00:00:00Z", s);
    }

    #[test]
    fn parse_explicit_datetime_with_positive_offset() {
        let iv = parse_iso_interval("2025-01-01T10:30:00+02:00/2025-04-01T15:45:00+02:00").unwrap();
        assert_eq!(iv.start_seconds, 1735720200); // 2025-01-01T08:30:00Z
        assert_eq!(iv.end_seconds, 1743515100); // 2025-04-01T13:45:00Z
    }

    #[test]
    fn parse_explicit_datetime_with_negative_offset() {
        let iv = parse_iso_interval("2025-01-01T10:30:00-05:00/2025-04-01T15:45:00-05:00").unwrap();
        assert_eq!(iv.start_seconds, 1735745400); // 2025-01-01T15:30:00Z
        assert_eq!(iv.end_seconds, 1743540300); // 2025-04-01T20:45:00Z
    }

    #[test]
    fn parse_datetime_with_offset_roundtrips_to_utc() {
        let iv = parse_iso_interval("2025-01-01T10:30:00+02:00/2025-04-01T15:45:00+02:00").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2025-01-01T08:30:00Z/2025-04-01T13:45:00Z", s);
    }

    #[test]
    fn compute_duration_from_sub_second_explicit_interval() {
        let iv = parse_iso_interval("2025-01-01T00:00:00.000Z/2025-01-01T00:00:00.500Z").unwrap();
        assert_eq!(iv.duration.second, 0.5);
        assert_eq!(iv.duration.day, 0);
        assert_eq!(iv.duration.hour, 0);
        assert_eq!(iv.duration.minute, 0);
    }

    #[test]
    fn parse_duplicate_duration_designator_is_error() {
        // Duplicate designators are not valid ISO 8601.
        assert!(parse_iso_interval("2025-01-01/P1M2M").is_err());
    }

    #[test]
    fn parse_out_of_order_duration_designator_is_error() {
        // ISO 8601 requires Y, M, D order before T and H, M, S order after T.
        assert!(parse_iso_interval("2025-01-01/P1D1Y").is_err());
        assert!(parse_iso_interval("2025-01-01/PT1S1H").is_err());
    }
}
