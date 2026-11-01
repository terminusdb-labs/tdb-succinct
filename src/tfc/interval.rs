use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use thiserror::Error;

use std::io;

use super::block::IdLookupResult;
use super::block::SizedDictEntry;
use super::datatypes::{
    Datatype, DateTimeInterval, Duration, FromLexical, ToLexical, INTERVAL_COMPONENT_DATE,
    INTERVAL_COMPONENT_DATETIME, INTERVAL_COMPONENT_OPEN, INTERVAL_COMPONENT_UNKNOWN,
    INTERVAL_ENCODED_WIDTH, INTERVAL_FLAG_DURATION_END, INTERVAL_FLAG_EXPLICIT,
    INTERVAL_FLAG_START_DURATION, INTERVAL_LEGACY_ENCODED_WIDTH, INTERVAL_NEG_INF,
    INTERVAL_NO_ZONE, INTERVAL_POS_INF, INTERVAL_UNKNOWN_END, INTERVAL_UNKNOWN_START,
};
use super::typed::{TypedDict, TypedDictBufBuilder, TypedDictEntry};
use crate::bitindex::BitIndex;
use crate::logarray::{LogArray, LogArrayBufBuilder, LogArrayFileBuilder};
use crate::storage::{FileLoad, FileStore};
use crate::wavelettree::{build_wavelet_tree_from_iter, wavelet_tree_from_iter, WaveletTree};

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
    #[error("duration combined with an open or unknown endpoint: `{0}`")]
    UnboundedWithDuration(String),
}

/// A non-finite interval endpoint per ISO 8601-2: open (`..`) or unknown
/// (empty).
#[derive(Clone, Copy)]
pub(crate) enum UnboundedEndpoint {
    Open,
    Unknown,
}

impl UnboundedEndpoint {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            ".." => Some(Self::Open),
            "" => Some(Self::Unknown),
            _ => None,
        }
    }

    /// Sentinel tuple `(seconds, nanos, component_type, tz)` for open and
    /// unknown endpoints: −∞/+∞ for open, far-side sentinels for unknown.
    fn sentinel(&self, is_end: bool) -> (i64, u32, u8, i16) {
        match (self, is_end) {
            (Self::Open, false) => (
                INTERVAL_NEG_INF,
                0,
                INTERVAL_COMPONENT_OPEN,
                INTERVAL_NO_ZONE,
            ),
            (Self::Open, true) => (
                INTERVAL_POS_INF,
                0,
                INTERVAL_COMPONENT_OPEN,
                INTERVAL_NO_ZONE,
            ),
            (Self::Unknown, false) => (
                INTERVAL_UNKNOWN_START,
                0,
                INTERVAL_COMPONENT_UNKNOWN,
                INTERVAL_NO_ZONE,
            ),
            (Self::Unknown, true) => (
                INTERVAL_UNKNOWN_END,
                0,
                INTERVAL_COMPONENT_UNKNOWN,
                INTERVAL_NO_ZONE,
            ),
        }
    }
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
        // duration/end — the computed start inherits the end's supplied zone
        if UnboundedEndpoint::from_str(right).is_some() {
            return Err(IntervalParseError::UnboundedWithDuration(s.to_string()));
        }
        let duration = parse_iso_duration(left)?;
        let (end_seconds, end_nanos, end_type, end_tz) = parse_date_or_datetime(right, true)?;
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
            start_tz: end_tz,
            end_tz,
        })
    } else if right_is_duration {
        // start/duration — the computed end inherits the start's supplied zone
        if UnboundedEndpoint::from_str(left).is_some() {
            return Err(IntervalParseError::UnboundedWithDuration(s.to_string()));
        }
        let (start_seconds, start_nanos, start_type, start_tz) =
            parse_date_or_datetime(left, false)?;
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
            start_tz,
            end_tz: start_tz,
        })
    } else {
        // start/end (explicit) — endpoints may be finite, open (`..`) or
        // unknown (empty) per ISO 8601-2; non-finite endpoints carry a
        // canonical zero duration.
        let (start_seconds, start_nanos, start_type, start_tz) =
            match UnboundedEndpoint::from_str(left) {
                Some(ep) => ep.sentinel(false),
                None => parse_date_or_datetime(left, false)?,
            };
        let (end_seconds, end_nanos, end_type, end_tz) = match UnboundedEndpoint::from_str(right) {
            Some(ep) => ep.sentinel(true),
            None => parse_date_or_datetime(right, true)?,
        };
        let duration = if start_type < INTERVAL_COMPONENT_OPEN && end_type < INTERVAL_COMPONENT_OPEN
        {
            compute_duration_from_endpoints(start_seconds, start_nanos, end_seconds, end_nanos)
        } else {
            Duration::ZERO
        };
        Ok(DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag: INTERVAL_FLAG_EXPLICIT,
            duration,
            start_tz,
            end_tz,
        })
    }
}

/// Parse a date (`YYYY-MM-DD`) or datetime (`YYYY-MM-DDTHH:MM:SS[.fff][Z|+hh:mm|-hh:mm]`) string.
/// Returns (unix_seconds, nanos, component_type, tz_offset_minutes). The instant is
/// always UTC; the offset is the supplied zone designator in minutes (`Z` and
/// date components give 0), or `INTERVAL_NO_ZONE` for a zone-less datetime.
/// When `is_end` is true and the component is a date, the date is bumped to the
/// next calendar day, matching the half-open interval convention for unqualified
/// ISO 8601 end dates.
pub(crate) fn parse_date_or_datetime(
    s: &str,
    is_end: bool,
) -> Result<(i64, u32, u8, i16), IntervalParseError> {
    if s.contains('T') {
        // DateTime: try RFC 3339 first (handles Z and numeric offsets and keeps
        // the offset), then fall back to naive datetimes which are local time.
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok((
                dt.timestamp(),
                dt.timestamp_subsec_nanos(),
                INTERVAL_COMPONENT_DATETIME,
                (dt.offset().local_minus_utc() / 60) as i16,
            ));
        }
        let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
            .map_err(|_| IntervalParseError::InvalidDateTime(s.to_string()))?;
        let dt = ndt.and_utc();
        Ok((
            dt.timestamp(),
            dt.timestamp_subsec_nanos(),
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_NO_ZONE,
        ))
    } else {
        // Date only — treated as a whole UTC day, so offset 0 (renders as Z).
        let mut nd = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| IntervalParseError::InvalidDate(s.to_string()))?;
        if is_end {
            nd = nd.succ_opt().unwrap_or(nd);
        }
        let ndt = nd.and_hms_opt(0, 0, 0).unwrap();
        Ok((ndt.and_utc().timestamp(), 0, INTERVAL_COMPONENT_DATE, 0))
    }
}

/// Designators in the ISO 8601 order used for duration parsing.
/// The order is: Y < M < W < D < H < Minute < S.
/// `W` may not be combined with `Y`/`M`/`D` (enforced at parse).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DurationDesignator {
    Year,
    Month,
    Week,
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
pub(crate) fn parse_iso_duration(s: &str) -> Result<Duration, IntervalParseError> {
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
        let mut seen_week = false;
        for ch in date_part.chars() {
            match ch {
                // ISO 8601: weeks may not be combined with Y/M/D
                'Y' | 'M' | 'D' if seen_week => return Err(err()),
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
                'W' => {
                    let d = DurationDesignator::Week;
                    validate_designator_order(&last_designator, d).map_err(|_| err())?;
                    if last_designator.is_some() {
                        // W must be the only date designator
                        return Err(err());
                    }
                    last_designator = Some(d);
                    let weeks = num_buf.parse::<u8>().map_err(|_| err())?;
                    day = weeks.checked_mul(7).ok_or_else(err)?;
                    num_buf.clear();
                    seen_week = true;
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
pub(crate) fn compute_duration_from_endpoints(
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
pub(crate) fn compute_end_from_start_and_duration(
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
pub(crate) fn compute_start_from_duration_and_end(
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

/// Number of tail bytes after the 24-byte sort prefix in the interval encoding.
const INTERVAL_TAIL_WIDTH: usize = INTERVAL_ENCODED_WIDTH - 24;

/// Craft a 52-byte probe key for a start boundary at `(secs, nanos)`. The tail
/// decides the ordering relative to entries sharing that start: `max_tail`
/// sorts the probe after every real entry (a real start_type byte can never be
/// 0xFF), `!max_tail` sorts it before them except a pathological all-zero
/// entry, which is reported via `IdLookupResult::Found`.
fn interval_probe_key(secs: i64, nanos: u32, max_tail: bool) -> Bytes {
    let mut buf = BytesMut::with_capacity(INTERVAL_ENCODED_WIDTH);
    buf.extend_from_slice(&secs.to_lexical());
    buf.put_u32(nanos);
    if max_tail {
        buf.extend_from_slice(&i64::MAX.to_lexical());
        buf.put_u32(u32::MAX);
        buf.extend_from_slice(&[0xFF; INTERVAL_TAIL_WIDTH]);
    } else {
        buf.extend_from_slice(&i64::MIN.to_lexical());
        buf.put_u32(0);
        buf.extend_from_slice(&[0; INTERVAL_TAIL_WIDTH]);
    }
    buf.freeze()
}

impl TypedDict {
    /// Global id range `[offset+1, offset+1+n)` of the DateTimeInterval segment.
    pub fn interval_segment(&self) -> Option<Range<u64>> {
        self.type_segment(Datatype::DateTimeInterval)
            .map(|(d, off)| (off + 1)..(off + 1 + d.num_entries() as u64))
    }

    /// Binary-search a crafted probe key within the interval segment. Returns
    /// `(k, found)` where `k` is the global 1-based id of the greatest entry
    /// less than the probe (or the entry itself when `found`).
    fn interval_probe(&self, secs: i64, nanos: u32, max_tail: bool) -> Option<(u64, bool)> {
        match self.id_slice(
            Datatype::DateTimeInterval,
            &interval_probe_key(secs, nanos, max_tail),
        ) {
            IdLookupResult::Found(k) => Some((k, true)),
            IdLookupResult::Closest(k) => Some((k, false)),
            IdLookupResult::NotFound => None,
        }
    }

    /// Ids of `DateTimeInterval` entries with `start <= (secs, nanos)`,
    /// as a half-open global id range.
    pub fn interval_ids_start_le(&self, secs: i64, nanos: u32) -> Range<u64> {
        let Some(seg) = self.interval_segment() else {
            return 0..0;
        };
        match self.interval_probe(secs, nanos, true) {
            Some((k, _)) => seg.start..(k + 1).min(seg.end),
            None => 0..0,
        }
    }

    /// Ids of entries with `start < (secs, nanos)`.
    pub fn interval_ids_start_lt(&self, secs: i64, nanos: u32) -> Range<u64> {
        let Some(seg) = self.interval_segment() else {
            return 0..0;
        };
        match self.interval_probe(secs, nanos, false) {
            Some((k, found)) => seg.start..(if found { k } else { k + 1 }).min(seg.end),
            None => 0..0,
        }
    }

    /// Global id of the first unknown-start entry (`start ==
    /// INTERVAL_POS_INF`): the upper cap for `start_ge`/`start_gt` ranges so
    /// unknown starts satisfy no instant comparison.
    fn interval_unknown_start_bound(&self, seg: &Range<u64>) -> u64 {
        match self.interval_probe(INTERVAL_UNKNOWN_START, 0, false) {
            Some((k, found)) => (if found { k } else { k + 1 }).clamp(seg.start, seg.end),
            None => seg.end,
        }
    }

    /// Ids of entries with `start > (secs, nanos)`. Unknown starts are
    /// excluded: an unspecified start satisfies no instant comparison.
    pub fn interval_ids_start_gt(&self, secs: i64, nanos: u32) -> Range<u64> {
        let Some(seg) = self.interval_segment() else {
            return 0..0;
        };
        let cap = self.interval_unknown_start_bound(&seg);
        match self.interval_probe(secs, nanos, true) {
            Some((k, _)) => (k + 1).clamp(seg.start, cap)..cap,
            None => 0..0,
        }
    }

    /// Ids of entries with `start >= (secs, nanos)`. Unknown starts are
    /// excluded.
    pub fn interval_ids_start_ge(&self, secs: i64, nanos: u32) -> Range<u64> {
        let Some(seg) = self.interval_segment() else {
            return 0..0;
        };
        let cap = self.interval_unknown_start_bound(&seg);
        match self.interval_probe(secs, nanos, false) {
            Some((k, found)) => (if found { k } else { k + 1 }).clamp(seg.start, cap)..cap,
            None => 0..0,
        }
    }

    /// Ids of entries with `start == (secs, nanos)`.
    pub fn interval_ids_start_eq(&self, secs: i64, nanos: u32) -> Range<u64> {
        let Some(seg) = self.interval_segment() else {
            return 0..0;
        };
        let lo = match self.interval_probe(secs, nanos, false) {
            Some((k, found)) => (if found { k } else { k + 1 }).clamp(seg.start, seg.end),
            None => return 0..0,
        };
        let hi = match self.interval_probe(secs, nanos, true) {
            Some((k, _)) => (k + 1).min(seg.end),
            None => return 0..0,
        };
        lo..hi.max(lo)
    }
}

impl TypedDict {
    /// Return a dictionary whose DateTimeInterval segment uses the 52-byte
    /// encoding. Legacy 48-byte entries are decoded and re-encoded with a
    /// zero zone offset (they were always rendered `Z`). Intended to run
    /// once at layer-open so every downstream reader sees a single width;
    /// returns a clone unchanged when the segment is already 52-byte.
    pub fn upgrade_legacy_intervals(&self) -> Self {
        let Some(seg) = self.interval_segment() else {
            return self.clone();
        };
        let legacy = seg.clone().any(|id| {
            self.entry(id as usize)
                .expect("interval id in segment")
                .to_bytes()
                .len()
                != INTERVAL_ENCODED_WIDTH
        });
        if !legacy {
            return self.clone();
        }

        let mut builder = TypedDictBufBuilder::new(
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
        );
        for entry in self.iter() {
            let entry = if entry.datatype() == Datatype::DateTimeInterval {
                let bytes = entry.to_bytes();
                let upgraded = match bytes.len() {
                    INTERVAL_LEGACY_ENCODED_WIDTH => {
                        DateTimeInterval::from_legacy_lexical(bytes).to_lexical()
                    }
                    INTERVAL_ENCODED_WIDTH => bytes,
                    n => panic!(
                        "invalid DateTimeInterval encoding width: expected {} or {}, got {}",
                        INTERVAL_ENCODED_WIDTH, INTERVAL_LEGACY_ENCODED_WIDTH, n
                    ),
                };
                TypedDictEntry::new(Datatype::DateTimeInterval, SizedDictEntry::from(upgraded))
            } else {
                entry
            };
            builder.add(entry);
        }
        let (t, o, bo, d) = builder.finalize();
        TypedDict::from_parts(t.freeze(), o.freeze(), bo.freeze(), d.freeze())
    }
}

/// Dense-rank index on interval end timestamps over a TypedDict's
/// DateTimeInterval segment.
///
/// `ends` is a stride-2 LogArray of the sorted distinct `(end_secs,
/// end_nanos)` keys; `tree` encodes each segment position's end rank.
/// Positions are 0-based within the interval segment.
#[derive(Clone)]
pub struct IntervalEndIndex {
    ends: LogArray,
    tree: WaveletTree,
}

/// Wavelet alphabet width needed to encode `n` distinct end ranks.
/// Minimum 1: width 0 would yield an empty tree of length 0.
pub(crate) fn end_rank_width(n: usize) -> u8 {
    (u64::BITS - n.saturating_sub(1).leading_zeros()).max(1) as u8
}

/// `(end_secs, end_nanos)` per segment position, in dictionary order.
fn interval_segment_ends(dict: &TypedDict) -> Vec<(i64, u32)> {
    dict.interval_segment()
        .map(|seg| {
            seg.map(|id| {
                let entry = dict.entry(id as usize).expect("interval id in segment");
                let iv = <DateTimeInterval as FromLexical<DateTimeInterval>>::from_lexical(
                    entry.to_bytes(),
                );
                (iv.end_seconds, iv.end_nanos)
            })
            .collect()
        })
        .unwrap_or_default()
}

impl IntervalEndIndex {
    pub fn from_parts(ends: LogArray, bits: BitIndex) -> Self {
        let tree = WaveletTree::from_parts(bits, end_rank_width(ends.len() / 2));
        IntervalEndIndex { ends, tree }
    }

    /// Build the index in memory over a dictionary's DateTimeInterval
    /// segment (used at layer-open for layers without index files).
    pub fn build(dict: &TypedDict) -> Self {
        let ends = interval_segment_ends(dict);
        let mut distinct = ends.clone();
        distinct.sort_unstable();
        distinct.dedup();

        let mut ends_builder = LogArrayBufBuilder::new(BytesMut::new(), 64);
        for &(s, n) in &distinct {
            ends_builder.push(s as u64);
            ends_builder.push(n as u64);
        }
        let ends_table =
            LogArray::parse(ends_builder.finalize().freeze()).expect("interval end rank table");

        let ranks = ends
            .iter()
            .map(|e| distinct.binary_search(e).unwrap() as u64);
        let tree = wavelet_tree_from_iter(end_rank_width(distinct.len()), ranks);
        IntervalEndIndex {
            ends: ends_table,
            tree,
        }
    }

    /// Number of interval positions covered.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.len() == 0
    }

    /// Number of distinct end keys in the rank table.
    pub fn num_distinct_ends(&self) -> usize {
        self.ends.len() / 2
    }

    /// First rank whose end key is `>= (secs, nanos)`.
    pub fn end_lower_bound(&self, secs: i64, nanos: u32) -> u64 {
        self.end_search(secs, nanos, false)
    }

    /// First rank whose end key is `> (secs, nanos)`.
    pub fn end_upper_bound(&self, secs: i64, nanos: u32) -> u64 {
        self.end_search(secs, nanos, true)
    }

    fn end_search(&self, secs: i64, nanos: u32, upper: bool) -> u64 {
        let n = self.num_distinct_ends() as u64;
        let (mut lo, mut hi) = (0u64, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let s = self.ends.entry((2 * mid) as usize) as i64;
            let ns = self.ends.entry((2 * mid + 1) as usize) as u32;
            let before = if upper {
                (s, ns) <= (secs, nanos)
            } else {
                (s, ns) < (secs, nanos)
            };
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// First rank above the unknown-end block (rank keys `(_, _)` >
    /// `(INTERVAL_UNKNOWN_END, 0)`): NULL-semantics cut for the `le`/`lt`
    /// family, whose ranges otherwise reach rank 0.
    fn first_non_unknown_end_rank(&self) -> u64 {
        self.end_lower_bound(INTERVAL_UNKNOWN_END + 1, 0)
    }

    /// Segment positions in `pos` with `end <= (secs, nanos)`. Unknown ends
    /// are excluded: an unspecified end satisfies no instant comparison.
    pub fn positions_end_le(&self, pos: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        match self.end_upper_bound(secs, nanos) {
            0 => Vec::new(),
            hi => self
                .tree
                .positions_in_range(pos, self.first_non_unknown_end_rank(), hi - 1),
        }
    }

    /// Segment positions in `pos` with `end < (secs, nanos)`. Unknown ends
    /// are excluded.
    pub fn positions_end_lt(&self, pos: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        match self.end_lower_bound(secs, nanos) {
            0 => Vec::new(),
            hi => self
                .tree
                .positions_in_range(pos, self.first_non_unknown_end_rank(), hi - 1),
        }
    }

    /// Segment positions in `pos` with `end >= (secs, nanos)`.
    pub fn positions_end_ge(&self, pos: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.tree
            .positions_ge(pos, self.end_lower_bound(secs, nanos))
    }

    /// Segment positions in `pos` with `end > (secs, nanos)`.
    pub fn positions_end_gt(&self, pos: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.tree
            .positions_ge(pos, self.end_upper_bound(secs, nanos))
    }

    /// Segment positions in `pos` with `end == (secs, nanos)`.
    pub fn positions_end_eq(&self, pos: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        let lo = self.end_lower_bound(secs, nanos);
        let hi = self.end_upper_bound(secs, nanos);
        if lo >= hi {
            Vec::new()
        } else {
            self.tree.positions_in_range(pos, lo, hi - 1)
        }
    }

    /// Segment positions in `pos` with `end` in `[lo, hi]` inclusive.
    /// Unknown ends are excluded.
    pub fn positions_end_range(&self, pos: Range<u64>, lo: (i64, u32), hi: (i64, u32)) -> Vec<u64> {
        let r_lo = self
            .end_lower_bound(lo.0, lo.1)
            .max(self.first_non_unknown_end_rank());
        let r_hi = self.end_upper_bound(hi.0, hi.1);
        if r_lo >= r_hi {
            Vec::new()
        } else {
            self.tree.positions_in_range(pos, r_lo, r_hi - 1)
        }
    }
}

/// Build the interval end index for a dictionary's DateTimeInterval segment
/// into four files: the stride-2 end rank table (`ends`), and the wavelet
/// tree's `bits`, `blocks` and `sblocks`.
pub async fn build_interval_end_index<F: 'static + FileLoad + FileStore>(
    dict: &TypedDict,
    destination_ends: F,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    let mut ends: Vec<(i64, u32)> = Vec::new();
    if let Some(seg) = dict.interval_segment() {
        for id in seg {
            let entry = dict.entry(id as usize).expect("interval id in segment");
            let iv =
                <DateTimeInterval as FromLexical<DateTimeInterval>>::from_lexical(entry.to_bytes());
            ends.push((iv.end_seconds, iv.end_nanos));
        }
    }

    let mut distinct = ends.clone();
    distinct.sort_unstable();
    distinct.dedup();

    // stride-2 rank table: (secs bitcast, nanos) pairs, sorted
    let mut ends_builder = LogArrayFileBuilder::new(destination_ends.open_write().await?, 64);
    for &(s, n) in &distinct {
        ends_builder.push(s as u64).await?;
        ends_builder.push(n as u64).await?;
    }
    ends_builder.finalize().await?;

    // rank of each position's end key
    let ranks: Vec<u64> = ends
        .iter()
        .map(|e| distinct.binary_search(e).unwrap() as u64)
        .collect();
    // alphabet must cover the distinct count
    let width = end_rank_width(distinct.len());
    build_wavelet_tree_from_iter(
        width,
        ranks.into_iter(),
        destination_bits,
        destination_blocks,
        destination_sblocks,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tfc::datatypes::{FromLexical, TdbDataType, ToLexical};
    use crate::tfc::typed::{TypedDictBufBuilder, TypedDictEntry};

    fn iv(s: &str) -> TypedDictEntry {
        DateTimeInterval::make_entry(&parse_iso_interval(s).unwrap())
    }

    fn build_dict(mut entries: Vec<TypedDictEntry>) -> TypedDict {
        entries.sort();
        let mut b = TypedDictBufBuilder::new(
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
        );
        b.add_all(entries.into_iter());
        let (t, o, bo, d) = b.finalize();
        TypedDict::from_parts(t.freeze(), o.freeze(), bo.freeze(), d.freeze())
    }

    /// Two strings (ids 1-2) then four intervals (ids 3-6):
    /// 3: start 2025-01-01, 4-5: start 2025-03-01 (duplicate, different ends),
    /// 6: start 2025-06-01.
    fn interval_dict() -> TypedDict {
        build_dict(vec![
            String::make_entry(&"alpha".to_string()),
            String::make_entry(&"beta".to_string()),
            iv("2025-01-01/2025-02-01"),
            iv("2025-03-01/2025-04-01"),
            iv("2025-03-01/2025-05-01"),
            iv("2025-06-01/2025-07-01"),
        ])
    }

    const S1: i64 = 1735689600; // 2025-01-01T00:00:00Z
    const S2: i64 = 1740787200; // 2025-03-01T00:00:00Z
    const S4: i64 = 1748736000; // 2025-06-01T00:00:00Z

    #[test]
    fn probe_start_le_includes_equal_start() {
        let dict = interval_dict();
        assert_eq!(3..6, dict.interval_ids_start_le(S2, 0));
        assert_eq!(3..7, dict.interval_ids_start_le(S4, 0));
    }

    #[test]
    fn probe_start_lt_excludes_equal_start() {
        let dict = interval_dict();
        assert_eq!(3..4, dict.interval_ids_start_lt(S2, 0));
    }

    #[test]
    fn probe_start_gt_excludes_equal_start() {
        let dict = interval_dict();
        assert_eq!(6..7, dict.interval_ids_start_gt(S2, 0));
    }

    #[test]
    fn probe_start_ge_includes_equal_start() {
        let dict = interval_dict();
        assert_eq!(4..7, dict.interval_ids_start_ge(S2, 0));
    }

    #[test]
    fn probe_start_eq_range_over_duplicate_starts() {
        let dict = interval_dict();
        assert_eq!(4..6, dict.interval_ids_start_eq(S2, 0));
        assert_eq!(3..4, dict.interval_ids_start_eq(S1, 0));
    }

    #[test]
    fn probe_boundaries_and_missing_segment() {
        let dict = interval_dict();
        // Below first start / above last start
        assert_eq!(3..3, dict.interval_ids_start_le(S1 - 1, 0));
        assert_eq!(7..7, dict.interval_ids_start_gt(S4, 0));
        // Between two starts
        assert_eq!(3..4, dict.interval_ids_start_le(S1 + 1, 0));
        assert_eq!(4..7, dict.interval_ids_start_gt(S1 + 1, 0));
        // No interval segment at all
        let str_dict = build_dict(vec![String::make_entry(&"only".to_string())]);
        assert_eq!(0..0, str_dict.interval_ids_start_le(S2, 0));
        assert_eq!(0..0, str_dict.interval_ids_start_eq(S2, 0));
    }

    fn build_end_index(dict: &TypedDict) -> IntervalEndIndex {
        use crate::storage::memory::MemoryBackedStore;
        use futures::executor::block_on;

        let ends_f = MemoryBackedStore::new();
        let bits_f = MemoryBackedStore::new();
        let blocks_f = MemoryBackedStore::new();
        let sblocks_f = MemoryBackedStore::new();
        block_on(build_interval_end_index(
            dict,
            ends_f.clone(),
            bits_f.clone(),
            blocks_f.clone(),
            sblocks_f.clone(),
        ))
        .unwrap();
        let ends = LogArray::parse(block_on(ends_f.map()).unwrap()).unwrap();
        let bits = BitIndex::from_maps(
            block_on(bits_f.map()).unwrap(),
            block_on(blocks_f.map()).unwrap(),
            block_on(sblocks_f.map()).unwrap(),
        );
        IntervalEndIndex::from_parts(ends, bits)
    }

    /// Decoded (end_secs, end_nanos) per segment position for a dict.
    fn segment_ends(dict: &TypedDict) -> Vec<(i64, u32)> {
        interval_segment_ends(dict)
    }

    #[test]
    fn interval_end_index_matches_scan() {
        // intervals with distinct, duplicate and boundary ends
        let dict = build_dict(vec![
            iv("2025-01-01/2025-02-01"),
            iv("2025-02-01/2025-03-01"),
            iv("2025-03-01/2025-03-01"), // empty interval, end == mid values
            iv("2025-04-01/2025-03-15"),
            iv("2025-05-01/2025-06-01"),
        ]);
        let ends = segment_ends(&dict);
        let idx = build_end_index(&dict);
        let n = ends.len() as u64;
        assert_eq!(n as usize, idx.len());

        let scan = |pred: &dyn Fn((i64, u32)) -> bool, pos: Range<u64>| -> Vec<u64> {
            (pos.start..pos.end.min(ends.len() as u64))
                .filter(|&i| pred(ends[i as usize]))
                .collect()
        };
        let t = |s: &str| {
            let iv = parse_iso_interval(&format!("{s}/{s}")).unwrap();
            (iv.end_seconds, iv.end_nanos)
        };
        let t_mar1 = t("2025-03-01");
        let t_mar15 = t("2025-03-15");

        // in-memory build agrees with the file-built index
        let mem_idx = IntervalEndIndex::build(&dict);
        assert_eq!(idx.len(), mem_idx.len());
        assert_eq!(
            idx.positions_end_le(0..n, i64::MAX, u32::MAX),
            mem_idx.positions_end_le(0..n, i64::MAX, u32::MAX)
        );
        assert_eq!(
            idx.positions_end_range(0..n, t_mar1, t_mar15),
            mem_idx.positions_end_range(0..n, t_mar1, t_mar15)
        );

        for pos in [0..n, 1..4, 0..0] {
            assert_eq!(
                scan(&|e| e <= t_mar1, pos.clone()),
                idx.positions_end_le(pos.clone(), t_mar1.0, t_mar1.1)
            );
            assert_eq!(
                scan(&|e| e < t_mar15, pos.clone()),
                idx.positions_end_lt(pos.clone(), t_mar15.0, t_mar15.1)
            );
            assert_eq!(
                scan(&|e| e >= t_mar1, pos.clone()),
                idx.positions_end_ge(pos.clone(), t_mar1.0, t_mar1.1)
            );
            assert_eq!(
                scan(&|e| e > t_mar1, pos.clone()),
                idx.positions_end_gt(pos.clone(), t_mar1.0, t_mar1.1)
            );
            assert_eq!(
                scan(&|e| e == t_mar1, pos.clone()),
                idx.positions_end_eq(pos.clone(), t_mar1.0, t_mar1.1)
            );
            assert_eq!(
                scan(&|e| e >= t_mar1 && e <= t_mar15, pos.clone()),
                idx.positions_end_range(pos.clone(), t_mar1, t_mar15)
            );
        }
        // bounds outside the present keys
        assert!(idx.positions_end_le(0..n, 0, 0).is_empty());
        assert_eq!(5, idx.positions_end_ge(0..n, 0, 0).len());
        assert_eq!(
            n as usize,
            idx.positions_end_range(0..n, (0, 0), (i64::MAX, u32::MAX))
                .len()
        );
    }

    #[test]
    fn upgrade_legacy_intervals_converts_48_to_52() {
        let iv1 = parse_iso_interval("2025-01-01/2025-02-01").unwrap();
        let iv2 = parse_iso_interval("2025-03-01/2025-04-01").unwrap();
        // craft legacy entries by truncating the timezone tail
        let legacy = |iv: &DateTimeInterval| {
            TypedDictEntry::new(
                Datatype::DateTimeInterval,
                SizedDictEntry::from(iv.to_lexical().slice(0..INTERVAL_LEGACY_ENCODED_WIDTH)),
            )
        };
        let dict = build_dict(vec![
            String::make_entry(&"s".to_string()),
            legacy(&iv1),
            legacy(&iv2),
        ]);

        let up = dict.upgrade_legacy_intervals();
        let bytes = up.entry(2).unwrap().to_bytes();
        assert_eq!(INTERVAL_ENCODED_WIDTH, bytes.len());
        let decoded = <DateTimeInterval as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        // legacy data always rendered Z -> zero offset
        assert_eq!(0, decoded.start_tz);
        assert_eq!(0, decoded.end_tz);
        let rendered =
            <String as FromLexical<DateTimeInterval>>::from_lexical(decoded.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/2025-02-02T00:00:00Z", rendered);
        // probes see the upgraded segment
        assert_eq!(2..4, up.interval_ids_start_le(iv2.start_seconds, 0));

        // mixed-width segment (delta inheriting parent entries): per-entry
        // dispatch upgrades only the legacy records
        let iv3 = parse_iso_interval("2025-05-01/2025-06-01").unwrap();
        let mixed = build_dict(vec![
            legacy(&iv1),
            legacy(&iv2),
            DateTimeInterval::make_entry(&iv3),
        ]);
        let up = mixed.upgrade_legacy_intervals();
        for id in up.interval_segment().unwrap() {
            assert_eq!(
                INTERVAL_ENCODED_WIDTH,
                up.entry(id as usize).unwrap().to_bytes().len()
            );
        }
        assert_eq!(1..3, up.interval_ids_start_le(iv2.start_seconds, 0));

        // a 52-byte dictionary is returned unchanged
        let fresh = interval_dict();
        let same = fresh.upgrade_legacy_intervals();
        assert_eq!(
            INTERVAL_ENCODED_WIDTH,
            same.entry(3).unwrap().to_bytes().len()
        );
    }

    #[test]
    fn probe_nanos_distinguish_same_second() {
        // Two entries with the same start second, nanos 0 and 500
        let dict = build_dict(vec![
            iv("2025-01-01T00:00:00Z/2025-02-01"),
            iv("2025-01-01T00:00:00.5Z/2025-02-01"),
        ]);
        assert_eq!(1..2, dict.interval_ids_start_lt(S1, 500_000_000));
        assert_eq!(2..3, dict.interval_ids_start_ge(S1, 500_000_000));
    }

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
        // +02:00 means the local 09:00:00 is 07:00:00Z; the offset is kept.
        let s = "2025-01-01T09:00:00.000+02:00/2025-01-01T09:00:00.123456789+02:00";
        let iv = parse_iso_interval(s).unwrap();
        assert_eq!(iv.start_seconds, 1735689600 + 7 * 3600);
        assert_eq!(iv.start_nanos, 0);
        assert_eq!(iv.end_seconds, 1735689600 + 7 * 3600);
        assert_eq!(iv.end_nanos, 123_456_789);
        assert_eq!(iv.duration.second, 0.123456789);
        assert_eq!(iv.start_tz, 120);
        assert_eq!(iv.end_tz, 120);
    }

    #[test]
    fn parse_zoneless_datetime_is_local() {
        // XBRL OIM: a datetime without a zone designator is local time and
        // must stay distinguishable from UTC.
        let iv = parse_iso_interval("2025-01-01T09:00:00/2025-03-31T18:30:00").unwrap();
        assert_eq!(iv.start_tz, INTERVAL_NO_ZONE);
        assert_eq!(iv.end_tz, INTERVAL_NO_ZONE);
        assert_eq!(iv.start_seconds, 1735722000); // stored as the UTC instant
    }

    #[test]
    fn parse_z_datetime_is_offset_zero() {
        let iv = parse_iso_interval("2025-01-01T09:00:00Z/2025-01-02T00:00:00Z").unwrap();
        assert_eq!(iv.start_tz, 0);
        assert_eq!(iv.end_tz, 0);
    }

    #[test]
    fn parse_dates_keep_utc_convention() {
        let iv = parse_iso_interval("2025-01-01/2025-03-31").unwrap();
        assert_eq!(iv.start_tz, 0);
        assert_eq!(iv.end_tz, 0);
    }

    #[test]
    fn duration_forms_inherit_supplied_zone() {
        let iv = parse_iso_interval("2025-01-01T09:00:00+02:00/P3M").unwrap();
        assert_eq!(iv.start_tz, 120);
        assert_eq!(iv.end_tz, 120);
        let iv = parse_iso_interval("P3M/2025-04-01T09:00:00-05:00").unwrap();
        assert_eq!(iv.start_tz, -300);
        assert_eq!(iv.end_tz, -300);
    }

    #[test]
    fn roundtrip_explicit_datetime_with_timezone_offset_and_nanos() {
        // OIM fidelity: the supplied offset round-trips instead of normalizing to Z.
        let s = "2025-01-01T09:00:00.000+02:00/2025-01-01T09:00:00.123456789+02:00";
        let iv = parse_iso_interval(s).unwrap();
        let round = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!(
            round,
            "2025-01-01T09:00:00+02:00/2025-01-01T09:00:00.123456789+02:00"
        );
    }

    #[test]
    fn roundtrip_zoneless_datetime_stays_zoneless() {
        let s = "2025-01-01T09:00:00/2025-03-31T18:30:00";
        let iv = parse_iso_interval(s).unwrap();
        let round = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!(round, s);
    }

    #[test]
    fn roundtrip_mixed_zones() {
        let s = "2025-01-01T09:00:00-05:00/2025-03-31T18:30:00+05:30";
        let iv = parse_iso_interval(s).unwrap();
        let round = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!(round, s);
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
    fn parse_datetime_with_offset_roundtrips_supplied_zone() {
        let iv = parse_iso_interval("2025-01-01T10:30:00+02:00/2025-04-01T15:45:00+02:00").unwrap();
        let bytes = iv.to_lexical();
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(bytes);
        assert_eq!("2025-01-01T10:30:00+02:00/2025-04-01T15:45:00+02:00", s);
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

    // --- ISO 8601-2 open and unknown endpoints ---

    #[test]
    fn parse_open_end() {
        let iv = parse_iso_interval("2024-01-01T00:00:00Z/..").unwrap();
        assert!(iv.is_end_open());
        assert!(!iv.is_start_open());
        assert_eq!(iv.end_seconds, INTERVAL_POS_INF);
        assert_eq!(iv.end_type, INTERVAL_COMPONENT_OPEN);
        assert_eq!(iv.end_nanos, 0);
        assert_eq!(iv.end_tz, INTERVAL_NO_ZONE);
        assert_eq!(iv.flag, INTERVAL_FLAG_EXPLICIT);
        assert!(iv.duration.is_zero());
    }

    #[test]
    fn parse_open_start() {
        let iv = parse_iso_interval("../2024-01-01T00:00:00Z").unwrap();
        assert!(iv.is_start_open());
        assert_eq!(iv.start_seconds, INTERVAL_NEG_INF);
        assert_eq!(iv.start_type, INTERVAL_COMPONENT_OPEN);
    }

    #[test]
    fn parse_forever() {
        let iv = parse_iso_interval("../..").unwrap();
        assert!(iv.is_start_open() && iv.is_end_open());
    }

    #[test]
    fn parse_unknown_end() {
        let iv = parse_iso_interval("2024-01-01T00:00:00Z/").unwrap();
        assert!(iv.is_end_unknown());
        assert_eq!(iv.end_seconds, INTERVAL_UNKNOWN_END);
        assert_eq!(iv.end_type, INTERVAL_COMPONENT_UNKNOWN);
    }

    #[test]
    fn parse_unknown_start() {
        let iv = parse_iso_interval("/2024-01-01T00:00:00Z").unwrap();
        assert!(iv.is_start_unknown());
        assert_eq!(iv.start_seconds, INTERVAL_UNKNOWN_START);
    }

    #[test]
    fn parse_both_unknown() {
        let iv = parse_iso_interval("/").unwrap();
        assert!(iv.is_start_unknown() && iv.is_end_unknown());
    }

    #[test]
    fn duration_with_unbounded_endpoint_is_error() {
        for s in ["P1Y/..", "P1Y/", "../P1Y", "/P1Y"] {
            assert!(
                matches!(
                    parse_iso_interval(s),
                    Err(IntervalParseError::UnboundedWithDuration(_))
                ),
                "{s}"
            );
        }
    }

    #[test]
    fn open_and_unknown_round_trip() {
        for s in [
            "2024-01-01T00:00:00Z/..",
            "../2024-01-01T00:00:00Z",
            "../..",
            "2024-01-01T00:00:00Z/",
            "/2024-01-01T00:00:00Z",
            "/",
            "../",
        ] {
            let iv = parse_iso_interval(s).unwrap();
            let round = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
            assert_eq!(s, round, "round trip {s}");
        }
    }

    fn raw_interval_bytes(
        start_seconds: i64,
        start_type: u8,
        end_seconds: i64,
        end_type: u8,
    ) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&start_seconds.to_lexical());
        buf.put_u32(0u32);
        buf.extend_from_slice(&end_seconds.to_lexical());
        buf.put_u32(0u32);
        buf.put_u8(start_type);
        buf.put_u8(end_type);
        buf.put_u8(INTERVAL_FLAG_EXPLICIT);
        buf.extend_from_slice(&Duration::ZERO.to_lexical());
        buf.put_i16(INTERVAL_NO_ZONE);
        buf.put_i16(INTERVAL_NO_ZONE);
        buf.freeze()
    }

    #[test]
    #[should_panic(expected = "finite interval endpoint on sentinel")]
    fn finite_endpoint_on_sentinel_is_rejected() {
        let bytes = raw_interval_bytes(
            INTERVAL_NEG_INF,
            INTERVAL_COMPONENT_DATE,
            1700000000,
            INTERVAL_COMPONENT_DATE,
        );
        DateTimeInterval::from_lexical(bytes);
    }

    #[test]
    #[should_panic(expected = "open interval endpoint with non-sentinel")]
    fn open_endpoint_without_sentinel_is_rejected() {
        let bytes = raw_interval_bytes(
            1700000000,
            INTERVAL_COMPONENT_OPEN,
            1700000000,
            INTERVAL_COMPONENT_DATE,
        );
        DateTimeInterval::from_lexical(bytes);
    }

    #[test]
    #[should_panic(expected = "unknown interval component type")]
    fn unknown_component_type_is_rejected() {
        let bytes = raw_interval_bytes(1700000000, 99, 1700000000, INTERVAL_COMPONENT_DATE);
        DateTimeInterval::from_lexical(bytes);
    }

    fn iv_entry(s: &str) -> TypedDictEntry {
        DateTimeInterval::make_entry(&parse_iso_interval(s).unwrap())
    }

    #[test]
    fn start_probes_exclude_unknown_starts() {
        // sort order: open start (-inf) < finite starts < unknown start (+inf)
        let dict = build_dict(vec![
            iv_entry("../2024-01-01T00:00:00Z"),
            iv_entry("2024-01-01T00:00:10Z/2024-01-01T00:00:20Z"),
            iv_entry("/2024-01-01T00:00:00Z"),
        ]);
        let seg = dict.interval_segment().unwrap();
        let t10 = parse_iso_interval("2024-01-01T00:00:10Z/2024-01-01T00:00:20Z")
            .unwrap()
            .start_seconds;

        // open start (-inf) and the finite start satisfy start <= t10; the
        // unknown start does not participate
        assert_eq!(seg.start..seg.start + 2, dict.interval_ids_start_le(t10, 0));
        // only finite starts satisfy start >= a finite bound: the open start
        // (-inf) is below it and the unknown start is cut above
        assert_eq!(
            seg.start + 1..seg.end - 1,
            dict.interval_ids_start_ge(t10, 0)
        );
        assert_eq!(seg.start + 1..seg.end - 1, dict.interval_ids_start_gt(0, 0));
        // start_le still covers the open start: -inf <= everything
        assert_eq!(seg.start..seg.start + 2, dict.interval_ids_start_le(t10, 0));
        // the unknown block is still reachable lexically via start_eq at the sentinel
        assert_eq!(
            seg.end - 1..seg.end,
            dict.interval_ids_start_eq(INTERVAL_UNKNOWN_START, 0)
        );
    }

    #[test]
    fn end_index_excludes_unknown_ends() {
        let dict = build_dict(vec![
            iv_entry("../.."),                                     // open end
            iv_entry("2024-01-01T00:00:10Z/2024-01-01T00:00:20Z"), // finite end
            iv_entry("2024-01-01T00:00:30Z/"),                     // unknown end
        ]);
        let index = IntervalEndIndex::build(&dict);
        let t20 = parse_iso_interval("2024-01-01T00:00:10Z/2024-01-01T00:00:20Z")
            .unwrap()
            .end_seconds;

        // dict order: ../.. (start -inf) first, then by end? no — dict sorts
        // by start first: ../.. (start MIN) id seg.start, finite id +1,
        // unknown-start? no — unknown END sorts by its start (t30) → id +2.
        // positions in the index are segment-relative: pos 0=../..,1=finite,2=x/
        assert_eq!(vec![1], index.positions_end_le(0..3, t20, 0));
        // end_ge includes +inf (forever) but not unknown
        assert_eq!(vec![0, 1], index.positions_end_ge(0..3, t20, 0));
        // unknown ends reachable lexically via end_eq at the sentinel
        assert_eq!(
            vec![2],
            index.positions_end_eq(0..3, INTERVAL_UNKNOWN_END, 0)
        );
        // forever satisfies containment at any instant
        assert_eq!(vec![0, 1], index.positions_end_ge(0..3, t20, 0));
    }
}
