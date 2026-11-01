//! Lexical-payload interval encoding (`xdd:dateTimeInterval` v2).
//!
//! Each entry stores the supplied interval string — lightly canonicalised
//! (date/time separators stripped, restored at render) — plus a derived sort
//! key. Ordering resolves to the tuple
//!
//! ```text
//! (start.class, start.instant, end.class, end.instant, period,
//!  start.kind, end.kind, start.tshift, end.tshift, flag, payload)
//! ```
//!
//! Instant-major: the instants order first, spelling metadata (kind,
//! timeshift, flag) only breaks ties between identical instant pairs.
//! `period` is the supplied canonical duration for `T/P`/`P/T` forms and the
//! canonical *derived* period for `T/T` — greedy `Y M D H Min S` anchored at
//! the start (`PT0S` for a zero span) — so equal spans cluster whatever the
//! form. `tshift` is the timezone axis — class byte plus signed i16 offset
//! minutes for `±HH:MM` spellings.
//!
//! so dictionary order follows the mirrored-tail component order from the
//! ordering spec: the negative tail `unbounded < unspecified < unknown < finite`
//! on the start axis, and `finite < unknown < unspecified < unbounded` on the
//! end axis.
//!
//! The instant key is the existing order-preserving decimal encoding
//! (`integer_and_fraction_to_storage`): arbitrary-precision seconds from the
//! Unix epoch plus arbitrary-precision fractional digits — exact rational
//! ordering without rational arithmetic.
//!
//! Special classes are incommensurable: an interval may contain specials of at
//! most one class (`../..`, `X*/X*`, `/` are legal; `X*/..`, `../X*`, `/..`,
//! etc. are rejected).

use super::datatypes::Duration;
use super::decimal::{decode_fraction, integer_and_fraction_to_storage};
use super::integer::storage_to_bigint_and_sign;
use super::interval::{parse_iso_duration, IntervalParseError, UnboundedEndpoint};
use bytes::{Buf, BufMut, BytesMut};
use chrono::{Datelike, NaiveDate, Timelike};
use rug::Integer;
use thiserror::Error;

// -- class bytes -------------------------------------------------------------

// Start position (negative tail): unbounded < unspecified < unknown < finite.
pub const START_UNBOUNDED: u8 = 0;
pub const START_UNSPECIFIED: u8 = 1;
pub const START_UNKNOWN: u8 = 2;
pub const START_FINITE: u8 = 3;

// End position (positive tail): finite < unknown < unspecified < unbounded.
pub const END_FINITE: u8 = 0;
pub const END_UNKNOWN: u8 = 1;
pub const END_UNSPECIFIED: u8 = 2;
pub const END_UNBOUNDED: u8 = 3;

// -- timezone axis ------------------------------------------------------------
// negative offsets < -00:00 < Z < zone-less < +00:00 < positive offsets.
// NEG/POS carry a trailing i16-BE offset in minutes (ascending by value, so
// larger negative magnitudes sort first: -14:00 < -05:00).
pub const TZ_NEG: u8 = 0;
pub const TZ_NEG_ZERO: u8 = 1;
pub const TZ_Z: u8 = 2;
pub const TZ_ZONELESS: u8 = 3;
pub const TZ_POS_ZERO: u8 = 4;
pub const TZ_POS: u8 = 5;

// -- kind / flag --------------------------------------------------------------

pub const KIND_DATE: u8 = 0;
pub const KIND_DATETIME: u8 = 1;

// -- leap seconds ---------------------------------------------------------------
//
// The instant domain counts leap seconds: `day_posix + leaps_before(day) +
// sod`. Every post-leap instant shifts by a uniform +27 (today) — order-
// preserving — while each `23:59:60` occupies its own real instant strictly
// between `23:59:59` and `00:00:00`. Durations therefore count the leap
// second exactly like any other second.
//
// IERS positive leap days (the dates on which `23:59:60` existed), as YYYYMMDD
// keys. The set is closed: the practice ends by 2035 and the last was
// 2016-12-31 — this table never grows.
const LEAP_DAYS: &[u32] = &[
    19720630, 19721231, 19731231, 19741231, 19751231, 19761231, 19771231, 19781231, 19791231,
    19810630, 19820630, 19830630, 19850630, 19871231, 19891231, 19901231, 19920630, 19930630,
    19940630, 19951231, 19970630, 19981231, 20051231, 20081231, 20120630, 20150630, 20161231,
];

/// YYYYMMDD as i64 — negative years keep their sign so deep-past dates
/// compare correctly against the leap table.
fn day_key(d: NaiveDate) -> i64 {
    d.year() as i64 * 10000 + d.month() as i64 * 100 + d.day() as i64
}

/// Count of leap days strictly before `key` — the leap-second shift that day.
fn leaps_before(key: i64) -> i64 {
    LEAP_DAYS.partition_point(|&d| (d as i64) < key) as i64
}

fn is_leap_day(key: i64) -> bool {
    key >= 0 && LEAP_DAYS.binary_search(&(key as u32)).is_ok()
}

/// Leap-aware seconds since epoch for a wall-clock `(date, sod)`. `sod` counts
/// seconds of day *including* a leap second, so `86400` is valid on leap days.
fn leap_aware_secs(date: NaiveDate, sod: i64, offset_minutes: i16) -> i64 {
    let base = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    base + leaps_before(day_key(date)) + sod - offset_minutes as i64 * 60
}

/// POSIX epoch seconds → leap-aware seconds: the domain that instants, probe
/// boundaries and end-index rank keys live in. Adds the count of leap days
/// strictly before the instant's wall date.
pub fn posix_to_leap_aware_secs(secs: i64) -> i64 {
    secs + leaps_before(day_key(
        chrono::DateTime::from_timestamp(secs, 0)
            .expect("instant in chrono range")
            .date_naive(),
    ))
}

/// Inverse of `leap_aware_secs` at zero offset: the wall-clock reading of a
/// leap-aware instant, plus whether the instant is the day's `23:59:60`
/// second. The wall date is within two days of the posix approximation — the
/// leap shift is at most 27 s.
fn leap_instant_to_naive(s: i64) -> (chrono::NaiveDateTime, bool) {
    let approx = chrono::DateTime::from_timestamp(s.saturating_sub(30), 0)
        .expect("leap instant outside chrono range")
        .date_naive();
    for i in -1..=2 {
        let d = approx + chrono::Duration::days(i);
        let key = day_key(d);
        let start = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() + leaps_before(key);
        let span = if is_leap_day(key) { 86401 } else { 86400 };
        if s >= start && s < start + span {
            let sod = s - start;
            return (
                d.and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::seconds(sod.min(86399)),
                sod == 86400,
            );
        }
    }
    unreachable!("leap instant outside candidate days")
}

pub const FLAG_EXPLICIT: u8 = 0; // T/T
pub const FLAG_START_DURATION: u8 = 1; // T/P
pub const FLAG_DURATION_END: u8 = 2; // P/T

// -- errors -------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum LexicalIntervalError {
    #[error("invalid interval: {0}")]
    Parse(#[from] IntervalParseError),
    #[error("special classes are incommensurable within one interval: `{0}`")]
    Incommensurable(String),
    #[error("invalid component: `{0}`")]
    InvalidComponent(String),
    #[error("character outside the payload alphabet: `{0}`")]
    Unpackable(char),
    #[error("malformed entry bytes")]
    MalformedEntry,
    #[error("finite endpoint `{0}` is not representable at nanos precision")]
    UnrepresentableEnd(String),
    #[error("unknown interval relation: `{0}`")]
    UnknownRelation(String),
}

// -- components ---------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct FiniteComponent {
    pub is_neg: bool,
    /// Signed ceiling integer seconds (`-1.5` stores int=-1, frac="5").
    pub int: Integer,
    /// Canonical fraction digits (trailing zeros stripped); None = integral.
    /// The instant field encodes the *value*, so equal-instant spellings
    /// share the ordering key — semantic comparison falls out of byte order.
    /// For negatives the fraction counts *down* from the ceiling int:
    /// `int - frac/10^len`.
    pub frac: Option<String>,
    /// Supplied fraction digit count (0 = no fraction). The supplied digits
    /// are always `canonical + trailing zeros`, so this byte preserves
    /// precision: `.5`, `.50`, `.500` are the same instant but distinct
    /// literals, and `.00` renders back with its dot.
    pub frac_len: u8,
    pub kind: u8,
    pub tz_class: u8,
    /// Signed offset minutes; only meaningful for TZ_NEG / TZ_POS.
    pub offset_minutes: i16,
    /// Canonicalised component spelling (separators stripped).
    pub canonical: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Component {
    Finite(FiniteComponent),
    Unbounded,
    Unspecified,
    Unknown,
}

impl Component {
    fn class_byte(&self, is_start: bool) -> u8 {
        match (self, is_start) {
            (Component::Finite(_), true) => START_FINITE,
            (Component::Finite(_), false) => END_FINITE,
            (Component::Unbounded, true) => START_UNBOUNDED,
            (Component::Unbounded, false) => END_UNBOUNDED,
            (Component::Unspecified, true) => START_UNSPECIFIED,
            (Component::Unspecified, false) => END_UNSPECIFIED,
            (Component::Unknown, true) => START_UNKNOWN,
            (Component::Unknown, false) => END_UNKNOWN,
        }
    }

    fn is_special(&self) -> Option<u8> {
        match self {
            Component::Finite(_) => None,
            Component::Unbounded => Some(0),
            Component::Unspecified => Some(1),
            Component::Unknown => Some(2),
        }
    }

    fn canonical_str(&self) -> &str {
        match self {
            Component::Finite(f) => &f.canonical,
            Component::Unbounded => "..",
            Component::Unspecified => "X*",
            Component::Unknown => "",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LexicalInterval {
    pub start: Component,
    pub end: Component,
    pub flag: u8,
    /// Canonicalised supplied string, e.g. `20251201T000000Z/P1M`.
    pub canonical: String,
}

// -- canonicalisation ---------------------------------------------------------

/// Strip date/time separators from one component spelling: `-` separators in
/// the date portion (before `T`) and all `:` separators go; the leading sign
/// and the `±` zone sign stay.
fn canonicalise_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut seen_t = false;
    for (i, ch) in s.chars().enumerate() {
        match ch {
            'T' => {
                seen_t = true;
                out.push(ch);
            }
            '-' if i > 0 && !seen_t => continue,
            ':' => continue,
            _ => out.push(ch),
        }
    }
    out
}

/// Re-insert separators into a canonicalised finite component:
/// `YYYYMMDD[T]HHMMSS[.frac][Z|±HHMM]` → `YYYY-MM-DDTHH:MM:SS[.frac][±HH:MM]`.
fn extend_component(s: &str) -> String {
    let (sign, body) = match s.strip_prefix('-') {
        Some(b) => ("-", b),
        None => ("", s),
    };
    let mut out = String::with_capacity(s.len() + 6);
    out.push_str(sign);

    // date portion: all digits before `T`; the last four are MMDD, the rest
    // is the (possibly expanded) year.
    let (datep, after_date) = match body.find('T') {
        Some(p) => (&body[..p], Some(&body[p..])),
        None => (body, None),
    };
    if datep.len() >= 8 && datep.chars().all(|c| c.is_ascii_digit()) {
        let yend = datep.len() - 4;
        out.push_str(&datep[..yend]);
        out.push('-');
        out.push_str(&datep[yend..yend + 2]);
        out.push('-');
        out.push_str(&datep[yend + 2..]);
        if let Some(t) = after_date {
            out.push('T');
            let mut rest = &t[1..];
            // time portion: 6 digits -> HH:MM:SS
            if rest.len() >= 6 && rest[..6].chars().all(|c| c.is_ascii_digit()) {
                out.push_str(&rest[..2]);
                out.push(':');
                out.push_str(&rest[2..4]);
                out.push(':');
                out.push_str(&rest[4..6]);
                rest = &rest[6..];
                // optional fraction
                if let Some(frac) = rest.strip_prefix('.') {
                    let end = frac
                        .find(|c: char| !c.is_ascii_digit())
                        .unwrap_or(frac.len());
                    out.push('.');
                    out.push_str(&frac[..end]);
                    rest = &frac[end..];
                }
            }
            // zone: Z | ±HHMM
            if !rest.is_empty() {
                if rest == "Z" {
                    out.push('Z');
                } else if rest.len() == 5
                    && (rest.starts_with('+') || rest.starts_with('-'))
                    && rest[1..].chars().all(|c| c.is_ascii_digit())
                {
                    out.push_str(&rest[..3]);
                    out.push(':');
                    out.push_str(&rest[3..]);
                } else {
                    out.push_str(rest); // unrecognised tail — verbatim
                }
            }
        }
    } else {
        out.push_str(body); // not a standard date shape — verbatim
    }
    out
}

// -- component parsing --------------------------------------------------------

/// Split a wall-clock time `HH:MM:SS[.frac]` or `HHMMSS[.frac]` into
/// (seconds_of_day, fraction_digits).
fn parse_wall_time(s: &str) -> Result<(u32, Option<String>), LexicalIntervalError> {
    let (hms, frac) = match s.split_once('.') {
        Some((h, f)) => (h, Some(f.to_string())),
        None => (s, None),
    };
    let digits: String = hms.chars().filter(|c| *c != ':').collect();
    if digits.len() != 6 || !digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(LexicalIntervalError::InvalidComponent(s.to_string()));
    }
    // `23:59:60` is the leap-second spelling: the extra second before
    // midnight, i.e. second 86400 of the day. Any other :60 is invalid.
    let (h, m, sec) = (
        digits[..2].parse::<u32>().unwrap(),
        digits[2..4].parse::<u32>().unwrap(),
        digits[4..6].parse::<u32>().unwrap(),
    );
    let secs = if digits == "235960" {
        86400
    } else {
        if h > 23 || m > 59 || sec > 59 {
            return Err(LexicalIntervalError::InvalidComponent(s.to_string()));
        }
        h * 3600 + m * 60 + sec
    };
    if let Some(f) = &frac {
        if f.is_empty() || !f.chars().all(|c| c.is_ascii_digit()) {
            return Err(LexicalIntervalError::InvalidComponent(s.to_string()));
        }
    }
    Ok((secs, frac))
}

/// Split a datetime's time-tail into (wall, tz_class, offset_minutes).
fn parse_zone(s: &str) -> (&str, u8, i16) {
    if let Some(wall) = s.strip_suffix('Z') {
        return (wall, TZ_Z, 0);
    }
    // zone sign: last '+' or '-' that is not the leading date sign — within the
    // time tail the only signs are zone signs.
    let pos = s.rfind(['+', '-']);
    if let Some(i) = pos {
        let zone = &s[i..];
        let digits: String = zone[1..].chars().filter(|c| *c != ':').collect();
        if digits.len() == 4 && digits.chars().all(|c| c.is_ascii_digit()) {
            let off =
                digits[..2].parse::<i16>().unwrap() * 60 + digits[2..].parse::<i16>().unwrap();
            let (cls, val) = match zone.as_bytes()[0] {
                b'+' => (if off == 0 { TZ_POS_ZERO } else { TZ_POS }, off),
                _ => (if off == 0 { TZ_NEG_ZERO } else { TZ_NEG }, -off),
            };
            return (&s[..i], cls, val);
        }
    }
    (s, TZ_ZONELESS, 0)
}

/// (is_neg, signed ceiling int, canonical frac, supplied digit count) from
/// seconds + supplied frac digits. The canonical digits carry the value —
/// trailing zeros are precision and live only in `frac_len`.
/// Negatives use the ceiling convention: value = `int - frac/10^len`, so
/// `secs=-2, frac="5"` (v=-1.5) stores int=-1, frac="5" (= 10-5). An
/// integral negative (`.00`) stores no canonical frac; its dot survives
/// through `frac_len` alone.
fn instant_parts(secs: i64, frac: Option<String>) -> (bool, Integer, Option<String>, u8) {
    let canon = |f: &str| {
        let t = f.trim_end_matches('0');
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    };
    let supplied_len = frac.as_deref().map_or(0, |f| {
        u8::try_from(f.len()).expect("fraction longer than 255 digits")
    });
    if secs >= 0 {
        return (
            false,
            Integer::from(secs),
            frac.as_deref().and_then(canon),
            supplied_len,
        );
    }
    match frac {
        None => (true, Integer::from(secs), None, 0),
        Some(f) => {
            // v = secs + f/10^len < 0 → canonical digits are the
            // 10^len − f complement, trimmed; an integral value keeps int.
            let len = f.len();
            let ten = format!("1{}", "0".repeat(len)).parse::<Integer>().unwrap();
            let comp = (&ten - f.parse::<Integer>().unwrap()) % &ten;
            if comp == 0 {
                return (true, Integer::from(secs), None, supplied_len);
            }
            let digits = format!("{:0>width$}", comp.to_string(), width = len);
            (true, Integer::from(secs + 1), canon(&digits), supplied_len)
        }
    }
}

fn parse_finite(s: &str, is_end: bool) -> Result<FiniteComponent, LexicalIntervalError> {
    let canonical = canonicalise_component(s);
    if let Some(t_pos) = s.find('T') {
        // dateTime component
        let date_part = &s[..t_pos];
        let (wall, tz_class, offset_minutes) = parse_zone(&s[t_pos + 1..]);
        let (sod, frac) = parse_wall_time(wall)?;
        let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
            .map_err(|_| LexicalIntervalError::InvalidComponent(s.to_string()))?;
        // `23:59:60` is the real second before midnight on IERS leap days —
        // anywhere else it is invalid, like Feb 29 in a common year.
        if sod as i64 == 86400 && !is_leap_day(day_key(date)) {
            return Err(LexicalIntervalError::InvalidComponent(s.to_string()));
        }
        let secs = leap_aware_secs(
            date,
            sod as i64,
            if tz_class == TZ_ZONELESS {
                0
            } else {
                offset_minutes
            },
        );
        let (is_neg, int, frac, frac_len) = instant_parts(secs, frac);
        Ok(FiniteComponent {
            is_neg,
            int,
            frac,
            frac_len,
            kind: KIND_DATETIME,
            tz_class,
            offset_minutes: offset_minutes.abs(),
            canonical,
        })
    } else {
        // date component — inclusive whole day, local-time managed
        let mut date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| LexicalIntervalError::InvalidComponent(s.to_string()))?;
        if is_end {
            date = date.succ_opt().unwrap_or(date);
        }
        let secs = leap_aware_secs(date, 0, 0);
        let (is_neg, int, frac, frac_len) = instant_parts(secs, None);
        Ok(FiniteComponent {
            is_neg,
            int,
            frac,
            frac_len,
            kind: KIND_DATE,
            tz_class: TZ_ZONELESS,
            offset_minutes: 0,
            canonical,
        })
    }
}

/// A component instant derived from duration arithmetic (nanos precision).
fn computed_finite(
    secs: i64,
    nanos: u32,
    kind: u8,
    tz_class: u8,
    offset_minutes: i16,
    canonical: String,
) -> FiniteComponent {
    let frac = if nanos == 0 {
        None
    } else {
        Some(format!("{:09}", nanos))
    };
    let (is_neg, int, frac, _) = instant_parts(secs, frac);
    let frac_len = frac.as_deref().map_or(0, |d| d.len() as u8);
    FiniteComponent {
        is_neg,
        int,
        frac,
        frac_len,
        kind,
        tz_class,
        offset_minutes: offset_minutes.abs(),
        canonical,
    }
}

fn parse_component(s: &str, is_end: bool) -> Result<Component, LexicalIntervalError> {
    match UnboundedEndpoint::from_str(s) {
        Some(UnboundedEndpoint::Open) => Ok(Component::Unbounded),
        Some(UnboundedEndpoint::Unknown) => Ok(Component::Unknown),
        None if s == "X*" => Ok(Component::Unspecified),
        None => Ok(Component::Finite(parse_finite(s, is_end)?)),
    }
}

// -- interval parsing ---------------------------------------------------------

/// Parse a supplied interval string into a `LexicalInterval`.
pub fn parse_lexical_interval(s: &str) -> Result<LexicalInterval, LexicalIntervalError> {
    if s.is_empty() {
        return Err(IntervalParseError::Empty.into());
    }
    let slash = s
        .find('/')
        .ok_or_else(|| IntervalParseError::MissingSeparator(s.to_string()))?;
    let (left, right) = (&s[..slash], &s[slash + 1..]);

    let left_dur = left.starts_with('P') || left.starts_with("-P");
    let right_dur = right.starts_with('P') || right.starts_with("-P");
    if left_dur && right_dur {
        return Err(IntervalParseError::TwoDurations(s.to_string()).into());
    }

    let canonical = format!(
        "{}/{}",
        canonicalise_component(left),
        canonicalise_component(right)
    );

    if left_dur || right_dur {
        // duration form — the anchored side must be finite
        let anchored = if left_dur { right } else { left };
        if UnboundedEndpoint::from_str(anchored).is_some() || anchored == "X*" {
            return Err(IntervalParseError::UnboundedWithDuration(s.to_string()).into());
        }
        let duration = parse_iso_duration(if left_dur { left } else { right })?;
        if left_dur {
            let end = parse_finite(anchored, true)?;
            let (s_secs, s_nanos) = secs_nanos(&end);
            let (cs, cn) = duration_shifted(
                &duration,
                s_secs,
                s_nanos,
                tshift_secs(end.tz_class, end.offset_minutes),
                -1,
            );
            let start = Component::Finite(computed_finite(
                cs,
                cn,
                end.kind,
                end.tz_class,
                end.offset_minutes,
                canonicalise_component(left),
            ));
            Ok(LexicalInterval {
                start,
                end: Component::Finite(end),
                flag: FLAG_DURATION_END,
                canonical,
            })
        } else {
            let start = parse_finite(anchored, false)?;
            let (s_secs, s_nanos) = secs_nanos(&start);
            let (ce, cn) = duration_shifted(
                &duration,
                s_secs,
                s_nanos,
                tshift_secs(start.tz_class, start.offset_minutes),
                1,
            );
            let end = Component::Finite(computed_finite(
                ce,
                cn,
                start.kind,
                start.tz_class,
                start.offset_minutes,
                canonicalise_component(right),
            ));
            Ok(LexicalInterval {
                start: Component::Finite(start),
                end,
                flag: FLAG_START_DURATION,
                canonical,
            })
        }
    } else {
        let start = parse_component(left, false)?;
        let end = parse_component(right, true)?;

        // incommensurability: at most one special class per interval
        let mut classes = [start.is_special(), end.is_special()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        classes.sort_unstable();
        classes.dedup();
        if classes.len() > 1 {
            return Err(LexicalIntervalError::Incommensurable(s.to_string()));
        }

        Ok(LexicalInterval {
            start,
            end,
            flag: FLAG_EXPLICIT,
            canonical,
        })
    }
}

/// Recover (secs, nanos) for the duration-arithmetic helpers, which work at
/// nanos precision and chrono's floor convention (secs + nanos, nanos ≥ 0).
fn secs_nanos(f: &FiniteComponent) -> (i64, u32) {
    // out-of-i64 seconds are unreachable through chrono-bounded parsing; if
    // expanded years ever land they must widen this path, not clamp.
    let int = f.int.to_i64().expect("finite seconds outside i64");
    let nanos9 = |d: &str| {
        let mut s = d[..d.len().min(9)].to_string();
        while s.len() < 9 {
            s.push('0');
        }
        s.parse::<u32>().unwrap_or(0)
    };
    match &f.frac {
        None => (int, 0),
        // value = int + 0.frac → floor secs = int
        Some(d) if !f.is_neg => (int, nanos9(d)),
        // value = int - 0.frac → floor secs = int - 1, subsec = 1 - 0.frac
        // (the 10^len - frac complement, truncated to nanos).
        Some(d) => {
            let len = d.len();
            let ten = format!("1{}", "0".repeat(len)).parse::<Integer>().unwrap();
            let comp = &ten - d.parse::<Integer>().unwrap();
            let padded = format!("{:0>width$}", comp.to_string(), width = len);
            (int - 1, nanos9(&padded))
        }
    }
}

// -- entry encode/decode ------------------------------------------------------

fn tz_bytes(f: &FiniteComponent, out: &mut BytesMut) {
    out.put_u8(f.tz_class);
    if f.tz_class == TZ_NEG || f.tz_class == TZ_POS {
        let minutes = if f.tz_class == TZ_NEG {
            -f.offset_minutes
        } else {
            f.offset_minutes
        };
        out.put_i16(minutes);
    }
}

/// Class byte + instant key — absent bytes for non-finite classes keep the
/// key self-delimiting (the class byte fixes each component's arity).
fn component_instant_key(c: &Component, is_start: bool, out: &mut BytesMut) {
    out.put_u8(c.class_byte(is_start));
    if let Component::Finite(f) = c {
        out.extend(integer_and_fraction_to_storage(
            f.is_neg,
            f.int.clone(),
            f.frac.as_deref(),
        ));
    }
}

/// kind + timeshift + supplied frac length — finite components only.
/// `frac_len` is a spelling field: it preserves precision without
/// touching the semantic instant ordering.
fn component_meta_key(c: &Component, out: &mut BytesMut) {
    if let Component::Finite(f) = c {
        out.put_u8(f.kind);
        tz_bytes(f, out);
        out.put_u8(f.frac_len);
    }
}

/// (floor secs, non-negative fraction digits) of a finite component's value
/// in a frame shifted by `shift` seconds. Negative-value fracs count down
/// from the ceiling int, so the sub-second digits are the `10^len - frac`
/// complement.
fn instant_floor(f: &FiniteComponent, shift: i64) -> (i64, Option<String>) {
    let int = f.int.to_i64().expect("finite seconds outside i64");
    match &f.frac {
        None => (int + shift, None),
        Some(d) if !f.is_neg => (int + shift, Some(d.clone())),
        Some(d) => {
            let len = d.len();
            let ten = format!("1{}", "0".repeat(len)).parse::<Integer>().unwrap();
            let comp = format!(
                "{:0>width$}",
                (&ten - d.parse::<Integer>().unwrap()).to_string(),
                width = len
            );
            (int - 1 + shift, Some(comp))
        }
    }
}

/// Signed shift seconds of a timeshift class.
fn tshift_secs(tz_class: u8, offset_minutes: i16) -> i64 {
    match tz_class {
        TZ_NEG => -(offset_minutes as i64) * 60,
        TZ_POS => offset_minutes as i64 * 60,
        _ => 0,
    }
}

/// Shift a leap-aware instant by a duration, in the anchor's local frame.
/// `Y`/`M`/`D`/`H`/`Min` are wall-clock units — a wall hour spanning a leap
/// second is 3601 real seconds — while `S` counts real elapsed seconds
/// including leap seconds. `direction` is +1 for end-from-start, -1 for
/// start-from-end (the duration's own sign applies on top).
fn duration_shifted(
    dur: &Duration,
    secs: i64,
    nanos: u32,
    tshift: i64,
    direction: i64,
) -> (i64, u32) {
    let sign = if dur.sign >= 0 { direction } else { -direction };
    // wall domain of the anchor, in its own frame. `leap60` marks the
    // anchor's position one real second inside minute 23:59 — carried as
    // +1 s into the re-encoded instant so an anchor on `23:59:60` is not
    // silently shifted back to `:59`.
    let (mut dt, leap60) = leap_instant_to_naive(secs + tshift);
    if dur.year != 0 || dur.month != 0 {
        dt = shift_months(dt, sign * (dur.year * 12 + dur.month as i64));
    }
    dt += chrono::Duration::seconds(
        sign * (dur.day as i64 * 86400 + dur.hour as i64 * 3600 + dur.minute as i64 * 60),
    );
    // back to the leap-aware instant domain, frame removed; `leap60` adds
    // the anchor's real +1 s position inside minute 23:59
    let base = leap_aware_secs(dt.date(), dt.time().num_seconds_from_midnight() as i64, 0)
        + leap60 as i64
        - tshift;
    let mut out_secs = base + sign * dur.second.trunc() as i64;
    // fractional seconds carry as real nanos with a proper borrow
    let mut n = nanos as i64 + sign * (dur.second.fract().abs() * 1e9).round() as i64;
    if n < 0 {
        n += 1_000_000_000;
        out_secs -= 1;
    } else if n >= 1_000_000_000 {
        n -= 1_000_000_000;
        out_secs += 1;
    }
    (out_secs, n as u32)
}

/// Add `months` calendar months, sign-split onto chrono's checked month
/// arithmetic (clamps to the target month's last day: Jan 31 + 1mo → Feb 28).
fn shift_months(dt: chrono::NaiveDateTime, months: i64) -> chrono::NaiveDateTime {
    let r = if months >= 0 {
        dt.checked_add_months(chrono::Months::new(months as u32))
    } else {
        dt.checked_sub_months(chrono::Months::new(months.unsigned_abs() as u32))
    };
    r.expect("month shift in range")
}

/// Decimal subtraction of aligned fraction digit strings: returns
/// (borrow, digits) — borrow 1 when `a < b`.
fn sub_frac(a: Option<&str>, b: Option<&str>) -> (i64, Option<String>) {
    let len = a.map_or(0, |s| s.len()).max(b.map_or(0, |s| s.len()));
    if len == 0 {
        return (0, None);
    }
    let pad = |s: Option<&str>| format!("{:0<w$}", s.unwrap_or(""), w = len);
    let ai: Integer = pad(a).parse().unwrap();
    let bi: Integer = pad(b).parse().unwrap();
    let (borrow, mut digits) = if ai >= bi {
        (0, (ai - bi).to_string())
    } else {
        let ten: Integer = format!("1{}", "0".repeat(len)).parse().unwrap();
        (1, (ten + ai - bi).to_string())
    };
    digits = format!("{:0>width$}", digits, width = len);
    let t = digits.trim_end_matches('0');
    (
        borrow,
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        },
    )
}

/// The canonical derived period of a `T/T` interval: greedy largest-units
/// decomposition `Y M D H Min S` anchored at the start's wall-clock frame
/// (its own timeshift — a three-month local span derives `P3M` whatever the
/// zone spelling). `W` is never emitted; a zero span is `PT0S`.
fn derived_period(iv: &LexicalInterval) -> String {
    let (Component::Finite(s), Component::Finite(e)) = (&iv.start, &iv.end) else {
        return String::new();
    };
    let shift = tshift_secs(s.tz_class, s.offset_minutes);
    let (ss, sfrac) = instant_floor(s, shift);
    let (es, efrac) = instant_floor(e, shift);
    // compare (secs, frac) numerically — pad fracs to a common denominator
    let len = sfrac
        .as_deref()
        .map_or(0, str::len)
        .max(efrac.as_deref().map_or(0, str::len));
    let to_num = |f: &Option<String>| {
        if len == 0 {
            return Integer::from(0);
        }
        format!("{:0<w$}", f.as_deref().unwrap_or(""), w = len)
            .parse::<Integer>()
            .unwrap()
    };
    let (neg, ss, es, sfrac, efrac) = if (es, to_num(&efrac)) < (ss, to_num(&sfrac)) {
        (true, es, ss, efrac, sfrac)
    } else {
        (false, ss, es, sfrac, efrac)
    };
    // borrow one second into the fraction when it goes negative, then the
    // second-grain datetimes carry the whole-second remainder
    let (borrow, rem_frac) = sub_frac(efrac.as_deref(), sfrac.as_deref());
    let es = es - borrow;
    // `leap60` marks the endpoint one real second inside minute 23:59 — it
    // belongs to the last day of its month, so `Y`/`M` decompose on the
    // naive walls unaffected.
    let (sdt, s60) = leap_instant_to_naive(ss);
    let (edt, e60) = leap_instant_to_naive(es);
    let (s60, e60) = (i64::from(s60), i64::from(e60));

    let mut y = edt.year() as i64 - sdt.year() as i64;
    let mut anchor = shift_months(sdt, y * 12);
    if anchor > edt {
        y -= 1;
        anchor = shift_months(sdt, y * 12);
    }
    let mut mo = (edt.year() as i64 - anchor.date().year() as i64) * 12
        + (edt.month() as i64 - anchor.date().month() as i64);
    let mut anchor2 = shift_months(anchor, mo);
    if anchor2 > edt {
        mo -= 1;
        anchor2 = shift_months(anchor, mo);
    }
    // wall units decompose on the naive walls — a wall hour that spans a
    // leap second still derives `PT1H`; the `sec` field then counts real
    // elapsed seconds (leap seconds included) after the wall anchor
    // `D`/`H`/`Min` decompose on flagged position counts — the leap second
    // is a 61st second inside minute 23:59, so `rem` is the naive span
    // corrected by the flags; the anchor carries the start flag as a +1 s
    // real position. `S` then counts real elapsed seconds after the wall
    // anchor (leap seconds included).
    let rem = (edt - anchor2).num_seconds() + e60 - s60;
    let mut d = rem / 86400;
    let mut h = rem % 86400 / 3600;
    let mut mi = rem % 3600 / 60;
    let real_at = |dt: chrono::NaiveDateTime| {
        leap_aware_secs(dt.date(), dt.time().num_seconds_from_midnight() as i64, 0) + s60
    };
    let mut anchor5 = anchor2
        + chrono::Duration::days(d)
        + chrono::Duration::hours(h)
        + chrono::Duration::minutes(mi);
    // an end *on* the leap second can sit just under a picked wall unit —
    // back off the smallest unit until the anchor precedes the real end
    while real_at(anchor5) > es {
        if mi > 0 {
            mi -= 1;
        } else if h > 0 {
            h -= 1;
            mi = 59;
        } else if d > 0 {
            d -= 1;
            h = 23;
            mi = 59;
        } else {
            break; // unreachable: the bare start position never passes the end
        }
        anchor5 = anchor2
            + chrono::Duration::days(d)
            + chrono::Duration::hours(h)
            + chrono::Duration::minutes(mi);
    }
    let sec = es - real_at(anchor5);

    let mut p = String::from("P");
    if y != 0 {
        p += &format!("{y}Y");
    }
    if mo != 0 {
        p += &format!("{mo}M");
    }
    if d != 0 {
        p += &format!("{d}D");
    }
    if h != 0 || mi != 0 || sec != 0 || rem_frac.is_some() {
        p += "T";
        if h != 0 {
            p += &format!("{h}H");
        }
        if mi != 0 {
            p += &format!("{mi}M");
        }
        if sec != 0 || rem_frac.is_some() {
            p += &format!("{sec}");
            if let Some(f) = rem_frac {
                p += &format!(".{f}");
            }
            p += "S";
        }
    }
    if p == "P" {
        p = "PT0S".to_string();
    }
    if neg {
        format!("-{p}")
    } else {
        p
    }
}

/// The `period` sort field: the supplied canonical duration for `T/P`/`P/T`
/// forms (`P90D` ≠ `P3M` even at equal span — anchor-relative units are not
/// interchangeable), the derived canonical period for `T/T`, empty when no
/// finite pair exists (all-special intervals).
fn period_key(iv: &LexicalInterval) -> String {
    if iv.flag == FLAG_EXPLICIT {
        return derived_period(iv);
    }
    let (l, r) = iv.canonical.split_once('/').unwrap_or((&iv.canonical, ""));
    (if iv.flag == FLAG_DURATION_END { l } else { r }).to_string()
}

/// Rebuild a finite component's canonical spelling from its key fields —
/// the entry stores no payload, so rendering derives everything: wall clock
/// from `instant ± offset`, fraction digits from the stored (complemented
/// for negatives) digits, zone marker from the tshift class.
fn reconstruct_component_canonical(f: &FiniteComponent, is_end: bool) -> String {
    // floor seconds: ceiling-frac negatives step one more down; a stored
    // all-zero frac is integral and needs no step
    let mut secs = f
        .int
        .to_i64()
        .expect("finite seconds outside i64 — unreachable via parse");
    if f.is_neg && f.frac.is_some() {
        secs -= 1;
    }
    match f.tz_class {
        TZ_NEG => secs -= f.offset_minutes as i64 * 60,
        TZ_POS => secs += f.offset_minutes as i64 * 60,
        _ => {}
    }
    let (mut naive, leap60) = leap_instant_to_naive(secs);
    // date-kind ends store the exclusive day bound; the supplied spelling is
    // the inclusive day before — subtract a calendar day on the naive wall,
    // not 86400 domain seconds (a leap-day span is 86401).
    if f.kind == KIND_DATE && is_end {
        naive -= chrono::Duration::days(1);
    }
    let y = naive.year();
    let mut out = format!(
        "{}{:04}{:02}{:02}",
        if y < 0 { "-" } else { "" },
        y.abs(),
        naive.month(),
        naive.day()
    );
    if f.kind != KIND_DATE {
        if leap60 {
            out.push_str("T235960");
        } else {
            out.push_str(&format!(
                "T{:02}{:02}{:02}",
                naive.hour(),
                naive.minute(),
                naive.second()
            ));
        }
        if f.frac_len > 0 {
            let supplied = match &f.frac {
                Some(d) => {
                    let canonical = if f.is_neg {
                        // stored digits are the 10^len − supplied complement
                        let ten: Integer = format!("1{}", "0".repeat(d.len())).parse().unwrap();
                        format!(
                            "{:0>w$}",
                            (&ten - d.parse::<Integer>().unwrap()).to_string(),
                            w = d.len()
                        )
                    } else {
                        d.clone()
                    };
                    canonical
                }
                // integral value with a supplied fraction: all zeros
                None => String::new(),
            };
            out.push('.');
            out.push_str(&supplied);
            // canonical + trailing zeros = the supplied digit count
            for _ in (supplied.len())..f.frac_len as usize {
                out.push('0');
            }
        }
        match f.tz_class {
            TZ_NEG | TZ_POS => out.push_str(&format!(
                "{}{:02}{:02}",
                if f.tz_class == TZ_NEG { "-" } else { "+" },
                f.offset_minutes / 60,
                f.offset_minutes % 60
            )),
            TZ_NEG_ZERO => out.push_str("-0000"),
            TZ_Z => out.push('Z'),
            TZ_POS_ZERO => out.push_str("+0000"),
            _ => {}
        }
    }
    out
}

/// The component side of the canonical: finite reconstructs, specials are
/// their fixed spellings.
fn component_canonical(c: &Component, is_end: bool) -> String {
    match c {
        Component::Finite(f) => reconstruct_component_canonical(f, is_end),
        Component::Unbounded => "..".to_string(),
        Component::Unknown => String::new(),
        Component::Unspecified => "X*".to_string(),
    }
}

/// Encode an interval to its dictionary entry bytes.
pub fn encode_lexical_interval(iv: &LexicalInterval) -> Vec<u8> {
    let mut out = BytesMut::new();
    component_instant_key(&iv.start, true, &mut out);
    component_instant_key(&iv.end, false, &mut out);
    // period: bytes + 0x00 terminator — plain string order, self-delimiting
    let period = period_key(iv);
    out.extend(period.as_bytes());
    out.put_u8(0);
    component_meta_key(&iv.start, &mut out);
    component_meta_key(&iv.end, &mut out);
    out.put_u8(iv.flag);
    // no payload: the canonical spelling is derivable from the key fields —
    // decode reconstructs it via reconstruct_component_canonical.
    out.to_vec()
}

/// Instant part only (int + fraction). kind/timeshift live later in the
/// entry — `decode_meta` fills them for finite components.
fn decode_finite(bytes: &mut &[u8]) -> Result<FiniteComponent, LexicalIntervalError> {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let frac_str = decode_fraction(bytes, is_pos);
    // decode_fraction only peeks the "false zero" terminator for empty
    // fractions; consume it so following fields stay aligned.
    if frac_str.is_empty() {
        if bytes.is_empty() {
            return Err(LexicalIntervalError::MalformedEntry);
        }
        bytes.get_u8();
    }
    Ok(FiniteComponent {
        is_neg: !is_pos,
        int,
        frac: if frac_str.is_empty() {
            None
        } else {
            Some(frac_str)
        },
        frac_len: 0,
        kind: 0,
        tz_class: 0,
        offset_minutes: 0,
        canonical: String::new(), // filled from payload at entry level
    })
}

/// kind + timeshift + supplied frac length for a finite component — the
/// meta block's position in the entry layout.
fn decode_meta(f: &mut FiniteComponent, bytes: &mut &[u8]) -> Result<(), LexicalIntervalError> {
    if bytes.len() < 2 {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    f.kind = bytes.get_u8();
    f.tz_class = bytes.get_u8();
    f.offset_minutes = if f.tz_class == TZ_NEG || f.tz_class == TZ_POS {
        if bytes.len() < 2 {
            return Err(LexicalIntervalError::MalformedEntry);
        }
        bytes
            .get_i16()
            .checked_abs()
            .ok_or(LexicalIntervalError::MalformedEntry)?
    } else {
        0
    };
    if bytes.is_empty() {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    f.frac_len = bytes.get_u8();
    // supplied digits are canonical + zeros: len 0 means no canonical frac,
    // and a shorter supplied length is impossible
    let canon_len = f.frac.as_deref().map_or(0, |d| d.len()) as usize;
    if (f.frac_len == 0 && f.frac.is_some()) || (f.frac_len as usize) < canon_len {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    Ok(())
}

fn decode_component(bytes: &mut &[u8], is_start: bool) -> Result<Component, LexicalIntervalError> {
    if bytes.is_empty() {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    let class = bytes.get_u8();
    let finite_class = if is_start { START_FINITE } else { END_FINITE };
    if class == finite_class {
        return Ok(Component::Finite(decode_finite(bytes)?));
    }
    match (is_start, class) {
        (true, START_UNBOUNDED) | (false, END_UNBOUNDED) => Ok(Component::Unbounded),
        (true, START_UNSPECIFIED) | (false, END_UNSPECIFIED) => Ok(Component::Unspecified),
        (true, START_UNKNOWN) | (false, END_UNKNOWN) => Ok(Component::Unknown),
        _ => Err(LexicalIntervalError::MalformedEntry),
    }
}

/// Decode a dictionary entry back to a `LexicalInterval`.
pub fn decode_lexical_entry(bytes: &[u8]) -> Result<LexicalInterval, LexicalIntervalError> {
    let mut cursor = bytes;
    let mut start = decode_component(&mut cursor, true)?;
    let mut end = decode_component(&mut cursor, false)?;
    // period: canonical duration bytes + 0x00 terminator — for duration-flag
    // forms this IS the supplied component spelling
    let plen = cursor
        .iter()
        .position(|&b| b == 0)
        .ok_or(LexicalIntervalError::MalformedEntry)?;
    let period = String::from_utf8_lossy(&cursor[..plen]).into_owned();
    cursor = &cursor[plen + 1..];
    if let Component::Finite(f) = &mut start {
        decode_meta(f, &mut cursor)?;
    }
    if let Component::Finite(f) = &mut end {
        decode_meta(f, &mut cursor)?;
    }
    if cursor.is_empty() {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    let flag = cursor.get_u8();
    // flag is the last field — a well-formed entry ends exactly here
    if !cursor.is_empty() {
        return Err(LexicalIntervalError::MalformedEntry);
    }
    // the canonical spelling is reconstructed, not stored: finite sides
    // rebuild their wall clock, a duration side is the period field itself
    let period_side = |is_left: bool| {
        (if is_left {
            flag == FLAG_DURATION_END
        } else {
            flag == FLAG_START_DURATION
        })
        .then(|| period.clone())
    };
    let (cs, ce) = (
        component_canonical(&start, false),
        component_canonical(&end, true),
    );
    let canonical = format!(
        "{}/{}",
        period_side(true).unwrap_or(cs.clone()),
        period_side(false).unwrap_or(ce.clone())
    );
    for (c, s) in [(&mut start, cs), (&mut end, ce)] {
        if let Component::Finite(f) = c {
            f.canonical = s;
        }
    }
    Ok(LexicalInterval {
        start,
        end,
        flag,
        canonical,
    })
}

/// Render an interval back to extended ISO 8601 form, from the stored
/// canonical payload (finite components get their separators restored;
/// everything else is verbatim).
pub fn render_lexical_interval(iv: &LexicalInterval) -> String {
    let (l, r) = iv.canonical.split_once('/').unwrap_or((&iv.canonical, ""));
    let render_side = |side: &str, c: &Component| match c {
        Component::Finite(_) => extend_component(side),
        _ => c.canonical_str().to_string(),
    };
    format!("{}/{}", render_side(l, &iv.start), render_side(r, &iv.end))
}

// -- dictionary probes --------------------------------------------------------

use std::ops::Range;

use bytes::Bytes;

use super::block::IdLookupResult;
use super::datatypes::Datatype;
use super::typed::TypedDict;

/// Id ranges answering a start-boundary probe.
///
/// `special` covers the unbounded + unspecified start block and participates
/// only in `le`/`lt` results (those classes act like the open start bound);
/// it is always empty for `ge`/`gt`/`eq`. Unknown starts satisfy no instant
/// comparison and never appear in either range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartIdRanges {
    pub special: Range<u64>,
    pub finite: Range<u64>,
}

/// Craft a probe key for a start boundary at `(secs, nanos)` in the lexical
/// entry layout. `secs` is a posix epoch reading — the boundary instant is
/// mapped into the entry domain's leap-aware scale internally. With
/// `max_tail` the probe sorts after every real entry sharing that start
/// instant (kind is at most 1 < 0xFF); without, the bare `[class][instant]`
/// prefix sorts before them.
fn lexical_probe_key(secs: i64, nanos: u32, max_tail: bool) -> Bytes {
    // nanos ≥ 1e9 would canonicalise to a *smaller* instant's fraction — the
    // caller gets a silently wrong boundary, so this is a hard contract.
    assert!(nanos < 1_000_000_000, "nanos must be < 1_000_000_000");
    let secs = posix_to_leap_aware_secs(secs);
    let frac = (nanos != 0).then(|| format!("{nanos:09}"));
    let (is_neg, int, frac, _) = instant_parts(secs, frac);
    let mut buf = BytesMut::new();
    buf.put_u8(START_FINITE);
    buf.extend(integer_and_fraction_to_storage(
        is_neg,
        int,
        frac.as_deref(),
    ));
    if max_tail {
        buf.put_u8(0xFF);
    }
    buf.freeze()
}

impl TypedDict {
    /// Global id range `[offset+1, offset+1+n)` of the DateTimeInterval
    /// segment. Same segment as the legacy-format probes — the caller must
    /// know the segment holds lexical-format entries (per-layer format).
    pub fn lexical_interval_segment(&self) -> Option<Range<u64>> {
        self.type_segment(Datatype::DateTimeInterval)
            .map(|(d, off)| (off + 1)..(off + 1 + d.num_entries() as u64))
    }

    /// Binary-search a crafted probe key within the interval segment.
    /// Returns `(k, found)` where `k` is the global 1-based id of the
    /// greatest entry less than the probe.
    fn lexical_probe(&self, probe: &[u8]) -> Option<(u64, bool)> {
        match self.id_slice(Datatype::DateTimeInterval, probe) {
            IdLookupResult::Found(k) => Some((k, true)),
            IdLookupResult::Closest(k) => Some((k, false)),
            IdLookupResult::NotFound => None,
        }
    }

    /// Global id of the first entry whose start class is `class` or greater:
    /// a bare class byte sorts strictly between the classes it separates.
    /// `NotFound` means the probe is below every entry — the boundary is the
    /// segment start.
    fn lexical_class_bound(&self, seg: &Range<u64>, class: u8) -> u64 {
        match self.lexical_probe(&[class]) {
            Some((k, found)) => (if found { k } else { k + 1 }).clamp(seg.start, seg.end),
            None => seg.start,
        }
    }

    /// Ids of entries with `start <= (secs, nanos)`: the unbounded and
    /// unspecified start blocks (which act like the open bound) plus finite
    /// starts at or below the boundary. Unknown starts are excluded.
    pub fn lexical_ids_start_le(&self, secs: i64, nanos: u32) -> StartIdRanges {
        let Some(seg) = self.lexical_interval_segment() else {
            return StartIdRanges {
                special: 0..0,
                finite: 0..0,
            };
        };
        let special = seg.start..self.lexical_class_bound(&seg, START_UNKNOWN);
        let first_finite = self.lexical_class_bound(&seg, START_FINITE);
        let hi = match self.lexical_probe(&lexical_probe_key(secs, nanos, true)) {
            Some((k, _)) => (k + 1).clamp(first_finite, seg.end),
            None => first_finite,
        };
        StartIdRanges {
            special,
            finite: first_finite..hi,
        }
    }

    /// Ids of entries with `start < (secs, nanos)`. Same special-block
    /// inclusion as `start_le`.
    pub fn lexical_ids_start_lt(&self, secs: i64, nanos: u32) -> StartIdRanges {
        let Some(seg) = self.lexical_interval_segment() else {
            return StartIdRanges {
                special: 0..0,
                finite: 0..0,
            };
        };
        let special = seg.start..self.lexical_class_bound(&seg, START_UNKNOWN);
        let first_finite = self.lexical_class_bound(&seg, START_FINITE);
        let hi = match self.lexical_probe(&lexical_probe_key(secs, nanos, false)) {
            Some((k, _)) => (k + 1).clamp(first_finite, seg.end),
            None => first_finite,
        };
        StartIdRanges {
            special,
            finite: first_finite..hi,
        }
    }

    /// Ids of entries with `start > (secs, nanos)`. Finite starts only —
    /// special classes satisfy no `>` comparison. `special` is always empty.
    pub fn lexical_ids_start_gt(&self, secs: i64, nanos: u32) -> StartIdRanges {
        let Some(seg) = self.lexical_interval_segment() else {
            return StartIdRanges {
                special: 0..0,
                finite: 0..0,
            };
        };
        let lo = match self.lexical_probe(&lexical_probe_key(secs, nanos, true)) {
            Some((k, _)) => (k + 1).clamp(seg.start, seg.end),
            None => seg.start,
        };
        StartIdRanges {
            special: 0..0,
            finite: lo..seg.end,
        }
    }

    /// Ids of entries with `start >= (secs, nanos)`. Finite starts only.
    pub fn lexical_ids_start_ge(&self, secs: i64, nanos: u32) -> StartIdRanges {
        let Some(seg) = self.lexical_interval_segment() else {
            return StartIdRanges {
                special: 0..0,
                finite: 0..0,
            };
        };
        let lo = match self.lexical_probe(&lexical_probe_key(secs, nanos, false)) {
            Some((k, _)) => (k + 1).clamp(seg.start, seg.end),
            None => seg.start,
        };
        StartIdRanges {
            special: 0..0,
            finite: lo..seg.end,
        }
    }

    /// Ids of entries with `start == (secs, nanos)`. Finite starts only —
    /// special starts never equal an instant.
    pub fn lexical_ids_start_eq(&self, secs: i64, nanos: u32) -> StartIdRanges {
        let Some(seg) = self.lexical_interval_segment() else {
            return StartIdRanges {
                special: 0..0,
                finite: 0..0,
            };
        };
        let lo = match self.lexical_probe(&lexical_probe_key(secs, nanos, false)) {
            Some((k, _)) => (k + 1).clamp(seg.start, seg.end),
            // probe below every entry: the equal block, if any, starts the segment
            None => seg.start,
        };
        let hi = match self.lexical_probe(&lexical_probe_key(secs, nanos, true)) {
            Some((k, _)) => (k + 1).clamp(seg.start, seg.end),
            None => lo,
        };
        StartIdRanges {
            special: 0..0,
            finite: lo..hi.max(lo),
        }
    }
}

// -- end index ----------------------------------------------------------------

use super::datatypes::{INTERVAL_POS_INF, INTERVAL_UNKNOWN_END};
use super::interval::{end_rank_width, IntervalEndIndex};
use crate::logarray::{LogArray, LogArrayBufBuilder};
use crate::wavelettree::wavelet_tree_from_iter;

/// `(secs, nanos)` rank key for an interval end, or `Err` when a finite end
/// is not representable at nanos precision (out-of-i64 seconds or a real
/// sub-nanosecond fraction).
///
/// Non-finite ends map onto the same sentinel keys the legacy index used:
/// unbounded and unspecified ends rank at `(INTERVAL_POS_INF, 0)` — both act
/// as the open bound, so `end_ge`/`end_gt` include them — and unknown ends
/// rank at `(INTERVAL_UNKNOWN_END, 0)`, parked below everything so no
/// instant comparison reaches them.
fn end_rank_key(c: &Component) -> Result<(i64, u32), LexicalIntervalError> {
    match c {
        Component::Unbounded | Component::Unspecified => Ok((INTERVAL_POS_INF, 0)),
        Component::Unknown => Ok((INTERVAL_UNKNOWN_END, 0)),
        Component::Finite(f) => {
            // canonical fractions have no trailing zeros, so more than nine
            // digits always means real sub-nanosecond precision
            if f.frac.as_deref().map_or(0, |d| d.len()) > 9 {
                return Err(LexicalIntervalError::UnrepresentableEnd(
                    f.canonical.clone(),
                ));
            }
            let int = f
                .int
                .to_i64()
                .ok_or_else(|| LexicalIntervalError::UnrepresentableEnd(f.canonical.clone()))?;
            // secs_nanos subtracts 1 from the ceiling int on fractional
            // negatives — reject i64::MIN up front instead of overflowing.
            // i64::MIN is also the unknown-end sentinel, so it can never be
            // a legal finite rank.
            if int == i64::MIN {
                return Err(LexicalIntervalError::UnrepresentableEnd(
                    f.canonical.clone(),
                ));
            }
            let (s, n) = secs_nanos(f);
            if s == i64::MIN {
                return Err(LexicalIntervalError::UnrepresentableEnd(
                    f.canonical.clone(),
                ));
            }
            Ok((s, n))
        }
    }
}

impl IntervalEndIndex {
    /// Build the end index in memory over a dictionary's DateTimeInterval
    /// segment holding lexical-format entries.
    ///
    /// Fails on any finite end not representable at nanos precision: the
    /// rank table is a fixed `(i64, u32)` domain and clamping would silently
    /// misorder. Layers containing such ends must use the fallback scan.
    pub fn build_lexical(dict: &TypedDict) -> Result<Self, LexicalIntervalError> {
        let mut ends = Vec::new();
        for id in dict.lexical_interval_segment().unwrap_or(0..0) {
            let entry = dict
                .entry(id as usize)
                .ok_or(LexicalIntervalError::MalformedEntry)?;
            let iv = decode_lexical_entry(&entry.to_bytes())?;
            ends.push(end_rank_key(&iv.end)?);
        }

        let mut distinct = ends.clone();
        distinct.sort_unstable();
        distinct.dedup();

        let mut ends_builder = LogArrayBufBuilder::new(BytesMut::new(), 64);
        for &(s, n) in &distinct {
            ends_builder.push(s as u64);
            ends_builder.push(n as u64);
        }
        let ends_table = LogArray::parse(ends_builder.finalize().freeze())
            .map_err(|_| LexicalIntervalError::MalformedEntry)?;

        let ranks = ends
            .iter()
            .map(|e| distinct.binary_search(e).unwrap() as u64);
        let tree = wavelet_tree_from_iter(end_rank_width(distinct.len()), ranks);
        Ok(IntervalEndIndex::from_parts(
            ends_table,
            tree.into_parts().1,
        ))
    }
}

// -- legacy fixed-width conversion ---------------------------------------------
//
// Layers written before the lexical format hold fixed-width entries: the
// shipped 48-byte width (UTC-normalised, no zone designator, finite
// endpoints only) and the branch-internal 52-byte width (zone designator +
// open/unknown sentinels, never shipped). Layer-open upgrades them to
// lexical entries in memory so every downstream reader sees one format.

use super::block::SizedDictEntry;
use super::datatypes::{
    DateTimeInterval, FromLexical, INTERVAL_COMPONENT_DATE, INTERVAL_COMPONENT_OPEN,
    INTERVAL_COMPONENT_UNKNOWN, INTERVAL_ENCODED_WIDTH, INTERVAL_FLAG_DURATION_END,
    INTERVAL_FLAG_START_DURATION, INTERVAL_LEGACY_ENCODED_WIDTH, INTERVAL_NO_ZONE,
};
use super::typed::{TypedDictBufBuilder, TypedDictEntry};
use crate::logarray::LogArrayFileBuilder;
use crate::storage::types::{FileLoad, FileStore};
use crate::wavelettree::build_wavelet_tree_from_iter;
use std::io;

/// Canonical `P…` rendering of a fixed-width `Duration`, in the shape
/// `derived_period` emits: zero is `PT0S`, negatives take a leading `-`.
fn duration_to_period(d: &Duration) -> String {
    let mut p = String::from("P");
    if d.year != 0 {
        p += &format!("{}Y", d.year);
    }
    if d.month != 0 {
        p += &format!("{}M", d.month);
    }
    if d.day != 0 {
        p += &format!("{}D", d.day);
    }
    if d.hour != 0 || d.minute != 0 || d.second != 0.0 {
        p += "T";
        if d.hour != 0 {
            p += &format!("{}H", d.hour);
        }
        if d.minute != 0 {
            p += &format!("{}M", d.minute);
        }
        if d.second != 0.0 {
            p += &format!("{}S", d.second);
        }
    }
    if p == "P" {
        p = "PT0S".to_string();
    }
    if d.sign < 0 {
        format!("-{p}")
    } else {
        p
    }
}

/// One fixed-width endpoint as a lexical `Component`. The stored seconds are
/// POSIX, so the leap shift joins here; nanoseconds become canonical fraction
/// digits; the zone designator maps onto the tshift axis (no-zone →
/// zone-less, `±0` is unreachable: the fixed format cannot spell `-00:00`).
fn legacy_component(
    seconds: i64,
    nanos: u32,
    component_type: u8,
    tz_minutes: i16,
    is_end: bool,
) -> Component {
    match component_type {
        INTERVAL_COMPONENT_OPEN => return Component::Unbounded,
        INTERVAL_COMPONENT_UNKNOWN => return Component::Unknown,
        _ => {}
    }
    let s = posix_to_leap_aware_secs(seconds);
    let (int, frac, is_neg) = if nanos == 0 {
        (Integer::from(s), None, s < 0)
    } else if s < 0 {
        // floor (s, nanos) → ceiling int and complemented fraction
        (
            Integer::from(s + 1),
            Some(
                format!("{:09}", 1_000_000_000u64 - nanos as u64)
                    .trim_end_matches('0')
                    .to_string(),
            ),
            true,
        )
    } else {
        (
            Integer::from(s),
            Some(format!("{nanos:09}").trim_end_matches('0').to_string()),
            false,
        )
    };
    let (tz_class, offset_minutes) = if tz_minutes == INTERVAL_NO_ZONE {
        (TZ_ZONELESS, 0)
    } else if tz_minutes < 0 {
        (TZ_NEG, tz_minutes)
    } else if tz_minutes > 0 {
        (TZ_POS, tz_minutes)
    } else {
        (TZ_Z, 0)
    };
    let frac_len = frac.as_deref().map_or(0, |d| d.len() as u8);
    let mut f = FiniteComponent {
        is_neg,
        int,
        frac,
        frac_len,
        kind: if component_type == INTERVAL_COMPONENT_DATE {
            KIND_DATE
        } else {
            KIND_DATETIME
        },
        tz_class,
        offset_minutes,
        canonical: String::new(),
    };
    f.canonical = reconstruct_component_canonical(&f, is_end);
    Component::Finite(f)
}

/// A fixed-width `DateTimeInterval` as a `LexicalInterval`. Components, flag
/// and zone designators carry over; the duration renders canonical.
pub fn legacy_interval_to_lexical(iv: &DateTimeInterval) -> LexicalInterval {
    let start = legacy_component(
        iv.start_seconds,
        iv.start_nanos,
        iv.start_type,
        iv.start_tz,
        false,
    );
    let end = legacy_component(iv.end_seconds, iv.end_nanos, iv.end_type, iv.end_tz, true);
    let period = duration_to_period(&iv.duration);
    let canonical = match iv.flag {
        INTERVAL_FLAG_DURATION_END => {
            format!("{}/{}", period, component_canonical(&end, true))
        }
        INTERVAL_FLAG_START_DURATION => {
            format!("{}/{}", component_canonical(&start, false), period)
        }
        _ => format!(
            "{}/{}",
            component_canonical(&start, false),
            component_canonical(&end, true)
        ),
    };
    LexicalInterval {
        start,
        end,
        flag: iv.flag,
        canonical,
    }
}

impl TypedDict {
    /// Return a dictionary whose DateTimeInterval segment holds lexical
    /// entries. Fixed-width entries (the shipped 48-byte width or the
    /// branch-internal 52-byte width) are decoded and re-encoded as lexical;
    /// anything else is assumed lexical already and passes through. Runs once
    /// at layer-open so every downstream reader sees a single format.
    pub fn upgrade_lexical_intervals(&self) -> Self {
        let Some(seg) = self.lexical_interval_segment() else {
            return self.clone();
        };
        let legacy = seg.clone().any(|id| {
            matches!(
                self.entry(id as usize).map(|e| e.to_bytes().len()),
                Some(INTERVAL_LEGACY_ENCODED_WIDTH | INTERVAL_ENCODED_WIDTH)
            )
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
                let lexical = match bytes.len() {
                    INTERVAL_LEGACY_ENCODED_WIDTH => {
                        encode_lexical_interval(&legacy_interval_to_lexical(
                            &DateTimeInterval::from_legacy_lexical(bytes.clone()),
                        ))
                    }
                    INTERVAL_ENCODED_WIDTH => encode_lexical_interval(&legacy_interval_to_lexical(
                        &DateTimeInterval::from_lexical(bytes.clone()),
                    )),
                    _ => bytes.to_vec(),
                };
                TypedDictEntry::new(
                    Datatype::DateTimeInterval,
                    SizedDictEntry::from(Bytes::copy_from_slice(&lexical)),
                )
            } else {
                entry
            };
            builder.add(entry);
        }
        let (t, o, bo, d) = builder.finalize();
        TypedDict::from_parts(t.freeze(), o.freeze(), bo.freeze(), d.freeze())
    }
}

/// Build the interval end index over a dictionary whose DateTimeInterval
/// segment holds lexical entries, writing the four files: the stride-2 end
/// rank table (`ends`), and the wavelet tree's `bits`, `blocks`, `sblocks`.
pub async fn build_interval_end_index_lexical<F: 'static + FileLoad + FileStore>(
    dict: &TypedDict,
    destination_ends: F,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    let mut ends = Vec::new();
    for id in dict.lexical_interval_segment().unwrap_or(0..0) {
        let entry = dict.entry(id as usize).expect("interval id in segment");
        let iv = decode_lexical_entry(&entry.to_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        ends.push(
            end_rank_key(&iv.end).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );
    }

    let mut distinct = ends.clone();
    distinct.sort_unstable();
    distinct.dedup();

    let mut ends_builder = LogArrayFileBuilder::new(destination_ends.open_write().await?, 64);
    for &(s, n) in &distinct {
        ends_builder.push(s as u64).await?;
        ends_builder.push(n as u64).await?;
    }
    ends_builder.finalize().await?;

    let ranks: Vec<u64> = ends
        .iter()
        .map(|e| distinct.binary_search(e).unwrap() as u64)
        .collect();
    build_wavelet_tree_from_iter(
        end_rank_width(distinct.len()),
        ranks.into_iter(),
        destination_bits,
        destination_blocks,
        destination_sblocks,
    )
    .await
}

// ---- Allen interval relations (literal-level evaluator) ----

/// The thirteen Allen interval relations; inverses evaluate as the
/// corresponding base relation with swapped operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllenRelation {
    Before,
    Meets,
    Overlaps,
    Starts,
    During,
    Finishes,
    Equals,
    After,
    MetBy,
    OverlappedBy,
    StartedBy,
    Contains,
    FinishedBy,
}

impl std::str::FromStr for AllenRelation {
    type Err = LexicalIntervalError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use AllenRelation::*;
        Ok(match s {
            "before" => Before,
            "meets" => Meets,
            "overlaps" => Overlaps,
            "starts" => Starts,
            "during" => During,
            "finishes" => Finishes,
            "equals" => Equals,
            "after" => After,
            "met_by" => MetBy,
            "overlapped_by" => OverlappedBy,
            "started_by" => StartedBy,
            "contains" => Contains,
            "finished_by" => FinishedBy,
            _ => return Err(LexicalIntervalError::UnknownRelation(s.to_string())),
        })
    }
}

/// Parse a literal as an interval; a bare instant literal coerces to the
/// zero-duration point interval `[t, t)` so `dateTime` operands work under
/// Allen relations.
pub fn literal_as_interval(s: &str) -> Result<LexicalInterval, LexicalIntervalError> {
    parse_lexical_interval(s).or_else(|_| parse_lexical_interval(&format!("{s}/{s}")))
}

/// Semantic ordering position of a component: `Bottom`/`Top` are the
/// unbounded extremes, `Null` is any incommensurable special (unknown,
/// unspecified) — comparisons against Null are false, never errors.
#[derive(Debug)]
enum Bound {
    Instant {
        secs: i64,
        is_neg: bool,
        frac: String,
    },
    Bottom,
    Top,
    Null,
}

impl Bound {
    fn start(c: &Component) -> Self {
        match c {
            Component::Finite(f) => Bound::instant(f),
            Component::Unbounded => Bound::Bottom,
            _ => Bound::Null,
        }
    }
    fn end(c: &Component) -> Self {
        match c {
            Component::Finite(f) => Bound::instant(f),
            Component::Unbounded => Bound::Top,
            _ => Bound::Null,
        }
    }
    fn instant(f: &FiniteComponent) -> Self {
        Bound::Instant {
            secs: secs_nanos(f).0,
            is_neg: f.is_neg,
            frac: f.frac.clone().unwrap_or_default(),
        }
    }
    fn cmp(a: &Self, b: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (a, b) {
            (Bound::Null, _) | (_, Bound::Null) => None,
            (Bound::Bottom, Bound::Bottom) | (Bound::Top, Bound::Top) => Some(Ordering::Equal),
            (Bound::Bottom, _) | (_, Bound::Top) => Some(Ordering::Less),
            (Bound::Top, _) | (_, Bound::Bottom) => Some(Ordering::Greater),
            (
                Bound::Instant {
                    secs: sa,
                    is_neg: na,
                    frac: fa,
                },
                Bound::Instant {
                    secs: sb,
                    is_neg: nb,
                    frac: fb,
                },
            ) => Some(sa.cmp(sb).then_with(|| {
                // Negative fracs count down from the ceiling int, so their
                // digits order *descending* with value.
                let c = fa.as_str().cmp(fb.as_str());
                if *na {
                    c.reverse()
                } else {
                    c
                }
            })),
        }
    }
    fn lt(a: &Self, b: &Self) -> bool {
        Bound::cmp(a, b) == Some(std::cmp::Ordering::Less)
    }
    fn gt(a: &Self, b: &Self) -> bool {
        Bound::cmp(a, b) == Some(std::cmp::Ordering::Greater)
    }
    fn eq(a: &Self, b: &Self) -> bool {
        Bound::cmp(a, b) == Some(std::cmp::Ordering::Equal)
    }
    fn le(a: &Self, b: &Self) -> bool {
        matches!(
            Bound::cmp(a, b),
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
        )
    }
}

/// Evaluate an Allen relation between two decoded intervals. Endpoints are
/// half-open `[start, end)`; non-finite endpoints follow the ordering
/// (`..` open = the true extreme) and unknown/unspecified endpoints make
/// every comparison false (NULL semantics). The point sorting is `i1, t, i2`
/// when i1 meets i2 at t: a point on the left of `during` is contained when
/// `bs <= t < be`, so the seam point belongs to the interval it starts —
/// exactly one of two consecutive intervals.
pub fn interval_relation(a: &LexicalInterval, rel: AllenRelation, b: &LexicalInterval) -> bool {
    use AllenRelation::*;
    match rel {
        After => return interval_relation(b, Before, a),
        MetBy => return interval_relation(b, Meets, a),
        OverlappedBy => return interval_relation(b, Overlaps, a),
        StartedBy => return interval_relation(b, Starts, a),
        Contains => return interval_relation(b, During, a),
        FinishedBy => return interval_relation(b, Finishes, a),
        _ => {}
    }
    let (as_, ae) = (Bound::start(&a.start), Bound::end(&a.end));
    let (bs, be) = (Bound::start(&b.start), Bound::end(&b.end));
    match rel {
        Equals => Bound::eq(&as_, &bs) && Bound::eq(&ae, &be),
        Before => Bound::lt(&ae, &bs),
        Meets => Bound::eq(&ae, &bs),
        Overlaps => Bound::lt(&as_, &bs) && Bound::lt(&bs, &ae) && Bound::lt(&ae, &be),
        Starts => Bound::eq(&as_, &bs) && Bound::lt(&ae, &be),
        Finishes => Bound::gt(&as_, &bs) && Bound::eq(&ae, &be),
        During if Bound::eq(&as_, &ae) => Bound::le(&bs, &as_) && Bound::lt(&as_, &be),
        During => Bound::lt(&bs, &as_) && Bound::lt(&ae, &be),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tfc::block::SizedDictEntry;
    use crate::tfc::typed::{TypedDictBufBuilder, TypedDictEntry};

    fn entry(s: &str) -> Vec<u8> {
        encode_lexical_interval(&parse_lexical_interval(s).unwrap())
    }

    fn iv_entry(s: &str) -> TypedDictEntry {
        TypedDictEntry::new(
            Datatype::DateTimeInterval,
            SizedDictEntry::from(Bytes::from(entry(s))),
        )
    }

    fn str_entry(s: &str) -> TypedDictEntry {
        TypedDictEntry::new(
            Datatype::String,
            SizedDictEntry::from(Bytes::copy_from_slice(s.as_bytes())),
        )
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

    /// Layout: ids 1-2 strings; 3: ../.. ; 4: X*/2025-12-31 ;
    /// 5: /2025-03-01 (unknown start); 6-8 finite starts.
    fn probe_dict() -> TypedDict {
        build_dict(vec![
            str_entry("alpha"),
            str_entry("beta"),
            iv_entry("../.."),
            iv_entry("X*/2025-12-31"),
            iv_entry("/2025-03-01"),
            iv_entry("2025-01-01/2025-02-01"),
            iv_entry("2025-03-01/2025-04-01"),
            iv_entry("2025-03-01/2025-05-01"),
            iv_entry("2025-06-01/2025-07-01"),
        ])
    }

    const S1: i64 = 1735689600; // 2025-01-01T00:00:00Z
    const S2: i64 = 1740787200; // 2025-03-01T00:00:00Z
    const S4: i64 = 1748736000; // 2025-06-01T00:00:00Z

    #[test]
    fn lexical_segment_ids() {
        let dict = probe_dict();
        assert_eq!(Some(3..10), dict.lexical_interval_segment());
    }

    #[test]
    fn probe_start_le_includes_open_and_unspecified() {
        let dict = probe_dict();
        let r = dict.lexical_ids_start_le(S2, 0);
        // {.., X*} block ids 3..5, finite starts <= S2 ids 6..9
        assert_eq!(r.special, 3..5);
        assert_eq!(r.finite, 6..9);
        let r = dict.lexical_ids_start_le(S4, 0);
        assert_eq!(r.finite, 6..10);
    }

    #[test]
    fn probe_start_lt_excludes_equal_start() {
        let dict = probe_dict();
        let r = dict.lexical_ids_start_lt(S2, 0);
        assert_eq!(r.special, 3..5);
        assert_eq!(r.finite, 6..7);
    }

    #[test]
    fn probe_start_ge_gt_finite_only() {
        let dict = probe_dict();
        let ge = dict.lexical_ids_start_ge(S2, 0);
        assert_eq!(ge.special, 0..0);
        assert_eq!(ge.finite, 7..10);
        let gt = dict.lexical_ids_start_gt(S2, 0);
        assert_eq!(gt.finite, 9..10);
        // below the lowest finite start: all finite
        assert_eq!(dict.lexical_ids_start_ge(S1 - 1, 0).finite, 6..10);
        // above the highest: empty
        assert_eq!(dict.lexical_ids_start_gt(S4, 0).finite, 10..10);
    }

    #[test]
    fn probe_start_eq_range_over_duplicate_starts() {
        let dict = probe_dict();
        assert_eq!(dict.lexical_ids_start_eq(S2, 0).finite, 7..9);
        assert_eq!(dict.lexical_ids_start_eq(S1, 0).finite, 6..7);
        assert_eq!(dict.lexical_ids_start_eq(S1 + 1, 0).finite, 7..7);
    }

    /// Semantic comparison: probes compare the instant *value*, not the
    /// spelling. Every literal denoting 2025-03-01T00:00:00Z — different
    /// fraction precisions, zone spellings, date vs dateTime kind — shares
    /// the instant field and must sit inside the eq range.
    #[test]
    fn probe_eq_unifies_same_instant_spellings() {
        let dict = build_dict(vec![
            // ids 1-8: spellings of the same instant (S2), sorted:
            // date kind < datetime, then tz class, then frac length
            iv_entry("2025-03-01/2030-01-01"), // 1 date kind
            iv_entry("2025-03-01T00:00:00-00:00/2030-01-01"), // 2 -00:00
            iv_entry("2025-03-01T00:00:00Z/2030-01-01"), // 3 Z
            iv_entry("2025-03-01T00:00:00.0Z/2030-01-01"), // 4 Z .0
            iv_entry("2025-03-01T00:00:00.000Z/2030-01-01"), // 5 Z .000
            iv_entry("2025-03-01T00:00:00/2030-01-01"), // 6 zone-less
            iv_entry("2025-03-01T00:00:00.00+00:00/2030-01-01"), // 7 +00:00 .00
            iv_entry("2025-03-01T01:00:00+01:00/2030-01-01"), // 8 +01:00
            iv_entry("2025-03-01T00:00:01Z/2030-01-01"), // 9 later instant
        ]);
        // one second boundary covers every spelling of the instant
        assert_eq!(dict.lexical_ids_start_eq(S2, 0).finite, 1..9);
        // le includes them (plus anything earlier — none here)
        assert_eq!(dict.lexical_ids_start_le(S2, 0).finite, 1..9);
        // ge includes the block and everything after
        assert_eq!(dict.lexical_ids_start_ge(S2, 0).finite, 1..10);
        // lt / gt exclude the whole equal-instant block
        assert_eq!(dict.lexical_ids_start_lt(S2, 0).finite, 1..1);
        assert_eq!(dict.lexical_ids_start_gt(S2, 0).finite, 9..10);
    }

    #[test]
    fn probe_missing_segment() {
        let dict = build_dict(vec![str_entry("only")]);
        assert_eq!(dict.lexical_interval_segment(), None);
        assert_eq!(dict.lexical_ids_start_le(S2, 0).finite, 0..0);
        assert_eq!(dict.lexical_ids_start_le(S2, 0).special, 0..0);
        assert_eq!(dict.lexical_ids_start_eq(S2, 0).finite, 0..0);
    }

    #[test]
    fn probe_nanos_boundary() {
        let dict = probe_dict();
        // S2-1ns and S2+1ns land either side of the S2 starts
        assert_eq!(dict.lexical_ids_start_le(S2 - 1, 999_999_999).finite, 6..7);
        assert_eq!(dict.lexical_ids_start_le(S2, 1).finite, 6..9);
    }

    #[test]
    fn end_index_lexical() {
        let dict = probe_dict();
        let idx = IntervalEndIndex::build_lexical(&dict).unwrap();
        assert_eq!(idx.len(), 7);
        // positions are 0-based within the interval segment
        let pos = 0..idx.len() as u64;
        // ends <= 2025-05-01T00:00:00Z: finite ends 2025-02-02 (pos3),
        // 2025-03-02 (pos2), 2025-04-02 (pos4); +inf (pos0) and the
        // 2026-01-01 (pos1), 2025-05-02 (pos5), 2025-07-02 (pos6) ends are over
        let got = idx.positions_end_le(pos.clone(), 1746057600, 0);
        let mut got = got;
        got.sort();
        assert_eq!(got, vec![2, 3, 4]);
        // end >= 2025-05-01: +inf (pos0), 2026-01-01 (pos1), 2025-05-02 (pos5),
        // 2025-07-02 (pos6)
        let mut ge = idx.positions_end_ge(pos.clone(), 1746057600, 0);
        ge.sort();
        assert_eq!(ge, vec![0, 1, 5, 6]);
    }

    #[test]
    fn end_index_rejects_subnano_end() {
        let dict = build_dict(vec![iv_entry(
            "2025-01-01T00:00:00.123456789012Z/2025-01-01T00:00:01.999999999999Z",
        )]);
        assert!(matches!(
            IntervalEndIndex::build_lexical(&dict),
            Err(LexicalIntervalError::UnrepresentableEnd(_))
        ));
    }

    // -- canonicalisation / packing ------------------------------------------

    #[test]
    fn canonicalise_strips_separators_keeps_signs() {
        assert_eq!(
            canonicalise_component("2025-01-31T10:30:00+02:00"),
            "20250131T103000+0200"
        );
        assert_eq!(canonicalise_component("-2025-01-31"), "-20250131");
        assert_eq!(canonicalise_component("2025-01-31"), "20250131");
        assert_eq!(canonicalise_component(".."), "..");
        assert_eq!(canonicalise_component("X*"), "X*");
        assert_eq!(canonicalise_component(""), "");
        assert_eq!(canonicalise_component("P1Y2MT3H"), "P1Y2MT3H");
        assert_eq!(canonicalise_component("-P1D"), "-P1D");
    }

    #[test]
    fn extend_restores_extended_form() {
        assert_eq!(extend_component("20250131"), "2025-01-31");
        assert_eq!(
            extend_component("20250131T103000+0200"),
            "2025-01-31T10:30:00+02:00"
        );
        assert_eq!(
            extend_component("20250131T103000.5-0000"),
            "2025-01-31T10:30:00.5-00:00"
        );
        assert_eq!(extend_component("20250131T103000Z"), "2025-01-31T10:30:00Z");
    }

    // -- component ordering ---------------------------------------------------

    #[test]
    fn start_tail_order() {
        // -inf < unspecified < unknown < finite
        let entries = [
            entry("2025-01-01T00:00:00Z/2025-01-02T00:00:00Z"), // finite start
            entry("X*/2025-01-02T00:00:00Z"),                   // unspecified
            entry("../2025-01-02T00:00:00Z"),                   // unbounded
            entry("/2025-01-02T00:00:00Z"),                     // unknown
        ];
        let mut sorted = entries.clone();
        sorted.sort();
        assert_eq!(sorted[0], entries[2]); // ..
        assert_eq!(sorted[1], entries[1]); // X*
        assert_eq!(sorted[2], entries[3]); // unknown
        assert_eq!(sorted[3], entries[0]); // finite
    }

    #[test]
    fn end_tail_order() {
        // finite < unknown < unspecified < +inf
        let s = "2025-01-01T00:00:00Z";
        let entries = [
            entry(&format!("{s}/2026-01-01T00:00:00Z")), // finite end
            entry(&format!("{s}/")),                     // unknown end
            entry(&format!("{s}/X*")),                   // unspecified end
            entry(&format!("{s}/..")),                   // unbounded end
        ];
        let mut sorted = entries.clone();
        sorted.sort();
        assert_eq!(sorted[0], entries[0]); // finite
        assert_eq!(sorted[1], entries[1]); // unknown
        assert_eq!(sorted[2], entries[2]); // X*
        assert_eq!(sorted[3], entries[3]); // ..
    }

    #[test]
    fn instant_ordering_precision() {
        // sub-second fractions order exactly at arbitrary precision
        let a = entry("2025-01-01T00:00:00.5Z/2026-01-01T00:00:00Z");
        let b = entry("2025-01-01T00:00:00.500000001Z/2026-01-01T00:00:00Z");
        let c = entry("2025-01-01T00:00:00.50000000000000000000000000000000000000000001Z/2026-01-01T00:00:00Z");
        // 0.5 < 0.5000000000…01 < 0.500000001
        assert!(a < c);
        assert!(c < b);
        // equal instant, distinct precision: `.5` and `.50` are different
        // literals that must stay adjacent — spelling is the tie-break
        let d = entry("2025-01-01T00:00:00.50Z/2026-01-01T00:00:00Z");
        assert!(a < d && d < c, ".5 < .50 < .500…01");
    }

    #[test]
    fn tz_axis_same_instant() {
        let e = "2026-01-01T00:00:00Z";
        // All denote 2025-01-01T07:00:00Z
        let entries = [
            entry(&format!("2025-01-01T02:00:00-05:00/{e}")), // neg offset
            entry(&format!("2025-01-01T07:00:00-00:00/{e}")), // -00:00
            entry(&format!("2025-01-01T07:00:00Z/{e}")),      // Z
            entry(&format!("2025-01-01T07:00:00/{e}")),       // zone-less
            entry(&format!("2025-01-01T07:00:00+00:00/{e}")), // +00:00
            entry(&format!("2025-01-01T08:00:00+01:00/{e}")), // pos offset
        ];
        let mut sorted = entries.clone();
        sorted.sort();
        assert_eq!(
            sorted, entries,
            "neg < -00:00 < Z < zoneless < +00:00 < pos"
        );
    }

    #[test]
    fn negative_offsets_order_by_value() {
        let e = "2026-01-01T00:00:00Z";
        // same instant 07:00Z spelled with -14:00 and -05:00 shifts
        let a = entry(&format!("2024-12-31T17:00:00-14:00/{e}"));
        let b = entry(&format!("2025-01-01T02:00:00-05:00/{e}"));
        assert!(a < b, "-14:00 sorts before -05:00 at equal instant");
    }

    // -- interval ordering ----------------------------------------------------

    #[test]
    fn flag_and_spelling_block() {
        // same instant pair, three spellings — contiguous, explicit first
        let a = entry("P1M/2026-01-01T00:00:00Z");
        let b = entry("2025-12-01T00:00:00Z/P1M");
        let c = entry("2025-12-01T00:00:00Z/2026-01-01T00:00:00Z");
        let mut sorted = vec![a.clone(), b.clone(), c.clone()];
        sorted.sort();
        assert_eq!(sorted, vec![c, b, a], "T/T < T/P < P/T at equal instants");
    }

    #[test]
    fn kind_orders_date_before_datetime() {
        let e = "2026-01-01T00:00:00Z";
        let d = entry(&format!("2025-01-01/{e}"));
        let dt = entry(&format!("2025-01-01T00:00:00Z/{e}"));
        assert!(d < dt);
    }

    // -- validation -----------------------------------------------------------

    #[test]
    fn incommensurable_specials_rejected() {
        for s in ["X*/..", "../X*", "/X*", "X*/", "/..", "../"] {
            assert!(
                matches!(
                    parse_lexical_interval(s),
                    Err(LexicalIntervalError::Incommensurable(_))
                ),
                "{s} should be rejected"
            );
        }
        for s in [
            "../..",
            "X*/X*",
            "/",
            "X*/2025-01-01",
            "2025-01-01/X*",
            "../2025-01-01",
            "2025-01-01/",
            "/2025-01-01",
            "2025-01-01/..",
        ] {
            assert!(parse_lexical_interval(s).is_ok(), "{s} should parse");
        }
    }

    #[test]
    fn duration_with_special_rejected() {
        for s in ["P1M/..", "P1M/", "P1M/X*", "../P1M", "/P1M", "X*/P1M"] {
            assert!(parse_lexical_interval(s).is_err(), "{s} should be rejected");
        }
    }

    #[test]
    fn invalid_wall_clock_values_error_not_panic() {
        for s in [
            "2025-01-01T24:00:00/2025-01-02",
            "2025-01-01T23:60:00/2025-01-02",
            "2025-01-01T23:59:61/2025-01-02",
            "2025-01-01T99:99:99/2025-01-02",
            "2025-01-01T24:00:00Z/P1D",
        ] {
            assert!(
                matches!(
                    parse_lexical_interval(s),
                    Err(LexicalIntervalError::InvalidComponent(_))
                ),
                "{s} should be a parse error, not a panic"
            );
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut enc = entry("2025-01-01/2025-01-02");
        enc.push(0xAA);
        assert!(matches!(
            decode_lexical_entry(&enc),
            Err(LexicalIntervalError::MalformedEntry)
        ));
    }

    #[test]
    #[should_panic(expected = "nanos must be < 1_000_000_000")]
    fn probe_nanos_above_second_rejected() {
        let dict = probe_dict();
        let _ = dict.lexical_ids_start_eq(S2, 1_000_000_000);
    }

    // -- round trip -----------------------------------------------------------

    #[test]
    fn entry_roundtrip() {
        for s in [
            "../..",
            "/",
            "X*/X*",
            "2025-01-31/2025-03-31",
            "2025-01-31T10:30:00Z/2025-03-31T15:45:00Z",
            "2025-01-31T10:30:00+02:00/2025-03-31T15:45:00-05:00",
            "2025-01-31T10:30:00/2025-03-31T15:45:00",
            "2025-12-01T00:00:00Z/P1M",
            "P1M/2025-12-31",
            "P1M/2026-01-01T00:00:00Z",
            "2025-01-01T00:00:00.5Z/2026-01-01T00:00:00Z",
            "2025-01-01/..",
            "../2026-01-01",
            "2025-01-01/",
            "/2026-01-01",
            "X*/2026-01-01",
            "2025-01-01/X*",
            "2025-01-01T00:00:00Z/2025-01-01T00:00:00Z",
        ] {
            let iv = parse_lexical_interval(s).unwrap();
            let enc = encode_lexical_interval(&iv);
            let dec = decode_lexical_entry(&enc).unwrap();
            assert_eq!(dec.canonical, iv.canonical, "{s}");
            assert_eq!(render_lexical_interval(&dec), s, "{s}");
        }
    }

    // -- deep time and fine precision ------------------------------------------

    #[test]
    fn cro_magnon_day_roundtrip_and_order() {
        // A day's events ~41 000 years ago — deep negative epoch seconds.
        let morning = "-39000-06-15T06:30:00Z/-39000-06-15T08:00:00Z";
        let evening = "-39000-06-15T18:45:00Z/-39000-06-15T20:15:00Z";
        for s in [morning, evening] {
            let iv = parse_lexical_interval(s).unwrap();
            let enc = encode_lexical_interval(&iv);
            let dec = decode_lexical_entry(&enc).unwrap();
            assert_eq!(render_lexical_interval(&dec), s, "{s}");
        }
        // morning sorts before evening even far below the epoch
        assert!(entry(morning) < entry(evening));

        // sanity: the start instant lands where chrono says it should
        // (-39000-06-15T06:30:00Z = -1292874048000 + 23400 s)
        let iv = parse_lexical_interval(morning).unwrap();
        let Component::Finite(f) = &iv.start else {
            panic!("finite start")
        };
        assert_eq!(f.int, Integer::from(-1292874024600_i64));
    }

    #[test]
    fn deep_past_dates_and_duration_forms() {
        // pure-date intervals and duration forms far below the epoch
        for s in [
            "-39000-01-01/-39000-12-31",
            "-12000-03-01T00:00:00Z/P30D",
            "P1Y/-39999-12-31T00:00:00Z",
            "-39000-06-15/-38999-06-14",
        ] {
            let iv = parse_lexical_interval(s).unwrap();
            let enc = encode_lexical_interval(&iv);
            let dec = decode_lexical_entry(&enc).unwrap();
            assert_eq!(render_lexical_interval(&dec), s, "{s}");
        }
    }

    #[test]
    fn sub_nanosecond_precision() {
        // picosecond, femtosecond and attosecond-scale fractions round-trip
        for s in [
            "2025-01-01T00:00:00.123456789012Z/2025-01-01T00:00:00.123456789013Z",
            "2025-01-01T00:00:00.000000000000001Z/2025-01-01T00:00:01Z",
            "2025-01-01T00:00:00.999999999999999999999999Z/2025-01-01T00:00:01Z",
            "1969-12-31T23:59:59.999999999999999Z/1970-01-01T00:00:00Z",
        ] {
            let iv = parse_lexical_interval(s).unwrap();
            let enc = encode_lexical_interval(&iv);
            let dec = decode_lexical_entry(&enc).unwrap();
            assert_eq!(render_lexical_interval(&dec), s, "{s}");
        }
    }

    #[test]
    fn sub_nanosecond_ordering() {
        // two instants differing only in the 12th fraction digit order correctly
        let e = "2026-01-01T00:00:00Z";
        let a = entry(&format!("2025-01-01T00:00:00.123456789011/{e}"));
        let b = entry(&format!("2025-01-01T00:00:00.123456789012/{e}"));
        assert!(a < b);
        // and across the epoch boundary
        let neg = entry(&format!("1969-12-31T23:59:59.999999999999/{e}"));
        let pos = entry(&format!("1970-01-01T00:00:00.000000000001/{e}"));
        assert!(neg < pos);
    }

    #[test]
    fn negative_subsecond_instants() {
        // v = -1.5 s and v = -0.5 s (floor/ceiling conventions on the wire)
        let iv = parse_lexical_interval("1969-12-31T23:59:58.5/1969-12-31T23:59:59.5").unwrap();
        let Component::Finite(f) = &iv.start else {
            panic!("finite start")
        };
        assert_eq!(f.int, Integer::from(-1_i64)); // -1.5 = int -1 minus frac .5
        assert_eq!(f.frac.as_deref(), Some("5"));
        let Component::Finite(g) = &iv.end else {
            panic!("finite end")
        };
        assert_eq!(g.int, Integer::from(0_i64));
        assert_eq!(g.frac.as_deref(), Some("5"));
        assert_eq!(
            render_lexical_interval(&decode_lexical_entry(&encode_lexical_interval(&iv)).unwrap()),
            "1969-12-31T23:59:58.5/1969-12-31T23:59:59.5"
        );
    }

    // -- ordering proofs ---------------------------------------------------------
    //
    // Auditable ordering tests: each `expected` list is the sorted order
    // written out literally — read top to bottom it *is* the spec. The
    // helper encodes, sorts bytewise, decodes and renders, so a failure
    // shows the exact strings out of place.

    /// Encode each lexical, sort the entry bytes, decode and render.
    fn sorted_rendered<S: AsRef<str>>(input: &[S]) -> Vec<String> {
        let mut es: Vec<Vec<u8>> = input.iter().map(|s| entry(s.as_ref())).collect();
        es.sort();
        es.iter()
            .map(|e| render_lexical_interval(&decode_lexical_entry(e).unwrap()))
            .collect()
    }

    #[test]
    fn order_start_tail_then_instant() {
        // start classes: unbounded < unspecified < unknown < finite; finite
        // starts then order by instant. Same end throughout.
        let expected = [
            "../2025-01-01",
            "X*/2025-01-01",
            "/2025-01-01",
            "1969-12-31/2025-01-01",
            "2025-01-01/2025-01-01",
        ];
        let input = [
            "2025-01-01/2025-01-01",
            "/2025-01-01",
            "1969-12-31/2025-01-01",
            "X*/2025-01-01",
            "../2025-01-01",
        ];
        assert_eq!(sorted_rendered(&input), expected);
    }

    #[test]
    fn order_end_tail_then_instant() {
        // end classes: finite < unknown < unspecified < unbounded; finite
        // ends order by instant first. Same start throughout.
        let expected = [
            "2025-01-01/1969-12-31",
            "2025-01-01/2025-02-01",
            "2025-01-01/",
            "2025-01-01/X*",
            "2025-01-01/..",
        ];
        assert_eq!(sorted_rendered(&expected), expected);
    }

    #[test]
    fn order_same_instant_timeshift_axis() {
        // Every start below denotes 2025-03-15T10:30:00Z. Order among equal
        // instants is the timeshift axis:
        // neg offsets < -00:00 < Z < zone-less < +00:00 < pos offsets,
        // with offsets ordered by value within each sign class.
        let e = "2030-01-01";
        let expected = [
            format!("2025-03-14T20:30:00-14:00/{e}"), // = 10:30Z; neg, -840
            format!("2025-03-15T05:30:00-05:00/{e}"), // neg, -300
            format!("2025-03-15T10:30:00-00:00/{e}"), // -00:00
            format!("2025-03-15T10:30:00Z/{e}"),      // Z
            format!("2025-03-15T10:30:00/{e}"),       // zone-less
            format!("2025-03-15T10:30:00+00:00/{e}"), // +00:00
            format!("2025-03-15T11:30:00+01:00/{e}"), // pos, +60
            format!("2025-03-15T16:00:00+05:30/{e}"), // pos, +330
            format!("2025-03-16T00:30:00+14:00/{e}"), // pos, +840
        ];
        let mut shuffled = expected.clone();
        shuffled.reverse();
        assert_eq!(sorted_rendered(&shuffled), expected);
    }

    #[test]
    fn order_kind_and_flag_tiebreaks() {
        // Same instant pair [Jan 1, Apr 1): kind date < dateTime, then flag
        // T/T < T/P < P/T — the four spellings stay adjacent and ordered.
        let expected = [
            "2026-01-01/2026-03-31",                     // T/T, date kind
            "2026-01-01/P3M",                            // T/P
            "P3M/2026-03-31",                            // P/T
            "2026-01-01T00:00:00Z/2026-04-01T00:00:00Z", // T/T, datetime kind
        ];
        let input = [
            "2026-01-01T00:00:00Z/2026-04-01T00:00:00Z",
            "P3M/2026-03-31",
            "2026-01-01/P3M",
            "2026-01-01/2026-03-31",
        ];
        assert_eq!(sorted_rendered(&input), expected);
    }

    #[test]
    fn order_period_field() {
        // Same resolved span [Jan 2, Apr 2): supplied P90D sorts against the
        // T/T derived P3M1D by plain string order in the period field.
        let expected = [
            "2025-01-02T00:00:00Z/2025-04-02T00:00:00Z", // derived "P3M1D"
            "2025-01-02T00:00:00Z/P90D",                 // supplied "P90D"
        ];
        let input = [
            "2025-01-02T00:00:00Z/P90D",
            "2025-01-02T00:00:00Z/2025-04-02T00:00:00Z",
        ];
        assert_eq!(sorted_rendered(&input), expected);
    }

    #[test]
    fn order_precision_and_spelling_tiebreaks() {
        // Distinct instants order by exact value at any precision; equal
        // instants differing only in precision stay adjacent, ordered by
        // spelling (`.5` < `.50` < `.500`): zeros are significant.
        let e = "2030-01-01";
        let expected = [
            format!("2025-03-15T10:30:00.499999999999999999/{e}"),
            format!("2025-03-15T10:30:00.5/{e}"),
            format!("2025-03-15T10:30:00.50/{e}"),
            format!("2025-03-15T10:30:00.500/{e}"),
            format!("2025-03-15T10:30:00.500000000000000001/{e}"),
            format!("2025-03-15T10:30:00.999999999999999999/{e}"),
        ];
        let mut shuffled = expected.clone();
        shuffled.reverse();
        assert_eq!(sorted_rendered(&shuffled), expected);
    }

    #[test]
    fn order_deep_time_and_epoch_boundary() {
        // Negative instants sort before positive; ceiling-frac negatives
        // keep exact sub-second order; epoch boundary is exact.
        let e = "2030-01-01";
        let expected = [
            format!("-39000-06-15T06:30:00Z/{e}"),
            format!("-39000-06-15T06:30:00.12345678901234567890123Z/{e}"),
            format!("1969-12-31T23:59:59/{e}"),
            format!("1969-12-31T23:59:59.5/{e}"), // -0.5 s
            format!("1969-12-31T23:59:59.999999999999999999/{e}"), // just under 0
            format!("1970-01-01T00:00:00Z/{e}"),  // exactly 0
            format!("1970-01-01T00:00:00.000000000000000001/{e}"), // just over 0
        ];
        let mut shuffled = expected.clone();
        shuffled.reverse();
        assert_eq!(sorted_rendered(&shuffled), expected);
    }

    #[test]
    fn order_master_exhaustive_list() {
        // One hand-audited list mixing every dimension: classes, instants,
        // kinds, timeshifts, flags, periods and spelling tiebreaks.
        let expected = [
            "../2025-03-15",                                  // start unbounded, end finite first
            "../..",                                          // start unbounded, end unbounded last
            "X*/2025-03-15",                                  // start unspecified, end finite
            "X*/X*",                                          // start unspecified, end unspecified
            "/2025-03-15",                                    // start unknown
            "1969-12-31/..",                                  // earliest finite start (-86400)
            "1969-12-31T23:59:59.5Z/1970-01-01",              // -0.5 s
            "1970-01-01T00:00:00Z/..",                        // epoch
            "P1M/2025-03-15",                                 // P/T: start resolved to Feb 16
            "2025-03-15/2025-03-16",                          // start Mar 15 00:00, end finite
            "2025-03-15/2025-06-14",                          // same start, later end
            "2025-03-15/",                                    // end unknown
            "2025-03-15/X*",                                  // end unspecified
            "2025-03-15/..",                                  // end unbounded last
            "2025-03-15T00:30:00-10:00/2025-03-15T12:30:00Z", // 10:30Z, neg tshift
            "2025-03-15T10:30:00-00:00/2025-03-15T12:30:00Z", // -00:00
            "2025-03-15T10:30:00Z/2025-03-15T12:30:00Z",      // Z
            "2025-03-15T10:30:00/2025-03-15T12:30:00Z",       // zone-less
            "2025-03-15T10:30:00+00:00/2025-03-15T12:30:00Z", // +00:00
            "2025-03-15T11:30:00+01:00/2025-03-15T12:30:00Z", // +01:00
            "2025-03-15T10:30:00.5Z/2025-03-15T12:30:00Z",    // later instant
            "2025-03-15T10:30:01Z/P2W",                       // later still, T/P
        ];
        let mut shuffled: Vec<&str> = expected.to_vec();
        shuffled.reverse();
        assert_eq!(sorted_rendered(&shuffled[..]), expected);
    }

    // -- the accepted spellings, in sorted order ------------------------------
    //
    // Every spelling the grammar accepts, already in canonical form
    // (extended separators, no trimmable fraction zeros), listed in the exact
    // order their encoded entries must sort. This table is the executable
    // specification of the lexical format: read top to bottom — each row
    // must sort before the next. Grouped and commented by the ordering rule
    // each block demonstrates.
    const SPELLINGS: &[&str] = &[
        // start-class tail: unbounded < unspecified < unknown < finite
        "../2025-03-15", // unbounded start; finite end sorts before special ends
        "../..",         // unbounded start, unbounded end
        "X*/2025-03-15", // unspecified start, finite end
        "X*/X*",         // unspecified start, unspecified end
        "/2025-03-15",   // unknown start
        // finite starts: deep past, then the epoch boundary
        "-39000-06-15/2030-01-01",
        "-39000-06-15T06:30:00Z/2030-01-01",
        "-39000-06-15T06:30:00.12345678901234567890123Z/2030-01-01",
        "1969-12-31/..",                                      // -86400 s
        "1969-12-31T23:59:59.5Z/2030-01-01",                  // -0.5 s
        "1969-12-31T23:59:59.999999999999999999Z/2030-01-01", // -1e-18 s
        "1970-01-01T00:00:00Z/..",                            // epoch
        "1970-01-01T00:00:00.000000000000000001Z/2030-01-01", // +1e-18 s
        // the leap second is a real instant: 23:59:59 < 23:59:60 < 00:00:00 —
        // and it orders by instant alone as an end too
        "2016-12-31T00:00:00Z/2016-12-31T23:59:60Z",
        "2016-12-31T23:59:59Z/2017-01-02",
        "2016-12-31T23:59:60Z/2017-01-02",
        "2017-01-01T00:00:00Z/2017-01-02",
        // duration-derived starts resolve to real instants
        "P1Y2M3DT4H5M6.5S/2025-03-15", // start = 2024-01-12T19:54:53.5
        "P1M/2025-03-15",              // start = Feb 16 (end Mar 15 → Mar 16)
        "P2W/2025-03-15T12:30:00Z",    // start = Mar 1; supplied P2W kept
        // Mar 15 00:00 start — end instant, then end-class tail
        "2025-03-15/2025-03-15", // end Mar 16 exclusive (inclusive Mar 15)
        "2025-03-15/P1D",        // same instant pair; flag T/P after T/T
        "2025-03-15/2025-03-16", // end Mar 17 exclusive
        "2025-03-15/2025-06-14",
        "2025-03-15/",   // end unknown
        "2025-03-15/X*", // end unspecified
        "2025-03-15/..", // end unbounded
        // 10:30Z instant cluster — end instant, then tshift, then flag
        "2025-03-15T10:30:00Z/PT6.5S", // end resolves to 10:30:00.5Z
        "2025-03-14T20:30:00-14:00/2025-03-15T12:30:00Z", // =10:30Z; neg,-840
        "2025-03-15T05:30:00-05:00/2025-03-15T12:30:00Z", // neg, -300
        "2025-03-15T10:30:00-00:00/2025-03-15T12:30:00Z", // -00:00
        "2025-03-15T10:30:00Z/2025-03-15T12:30:00Z", // Z
        "2025-03-15T10:30:00Z/PT2H",   // same pair; flag T/P
        "PT2H/2025-03-15T12:30:00Z",   // same pair; flag P/T
        "2025-03-15T10:30:00/2025-03-15T12:30:00Z", // zone-less
        "2025-03-15T10:30:00+00:00/2025-03-15T12:30:00Z", // +00:00
        "2025-03-15T11:30:00+01:00/2025-03-15T12:30:00Z", // pos, +60
        "2025-03-15T16:00:00+05:30/2025-03-15T12:30:00Z", // pos, +330
        "2025-03-16T00:30:00+14:00/2025-03-15T12:30:00Z", // pos, +840
        // later instants in the same hour
        "2025-03-15T10:30:00.499999999999999999Z/2030-01-01",
        "2025-03-15T10:30:00.5Z/2030-01-01",
        "2025-03-15T10:30:00.500000000000000001Z/2030-01-01",
        "2025-03-15T10:30:00.999999999999999999Z/2030-01-01",
        "2025-03-15T10:30:01Z/2025-03-15T12:30:00Z",
        "-P1D/2025-03-15", // negative duration: start = Mar 17
    ];

    /// Roundtrip: every accepted spelling encodes, decodes and renders back
    /// to the identical string.
    #[test]
    fn every_spelling_roundtrips_verbatim() {
        for s in SPELLINGS {
            let out = render_lexical_interval(&decode_lexical_entry(&entry(s)).unwrap());
            assert_eq!(&out, s, "roundtrip diverged");
        }
        // every row is a distinct literal — the table has no dedup collisions
        let set: std::collections::BTreeSet<Vec<u8>> = SPELLINGS.iter().map(|s| entry(s)).collect();
        assert_eq!(set.len(), SPELLINGS.len());
    }

    /// Ordering: entries sorted bytewise (Rust's plain `sort`, no custom
    /// comparator) render in exactly table order.
    #[test]
    fn spellings_sort_in_table_order() {
        let mut shuffled: Vec<&str> = SPELLINGS.to_vec();
        shuffled.reverse();
        assert_eq!(sorted_rendered(&shuffled), SPELLINGS);
    }

    /// Spellings differ by precision: `.50` ≠ `.5` — the trailing zero is a
    /// supplied significant figure and must round-trip verbatim. A fully-zero
    /// fraction keeps its dot and digits too.
    #[test]
    fn fraction_trailing_zeros_are_distinct_literals() {
        let variants = [
            "2025-03-15T10:30:00.5/2030-01-01",
            "2025-03-15T10:30:00.50/2030-01-01",
            "2025-03-15T10:30:00.500/2030-01-01",
            "2025-03-15T10:30:00.500000000/2030-01-01",
        ];
        let mut entries: Vec<Vec<u8>> = variants.iter().map(|v| entry(v)).collect();
        for e in entries.windows(2) {
            assert_ne!(e[0], e[1]);
            assert!(e[0] < e[1], "precision spellings order short to long");
        }
        // each spelling renders back verbatim
        for (i, v) in variants.iter().enumerate() {
            let rendered = render_lexical_interval(&decode_lexical_entry(&entries[i]).unwrap());
            assert_eq!(&rendered, v);
        }
        entries.clear();
        // all-zero fraction keeps its digits
        let zero = entry("2025-03-15T10:30:00.000/2030-01-01");
        assert_eq!(
            render_lexical_interval(&decode_lexical_entry(&zero).unwrap()),
            "2025-03-15T10:30:00.000/2030-01-01"
        );
        // and sorts after the fraction-less twin
        assert!(entry("2025-03-15T10:30:00/2030-01-01") < zero);
    }

    /// Negative instants keep precision too: the complement encoding
    /// `(10^len − f) mod 10^len` preserves digit count, and an all-zero
    /// fraction on a negative value stays integral (`.00` = −1.00 → int −1).
    #[test]
    fn negative_fraction_precision_round_trips() {
        for v in [
            "1969-12-31T23:59:59.5Z/1970-01-02",
            "1969-12-31T23:59:59.50Z/1970-01-02",
            "1969-12-31T23:59:59.00Z/1970-01-02",
            "1969-12-31T23:59:59.999Z/1970-01-02",
        ] {
            let e = entry(v);
            assert_eq!(
                render_lexical_interval(&decode_lexical_entry(&e).unwrap()),
                v,
                "{v}"
            );
        }
        // equal instant fields → ordering falls through every value field
        // to the spelling tail: supplied length is the tie-break, so .5
        // precedes .50 below the epoch exactly as above it
        let half = entry("1969-12-31T23:59:59.5Z/1970-01-02");
        let half0 = entry("1969-12-31T23:59:59.50Z/1970-01-02");
        assert!(half < half0);
    }

    /// Supplied duration fractions keep their precision in the period
    /// spelling, but the anchor is computed at nanos — a finer fraction
    /// must fail loudly rather than truncate.
    #[test]
    fn duration_fraction_precision() {
        // supplied spelling survives verbatim
        for v in ["2025-01-01/PT0.50S", "PT0.500S/2025-01-01"] {
            let e = entry(v);
            assert_eq!(
                render_lexical_interval(&decode_lexical_entry(&e).unwrap()),
                v
            );
        }
        // 9 digits is the nanos floor: accepted, computed end carries it
        let e = entry("2025-01-01T00:00:00Z/PT0.123456789S");
        assert_eq!(
            render_lexical_interval(&decode_lexical_entry(&e).unwrap()),
            "2025-01-01T00:00:00Z/PT0.123456789S"
        );
        // 10 digits is unrepresentable — parse must fail, not truncate
        assert!(matches!(
            parse_lexical_interval("2025-01-01/PT0.5000000001S"),
            Err(LexicalIntervalError::Parse(
                crate::tfc::interval::IntervalParseError::DurationTooPrecise(_)
            ))
        ));
    }

    /// Accepted-but-normalised forms render in extended canonical spelling.
    #[test]
    fn basic_forms_render_extended() {
        for (input, rendered) in [
            (
                "2025-03-15T103000Z/2030-01-01",
                "2025-03-15T10:30:00Z/2030-01-01",
            ),
            (
                "2025-03-15T10:30:00+0530/2030-01-01",
                "2025-03-15T10:30:00+05:30/2030-01-01",
            ),
            (
                "2025-03-15T10:30:00-0000/2030-01-01",
                "2025-03-15T10:30:00-00:00/2030-01-01",
            ),
        ] {
            assert_eq!(
                render_lexical_interval(&decode_lexical_entry(&entry(input)).unwrap()),
                rendered
            );
        }
    }

    /// Spellings outside the grammar are rejected, loudly.
    #[test]
    fn rejected_spellings() {
        for bad in [
            "20250315/2025-03-16",             // basic date — extended required
            "2025-03-15T10:30/2030-01-01",     // HH:MM without seconds
            "P1D/P2D",                         // two durations
            "../P1D",                          // duration anchored on a special
            "X*/P1D",                          // unspecified + duration
            "../X*",                           // two different special classes
            "2025-03-15T24:00:00/2030-01-01",  // hour 24 — ISO allows, xsd doesn't
            "2025-03-15T25:00:00/2030-01-01",  // hour 25
            "2025-03-15T12:34:60Z/2030-01-01", // :60 outside 23:59
            "2025-03-15T23:60:00Z/2030-01-01", // minute 60
            "2025-03-15T23:59:60Z/2030-01-01", // :60 on a non-leap day
            "2025-13-01/2030-01-01",           // month 13
            "201X/2025-03-15",                 // digit-level X — out of scope
        ] {
            assert!(
                parse_lexical_interval(bad).is_err(),
                "{bad} must be rejected"
            );
        }
    }

    // -- derived period --------------------------------------------------------

    #[test]
    fn derived_period_canonical_forms() {
        // date ends are inclusive: `2026-01-01/2026-03-31` materialises to
        // [Jan 1, Apr 1) — the exclusive form is `...T00:00:00/...T00:00:00`.
        for (s, want) in [
            ("2026-01-01/2026-03-31", "P3M"),
            ("2026-01-01T00:00:00/2026-04-01T00:00:00", "P3M"),
            ("2025-01-01/2026-03-31", "P1Y3M"),
            ("2026-01-01/2026-03-02", "P2M2D"), // through Mar 2 → Mar 3 exclusive
            ("2026-01-01/2026-01-15", "P15D"),  // W never emitted
            ("2025-01-31/2025-02-27", "P1M"),   // Jan 31 → Feb 28 clamp
            ("2025-01-31T00:00:00Z/2025-02-27T00:00:00Z", "P27D"),
            ("2025-12-01T00:00:00Z/2025-12-01T00:00:00Z", "PT0S"),
            ("-39000-06-15/-39000-09-14", "P3M"),
            ("2025-01-01T10:00:00.25Z/2025-01-01T10:00:01.5Z", "PT1.25S"),
        ] {
            let iv = parse_lexical_interval(s).unwrap();
            assert_eq!(derived_period(&iv), want, "{s}");
        }
        // all-special intervals have no finite pair to decompose
        assert_eq!(
            derived_period(&parse_lexical_interval("../..").unwrap()),
            ""
        );
    }

    #[test]
    fn derived_period_leap_second_counts_seconds_not_hours() {
        // 2016-12-31 carries a leap second: [23:00, 01:00) is 7201 real
        // seconds but exactly 2 wall hours. `H` absorbs the leap like a
        // leap day inside a year; `S` counts it.
        let span = "2016-12-31T23:00:00Z/2017-01-01T01:00:00Z";
        assert_eq!(
            derived_period(&parse_lexical_interval(span).unwrap()),
            "PT2H"
        );
        // seconds-based spans count the leap second in the residual
        for (s, want) in [
            ("2016-12-31T23:59:59Z/2017-01-01T00:00:00Z", "PT2S"),
            ("2016-12-31T23:59:59Z/2017-01-01T00:00:01Z", "PT3S"),
            ("2016-12-31T23:00:00Z/2017-01-01T00:00:00Z", "PT1H"),
        ] {
            assert_eq!(
                derived_period(&parse_lexical_interval(s).unwrap()),
                want,
                "{s}"
            );
        }
        // equivalence: a two-hour duration and a 7201-second duration land
        // on the same end instant; 7200 real seconds lands one second short
        let end_of = |s: &str| match &parse_lexical_interval(s).unwrap().end {
            Component::Finite(f) => secs_nanos(f),
            _ => panic!("finite end expected"),
        };
        assert_eq!(end_of("2016-12-31T23:00:00Z/PT2H"), end_of(span));
        assert_eq!(end_of("2016-12-31T23:00:00Z/PT7201S"), end_of(span));
        let (short, _) = end_of("2016-12-31T23:00:00Z/PT7200S");
        let (full, _) = end_of(span);
        assert_eq!(short, full - 1);

        // anchors *on* the leap second keep their real position: `PT1S`
        // lands on the next real instant, not back on itself
        let instant = |c: &Component| match c {
            Component::Finite(f) => secs_nanos(f),
            _ => panic!("finite component expected"),
        };
        let at = |s: &str| instant(&parse_lexical_interval(s).unwrap().start);
        assert_eq!(
            end_of("2016-12-31T23:59:60Z/PT1S"),
            at("2017-01-01T00:00:00Z/2017-01-02")
        );
        // a wall minute carries the position by field arithmetic: the 61st
        // second of 23:59 becomes the normalised 61st second of 00:00
        assert_eq!(
            end_of("2016-12-31T23:59:60Z/PT1M"),
            at("2017-01-01T00:01:00Z/2017-01-02")
        );
        // the leap day's minute 23:59 holds 61 real seconds: `PT60S` from
        // 23:59:00 lands on the leap second, `PT61S` on midnight — the
        // naive "60 s = 00:00" reading would alias the two instants
        assert_eq!(
            end_of("2016-12-31T23:59:00Z/PT60S"),
            at("2016-12-31T23:59:60Z/2017-01-02")
        );
        assert_eq!(
            end_of("2016-12-31T23:59:00Z/PT61S"),
            at("2017-01-01T00:00:00Z/2017-01-02")
        );
        // and the mirror direction: 1 s back from midnight is the leap
        // second itself; 61 s back is 23:59:00 — the minute runs 61 deep
        let start_of = |s: &str| match &parse_lexical_interval(s).unwrap().start {
            Component::Finite(f) => secs_nanos(f),
            _ => panic!("finite start expected"),
        };
        assert_eq!(
            start_of("PT1S/2017-01-01T00:00:00Z"),
            at("2016-12-31T23:59:60Z/2017-01-02")
        );
        assert_eq!(
            start_of("PT60S/2017-01-01T00:00:00Z"),
            at("2016-12-31T23:59:01Z/2017-01-02")
        );
        assert_eq!(
            start_of("PT61S/2017-01-01T00:00:00Z"),
            at("2016-12-31T23:59:00Z/2017-01-02")
        );

        // an end *on* the leap second derives an honest residue: `PT1H`
        // would overshoot, so the wall part is 59 minutes and the real
        // second residue is 60 — `60S` cannot fold into a wall minute here
        for (s, want) in [
            ("2016-12-31T23:59:60Z/2017-01-01T00:00:00Z", "PT1S"),
            ("2016-12-31T23:00:00Z/2016-12-31T23:59:60Z", "PT59M60S"),
            ("2016-12-31T23:59:60Z/2017-01-01T00:01:00Z", "PT1M"),
        ] {
            assert_eq!(
                derived_period(&parse_lexical_interval(s).unwrap()),
                want,
                "{s}"
            );
        }
    }

    #[test]
    fn derived_period_clusters_equal_spans() {
        // Four spellings of [Jan 1, Apr 1): all share start instant, end
        // instant and period bytes "P3M" — ordered by kind/tshift/flag.
        let p_end = entry("P3M/2026-03-31"); // P/T
        let t_dur = entry("2026-01-01/P3M"); // T/P
        let dates = entry("2026-01-01/2026-03-31"); // T/T, date kind
        let times = entry("2026-01-01T00:00:00Z/2026-04-01T00:00:00Z"); // T/T, dt
        let mut v = vec![p_end.clone(), t_dur.clone(), dates.clone(), times.clone()];
        v.sort();
        assert_eq!(v, vec![dates, t_dur, p_end, times]);
        // same resolved span, different period: P90D ≠ P3M1D — the period
        // field keeps them distinct and orders them deterministically.
        let p90 = entry("2025-01-02T00:00:00Z/P90D"); // supplied "P90D"
        let der = entry("2025-01-02T00:00:00Z/2025-04-02T00:00:00Z"); // derived "P3M1D"
        assert_ne!(p90, der);
        assert!(der < p90, "P3M1D < P90D in the period field");
    }

    #[test]
    fn edge_cases() {
        // zero-length instant interval, PT0S spellings, weeks verbatim,
        // epoch-boundary zero instants
        for s in [
            "2025-01-01T00:00:00Z/2025-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z/PT0S",
            "PT0S/2025-01-01T00:00:00Z",
            "2025-01-01/P2W",
            "1970-01-01T00:00:00Z/1970-01-02T00:00:00Z",
            "1969-12-31T00:00:00Z/1969-12-31T00:00:00.000000001Z",
        ] {
            let iv = parse_lexical_interval(s).unwrap();
            let enc = encode_lexical_interval(&iv);
            let dec = decode_lexical_entry(&enc).unwrap();
            assert_eq!(render_lexical_interval(&dec), s, "{s}");
        }

        // the three zero-length spellings of the same instant are adjacent
        let mut v = vec![
            entry("2025-01-01T00:00:00Z/PT0S"),
            entry("PT0S/2025-01-01T00:00:00Z"),
            entry("2025-01-01T00:00:00Z/2025-01-01T00:00:00Z"),
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                entry("2025-01-01T00:00:00Z/2025-01-01T00:00:00Z"),
                entry("2025-01-01T00:00:00Z/PT0S"),
                entry("PT0S/2025-01-01T00:00:00Z"),
            ]
        );
    }

    // -- front-coding / delta efficacy -----------------------------------------

    /// Measure the dictionary front-coding on a realistic common case: many
    /// same-timezone intervals sharing year, zone and shape. Verifies every
    /// entry survives a real block build/parse cycle and prints the raw vs
    /// front-coded sizes.
    ///
    /// Front coding only shares *leading* bytes: with the instant key first
    /// (required for ordering), adjacent starts diverge after ~4–6 bytes, and
    /// the 5-bit payload suffix never participates. The encoding's real space
    /// win over the 52-byte format is variable width + the packed payload —
    /// not dictionary front coding.
    #[test]
    fn delta_encoding_efficacy() {
        use crate::tfc::block::{build_block_unchecked, SizedDictBlock, BLOCK_SIZE};

        let build = |entries: &[Vec<u8>]| -> (usize, usize) {
            let mut sorted = entries.to_vec();
            sorted.sort();
            sorted.dedup();
            let raw: usize = sorted.iter().map(|e| e.len()).sum();
            let mut coded = 0usize;
            let mut buf = BytesMut::new();
            for chunk in sorted.chunks(BLOCK_SIZE) {
                buf.clear();
                let slices: Vec<&[u8]> = chunk.iter().map(|e| e.as_slice()).collect();
                coded += build_block_unchecked(None, &mut buf, &slices);
                // verify the block round-trips every entry
                let mut block_bytes = buf.clone().freeze();
                let block = SizedDictBlock::parse(&mut block_bytes).unwrap();
                for (i, want) in chunk.iter().enumerate() {
                    assert_eq!(
                        block.entry(i).to_vec().as_slice(),
                        want.as_slice(),
                        "block entry {i} lost"
                    );
                }
            }
            (raw, coded)
        };

        // common case: one timezone (+01:00), hourly windows across a quarter,
        // all in explicit T/T form — the shape a real dataset produces.
        let mut common = Vec::new();
        for month in 1..=3u32 {
            for day in 1..=28u32 {
                for hour in [9u32, 12, 15] {
                    common.push(entry(&format!(
                        "2025-{month:02}-{day:02}T{hour:02}:00:00+01:00/2025-{month:02}-{day:02}T{hour:02}:30:00+01:00"
                    )));
                }
            }
        }
        let (raw, coded) = build(&common);
        let pct = 100 - coded * 100 / raw;
        eprintln!(
            "common case ({} intervals): raw {raw}B ({:.1}B/entry), front-coded {coded}B ({:.1}B/entry), saved {pct}%",
            common.len(),
            raw as f64 / common.len() as f64,
            coded as f64 / common.len() as f64
        );
        assert!(
            coded < raw,
            "front coding saves something in the common case"
        );

        // adverse case: scattered years, zones and forms — less shared
        // structure for front coding to exploit.
        let mut mixed = Vec::new();
        for i in 0..40u32 {
            let day = i % 28 + 1;
            for s in [
                format!(
                    "-{}-06-{:02}T06:30:00Z/-{}-06-{:02}T08:00:00Z",
                    39000 + i,
                    day,
                    39000 + i,
                    day
                ),
                format!(
                    "2025-{:02}-{:02}T00:00:00.{}Z/P1M",
                    i % 12 + 1,
                    day,
                    100000000 + i
                ),
                format!(
                    "2024-{:02}-{:02}/2024-{:02}-{:02}",
                    i % 12 + 1,
                    day,
                    i % 12 + 1,
                    day + 1
                ),
                format!("P{}W/2025-06-30", i % 4 + 1),
                format!(
                    "1999-12-31T23:{:02}:59-14:00/2000-01-01T00:00:01+14:00",
                    i % 60
                ),
                format!("2025-{:02}-{:02}/X*", i % 12 + 1, day),
                format!("../2026-{:02}-{:02}", i % 12 + 1, day),
            ] {
                mixed.push(entry(&s));
            }
        }
        let (raw_m, coded_m) = build(&mixed);
        let pct_m = 100 - coded_m * 100 / raw_m;
        eprintln!(
            "mixed case ({} intervals): raw {raw_m}B ({:.1}B/entry), front-coded {coded_m}B ({:.1}B/entry), saved {pct_m}%",
            mixed.len(),
            raw_m as f64 / mixed.len() as f64,
            coded_m as f64 / mixed.len() as f64
        );
    }

    // -- legacy conversion ---------------------------------------------------

    fn legacy_entry(s: &str) -> TypedDictEntry {
        use crate::tfc::datatypes::TdbDataType;
        DateTimeInterval::make_entry(&crate::tfc::interval::parse_iso_interval(s).unwrap())
    }

    fn legacy_dti(s: &str) -> DateTimeInterval {
        DateTimeInterval::from_lexical(legacy_entry(s).to_bytes())
    }

    #[test]
    fn legacy_explicit_pair_renders_identically() {
        let iv =
            legacy_interval_to_lexical(&legacy_dti("2025-01-01T10:30:00Z/2025-03-31T12:45:00Z"));
        assert_eq!(
            "2025-01-01T10:30:00Z/2025-03-31T12:45:00Z",
            render_lexical_interval(&iv)
        );
    }

    #[test]
    fn legacy_offset_zone_round_trips() {
        let iv = legacy_interval_to_lexical(&legacy_dti(
            "2025-01-01T10:30:00+01:00/2025-03-31T12:45:00+01:00",
        ));
        assert_eq!(
            "2025-01-01T10:30:00+01:00/2025-03-31T12:45:00+01:00",
            render_lexical_interval(&iv)
        );
    }

    #[test]
    fn legacy_date_pair_renders_dates() {
        let iv = legacy_interval_to_lexical(&legacy_dti("2025-01-01/2025-04-01"));
        assert_eq!("2025-01-01/2025-04-01", render_lexical_interval(&iv));
    }

    #[test]
    fn legacy_duration_forms_keep_period_side() {
        let iv = legacy_interval_to_lexical(&legacy_dti("2025-01-01/P3M"));
        assert_eq!("2025-01-01/P3M", render_lexical_interval(&iv));
        let iv = legacy_interval_to_lexical(&legacy_dti("P3M/2025-03-31"));
        assert_eq!("P3M/2025-03-31", render_lexical_interval(&iv));
    }

    #[test]
    fn legacy_special_endpoints_convert() {
        let iv = legacy_interval_to_lexical(&legacy_dti("../2025-01-01"));
        assert_eq!("../2025-01-01", render_lexical_interval(&iv));
        let iv = legacy_interval_to_lexical(&legacy_dti("2025-01-01/.."));
        assert_eq!("2025-01-01/..", render_lexical_interval(&iv));
        let iv = legacy_interval_to_lexical(&legacy_dti("2025-01-01/"));
        assert_eq!("2025-01-01/", render_lexical_interval(&iv));
    }

    #[test]
    fn posix_to_leap_aware_secs_boundaries() {
        // pre-1972: no leap seconds at all
        assert_eq!(0, posix_to_leap_aware_secs(0));
        // the 2016 leap day's own midnight: 26 prior leap days
        assert_eq!(1_483_142_400 + 26, posix_to_leap_aware_secs(1_483_142_400));
        // 2017-01-01 midnight: all 27
        assert_eq!(1_483_228_800 + 27, posix_to_leap_aware_secs(1_483_228_800));
    }

    #[test]
    fn upgrade_lexical_intervals_converts_fixed_width() {
        let mut entries = vec![
            legacy_entry("2025-01-01T10:00:00Z/2025-03-31T12:00:00Z"),
            legacy_entry("2025-04-01T10:00:00Z/2025-06-30T12:00:00Z"),
        ];
        entries.sort();
        let dict = build_dict(entries);
        let dict = dict.upgrade_lexical_intervals();
        // every segment entry now decodes as lexical
        let seg = dict.lexical_interval_segment().unwrap();
        for id in seg.clone() {
            let e = dict.entry(id as usize).unwrap();
            decode_lexical_entry(&e.to_bytes()).expect("upgraded entry decodes as lexical");
        }
        // probes and end index work on the upgraded dictionary
        let s15 = posix_to_leap_aware_secs(1_737_417_600); // 2025-02-15 UTC
        let r = dict.lexical_ids_start_le(1_737_417_600, 0);
        assert_eq!(seg.start + 1, r.finite.end);
        let _ = s15;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lexical_end_index_files_round_trip() {
        use crate::bitindex::BitIndex;
        use crate::storage::memory::MemoryBackedStore;
        use crate::storage::types::FileLoad;
        let dict = build_dict(vec![
            iv_entry("2025-01-01T10:00:00Z/2025-03-31T12:00:00Z"),
            iv_entry("2025-04-01T10:00:00Z/2025-06-30T12:00:00Z"),
            iv_entry("2025-07-01T10:00:00Z/2025-09-30T12:00:00Z"),
        ]);
        let ends = MemoryBackedStore::new();
        let bits = MemoryBackedStore::new();
        let blocks = MemoryBackedStore::new();
        let sblocks = MemoryBackedStore::new();
        build_interval_end_index_lexical(
            &dict,
            ends.clone(),
            bits.clone(),
            blocks.clone(),
            sblocks.clone(),
        )
        .await
        .unwrap();
        let idx = IntervalEndIndex::from_parts(
            crate::logarray::LogArray::parse(ends.map().await.unwrap()).unwrap(),
            BitIndex::from_maps(
                bits.map().await.unwrap(),
                blocks.map().await.unwrap(),
                sblocks.map().await.unwrap(),
            ),
        );
        assert_eq!(3, idx.num_distinct_ends());
        // bound in the leap-aware domain: 2025-05-01 UTC
        let bound = posix_to_leap_aware_secs(1_746_057_600);
        assert_eq!(vec![0], idx.positions_end_le(0..3, bound, 0));
    }

    #[test]
    fn allen_relations_constraint_table() {
        use AllenRelation::*;
        // explicit datetime ends (bare `date` ends are inclusive and bump a day)
        let a = parse_lexical_interval("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z").unwrap();
        let b = parse_lexical_interval("2025-04-01T00:00:00Z/2025-07-01T00:00:00Z").unwrap();
        let c = parse_lexical_interval("2025-02-01T00:00:00Z/2025-03-01T00:00:00Z").unwrap();
        let d = parse_lexical_interval("2025-03-01T00:00:00Z/2025-06-01T00:00:00Z").unwrap();
        let a2 = parse_lexical_interval("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z").unwrap();
        let tail = parse_lexical_interval("2025-03-01T00:00:00Z/2025-04-01T00:00:00Z").unwrap();
        let head = parse_lexical_interval("2025-01-01T00:00:00Z/2025-02-01T00:00:00Z").unwrap();
        assert!(interval_relation(&a, Meets, &b));
        assert!(interval_relation(&b, MetBy, &a));
        assert!(!interval_relation(&a, Before, &b)); // meets, not before
        assert!(interval_relation(
            &a,
            Before,
            &parse_lexical_interval("2025-04-02T00:00:00Z/2025-05-01T00:00:00Z").unwrap()
        ));
        assert!(interval_relation(&a, Overlaps, &d));
        assert!(interval_relation(&d, OverlappedBy, &a));
        assert!(interval_relation(&head, Starts, &a));
        assert!(interval_relation(&a, StartedBy, &head));
        assert!(interval_relation(&c, During, &a));
        assert!(interval_relation(&a, Contains, &c));
        assert!(interval_relation(&tail, Finishes, &a));
        assert!(interval_relation(&a, FinishedBy, &tail));
        assert!(interval_relation(&a, Equals, &a2));
        assert!(!interval_relation(&a, Equals, &b));
        // starts is not during: shared-start sub-interval fails strict during
        assert!(!interval_relation(&head, During, &a));
        // meets is not overlaps
        assert!(!interval_relation(&a, Overlaps, &b));
    }

    #[test]
    fn allen_relations_semantic_instant_equality() {
        // Same instant, different spellings: meets across zone/precision.
        let a = parse_lexical_interval("2025-01-01T00:00:00Z/2025-04-01T00:00:00.50Z").unwrap();
        let b = parse_lexical_interval("2025-04-01T02:00:00.5+02:00/2025-07-01T00:00:00Z").unwrap();
        assert!(interval_relation(&a, AllenRelation::Meets, &b));
        assert!(!interval_relation(&a, AllenRelation::Before, &b));
    }

    #[test]
    fn allen_relations_point_operands() {
        use AllenRelation::*;
        let a = parse_lexical_interval("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z").unwrap();
        // a dateTime literal coerces to the point [t,t)
        let p = literal_as_interval("2025-02-01T00:00:00Z").unwrap();
        assert!(interval_relation(&p, During, &a));
        assert!(interval_relation(&a, Contains, &p));
        // a bare `date` literal coerces to the day interval [day, day+1), not a point
        let day = literal_as_interval("2025-02-01").unwrap();
        assert!(!Bound::eq(&Bound::start(&day.start), &Bound::end(&day.end)));
        // the point sorting: i1, t, i2 — a point at a meeting boundary belongs
        // to the interval it starts
        let s = literal_as_interval("2025-01-01T00:00:00Z").unwrap();
        assert!(interval_relation(&s, During, &a)); // t == a.start: member
        let e = literal_as_interval("2025-04-01T00:00:00Z").unwrap();
        assert!(!interval_relation(&e, During, &a)); // t == a.end: not a's
        assert!(interval_relation(
            &e,
            During,
            &parse_lexical_interval("2025-04-01T00:00:00Z/2025-07-01T00:00:00Z").unwrap()
        ));
        assert!(interval_relation(&e, MetBy, &a));
        // points never overlap/start/finish; equal points are Equals
        assert!(!interval_relation(&p, Overlaps, &a));
        assert!(interval_relation(
            &p,
            Equals,
            &literal_as_interval("2025-02-01T00:00:00Z").unwrap()
        ));
        assert!(interval_relation(
            &literal_as_interval("2025-05-01T00:00:00Z").unwrap(),
            Before,
            &literal_as_interval("2025-06-01T00:00:00Z").unwrap()
        ));
        // an interval can't be during a point
        assert!(!interval_relation(&a, During, &e));
    }

    #[test]
    fn allen_relations_special_endpoints() {
        use AllenRelation::*;
        let a = parse_lexical_interval("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z").unwrap();
        // ../end shares a's end but starts at -inf: a finished_by it
        let open_start = parse_lexical_interval("../2025-04-01T00:00:00Z").unwrap();
        assert!(interval_relation(&a, Finishes, &open_start));
        assert!(interval_relation(&open_start, FinishedBy, &a));
        assert!(!interval_relation(&a, During, &open_start)); // shared end: not strictly inside
                                                              // a meets an open-end interval starting at a's end
        let open_end = parse_lexical_interval("2025-04-01T00:00:00Z/..").unwrap();
        assert!(interval_relation(&a, Meets, &open_end));
        // and open_end is met-by a (a meets open_end — inverse)
        assert!(interval_relation(&open_end, MetBy, &a));
        // but +inf meets/befores nothing: open_end's own end can never equal a start
        assert!(!interval_relation(&open_end, Meets, &a));
        assert!(!interval_relation(&open_end, Before, &a));
        // unknown endpoints are NULL: every relation false
        let unk = parse_lexical_interval("/2025-04-01").unwrap();
        for rel in [Before, Meets, Overlaps, Starts, During, Finishes, Equals] {
            assert!(!interval_relation(&unk, rel, &a), "{rel:?}");
        }
        // two unknowns never equal
        let unk2 = parse_lexical_interval("/2025-06-01").unwrap();
        assert!(!interval_relation(&unk, Equals, &unk2));
    }
}
