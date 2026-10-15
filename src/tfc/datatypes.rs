use super::{
    datetime::{datetime_to_storage, storage_to_datetime},
    decimal::{decimal_to_storage, storage_to_decimal, Decimal},
    integer::{bigint_to_storage, storage_to_bigint},
    TypedDictEntry,
};
use base64::display::Base64Display;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use num_derive::FromPrimitive;
use rug::Integer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, FromPrimitive, Hash)]
pub enum Datatype {
    String = 0,
    UInt32,
    Int32,
    Float32,
    UInt64,
    Int64,
    Float64,
    Decimal,
    BigInt,
    Boolean,
    LangString,
    AnyURI,
    Language,
    NormalizedString,
    Token,
    NMToken,
    Name,
    NCName,
    Notation,
    QName,
    ID,
    IDRef,
    Entity,
    PositiveInteger,
    NonNegativeInteger,
    NonPositiveInteger,
    NegativeInteger,
    Date,
    DateTime,
    DateTimeStamp,
    Time,
    GYear,
    GMonth,
    GDay,
    GYearMonth,
    GMonthDay,
    Duration,
    YearMonthDuration,
    DayTimeDuration,
    UInt8,
    Int8,
    UInt16,
    Int16,
    Base64Binary,
    HexBinary,
    AnySimpleType,
    DateTimeInterval,
}

impl Datatype {
    pub fn cast<T: TdbDataType, B: Buf>(self, b: B) -> T {
        if T::datatype() != self {
            panic!("not the right datatype");
        }

        T::from_lexical(b)
    }

    pub fn record_size(&self) -> Option<u8> {
        match self {
            Datatype::Boolean => None,
            Datatype::String => None,
            Datatype::UInt32 => Some(4),
            Datatype::Int32 => Some(4),
            Datatype::UInt64 => Some(8),
            Datatype::Int64 => Some(8),
            Datatype::Float32 => Some(4),
            Datatype::Float64 => Some(8),
            Datatype::Decimal => None,
            Datatype::BigInt => None,
            Datatype::Token => None,
            Datatype::LangString => None,
            Datatype::DateTimeInterval => None,
            _ => None,
        }
    }
}

pub trait TdbDataType: FromLexical<Self> + ToLexical<Self> {
    fn datatype() -> Datatype;

    fn make_entry<T>(val: &T) -> TypedDictEntry
    where
        T: ToLexical<Self> + ?Sized,
    {
        TypedDictEntry::new(Self::datatype(), val.to_lexical().into())
    }
}

pub trait ToLexical<T: ?Sized> {
    fn to_lexical(&self) -> Bytes;
}

pub trait FromLexical<T: ?Sized> {
    fn from_lexical<B: Buf>(b: B) -> Self;
}

impl<T: AsRef<str>> ToLexical<String> for T {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_ref().as_bytes())
    }
}

impl FromLexical<String> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec).unwrap()
    }
}

impl TdbDataType for String {
    fn datatype() -> Datatype {
        Datatype::String
    }
}

impl TdbDataType for u8 {
    fn datatype() -> Datatype {
        Datatype::UInt8
    }
}

impl FromLexical<u8> for u8 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u8().unwrap()
    }
}

impl ToLexical<u8> for u8 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u8(*self).unwrap();

        buf.into_inner().freeze()
    }
}

impl TdbDataType for u16 {
    fn datatype() -> Datatype {
        Datatype::UInt16
    }
}

impl FromLexical<u16> for u16 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u16::<BigEndian>().unwrap()
    }
}

impl ToLexical<u16> for u16 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u16::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

impl TdbDataType for u32 {
    fn datatype() -> Datatype {
        Datatype::UInt32
    }
}

impl FromLexical<u16> for u32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u16::<BigEndian>().unwrap() as u32
    }
}

impl FromLexical<u32> for u32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u32::<BigEndian>().unwrap()
    }
}

impl ToLexical<u32> for u32 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I8_BYTE_MASK: u8 = 0b1000_0000;
impl TdbDataType for i8 {
    fn datatype() -> Datatype {
        Datatype::Int8
    }
}

impl FromLexical<i8> for i8 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u8().unwrap();
        (I8_BYTE_MASK ^ i) as i8
    }
}

impl ToLexical<i8> for i8 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I8_BYTE_MASK ^ (*self as u8);
        let mut buf = BytesMut::new().writer();
        buf.write_u8(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const I16_BYTE_MASK: u16 = 0b1000_0000 << 8;
impl TdbDataType for i16 {
    fn datatype() -> Datatype {
        Datatype::Int16
    }
}

impl FromLexical<i16> for i16 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u16::<BigEndian>().unwrap();
        (I16_BYTE_MASK ^ i) as i16
    }
}

impl ToLexical<i16> for i16 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I16_BYTE_MASK ^ (*self as u16);
        let mut buf = BytesMut::new().writer();
        buf.write_u16::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const I32_BYTE_MASK: u32 = 0b1000_0000 << (3 * 8);
impl TdbDataType for i32 {
    fn datatype() -> Datatype {
        Datatype::Int32
    }
}

impl FromLexical<i16> for i32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_i16::<BigEndian>().unwrap() as i32
    }
}

impl FromLexical<u16> for i32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u16::<BigEndian>().unwrap() as i32
    }
}

impl FromLexical<i32> for i32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        (I32_BYTE_MASK ^ i) as i32
    }
}

impl ToLexical<u8> for i32 {
    fn to_lexical(&self) -> Bytes {
        <i32 as ToLexical<i32>>::to_lexical(&self as &i32)
    }
}

impl ToLexical<u16> for i32 {
    fn to_lexical(&self) -> Bytes {
        <i32 as ToLexical<i32>>::to_lexical(&self as &i32)
    }
}

impl ToLexical<i8> for i32 {
    fn to_lexical(&self) -> Bytes {
        <i32 as ToLexical<i32>>::to_lexical(&self as &i32)
    }
}

impl ToLexical<i16> for i32 {
    fn to_lexical(&self) -> Bytes {
        <i32 as ToLexical<i32>>::to_lexical(&self as &i32)
    }
}

impl ToLexical<i32> for i32 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I32_BYTE_MASK ^ (*self as u32);
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for u64 {
    fn datatype() -> Datatype {
        Datatype::UInt64
    }
}

impl FromLexical<u64> for u64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u64::<BigEndian>().unwrap()
    }
}

impl ToLexical<u64> for u64 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I64_BYTE_MASK: u64 = 0b1000_0000 << (7 * 8);
impl TdbDataType for i64 {
    fn datatype() -> Datatype {
        Datatype::Int64
    }
}

impl FromLexical<i64> for i64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        (I64_BYTE_MASK ^ i) as i64
    }
}

impl ToLexical<i64> for i64 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I64_BYTE_MASK ^ (*self as u64);
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const F32_SIGN_MASK: u32 = 0x8000_0000;
const F32_COMPLEMENT: u32 = 0xffff_ffff;
impl TdbDataType for f32 {
    fn datatype() -> Datatype {
        Datatype::Float32
    }
}

impl FromLexical<f32> for f32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        if i & F32_SIGN_MASK > 0 {
            f32::from_bits(i ^ F32_SIGN_MASK)
        } else {
            f32::from_bits(i ^ F32_COMPLEMENT)
        }
    }
}

impl FromLexical<f32> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        f32::from_lexical(b) as f64
    }
}

impl ToLexical<f32> for f32 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u32 = if f.signum() == -1.0 {
            f.to_bits() ^ F32_COMPLEMENT
        } else {
            f.to_bits() ^ F32_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

const F64_SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const F64_COMPLEMENT: u64 = 0xffff_ffff_ffff_ffff;
impl TdbDataType for f64 {
    fn datatype() -> Datatype {
        Datatype::Float64
    }
}

impl FromLexical<f64> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        if i & F64_SIGN_MASK > 0 {
            f64::from_bits(i ^ F64_SIGN_MASK)
        } else {
            f64::from_bits(i ^ F64_COMPLEMENT)
        }
    }
}

impl ToLexical<f64> for f64 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u64 = if f.signum() == -1.0 {
            f.to_bits() ^ F64_COMPLEMENT
        } else {
            f.to_bits() ^ F64_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for Integer {
    fn datatype() -> Datatype {
        Datatype::BigInt
    }
}

impl FromLexical<Integer> for Integer {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_bigint(&mut b)
    }
}

impl FromLexical<Integer> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        // TODO make this better
        storage_to_bigint(&mut b).to_string()
    }
}

impl ToLexical<Integer> for Integer {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(bigint_to_storage(self.clone()))
    }
}

impl TdbDataType for Decimal {
    fn datatype() -> Datatype {
        Datatype::Decimal
    }
}

impl FromLexical<Decimal> for Decimal {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        Decimal(storage_to_decimal(&mut b))
    }
}

impl FromLexical<Decimal> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        // TODO make this better
        Decimal::from_lexical(b).0
    }
}

impl ToLexical<Decimal> for Decimal {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(decimal_to_storage(&self.0))
    }
}

impl TdbDataType for bool {
    fn datatype() -> Datatype {
        Datatype::Boolean
    }
}

impl FromLexical<bool> for bool {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let num = b.get_u8();
        num != 0
    }
}

impl ToLexical<bool> for bool {
    fn to_lexical(&self) -> Bytes {
        if *self {
            vec![1].into()
        } else {
            vec![0].into()
        }
    }
}

impl TdbDataType for NaiveDateTime {
    fn datatype() -> Datatype {
        Datatype::DateTime
    }
}

impl ToLexical<NaiveDateTime> for NaiveDateTime {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(datetime_to_storage(self))
    }
}

impl FromLexical<NaiveDateTime> for NaiveDateTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_datetime(&mut b)
    }
}

impl FromLexical<NaiveDateTime> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let ndt = storage_to_datetime(&mut b);
        ndt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
    }
}

pub struct DateTimeStamp(pub NaiveDateTime);

impl TdbDataType for DateTimeStamp {
    fn datatype() -> Datatype {
        Datatype::DateTimeStamp
    }
}

impl ToLexical<DateTimeStamp> for DateTimeStamp {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(datetime_to_storage(&self.0))
    }
}

impl FromLexical<DateTimeStamp> for DateTimeStamp {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        DateTimeStamp(storage_to_datetime(&mut b))
    }
}

impl FromLexical<DateTimeStamp> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let ndt = storage_to_datetime(&mut b);
        ndt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
    }
}

impl TdbDataType for NaiveTime {
    fn datatype() -> Datatype {
        Datatype::Time
    }
}

impl ToLexical<NaiveTime> for NaiveTime {
    fn to_lexical(&self) -> Bytes {
        self.to_string().into()
    }
}

impl FromLexical<NaiveTime> for NaiveTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec)
            .unwrap()
            .parse::<NaiveTime>()
            .unwrap()
    }
}

impl FromLexical<NaiveTime> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let ndt = NaiveTime::from_lexical(&mut b);
        ndt.format("%H:%M:%S%.fZ").to_string()
    }
}

pub struct Date {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for Date {
    fn datatype() -> Datatype {
        Datatype::Date
    }
}

impl ToLexical<Date> for Date {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let month = self.month.to_lexical();
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [year, month, day, offset].concat().into()
    }
}

impl FromLexical<Date> for Date {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let month = u8::from_lexical(&mut b);
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        Date {
            year,
            month,
            day,
            offset,
        }
    }
}

impl FromLexical<Date> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let date = Date::from_lexical(b);
        let year = date.year;
        let month = date.month;
        let day = date.day;
        let offset = offset_string(date.offset);
        format!("{year:04}-{month:02}-{day:02}{offset:}")
    }
}

pub struct GYear {
    pub year: i64,
    pub offset: i16,
}

impl TdbDataType for GYear {
    fn datatype() -> Datatype {
        Datatype::GYear
    }
}

impl ToLexical<GYear> for GYear {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let offset = self.offset.to_lexical();
        [year, offset].concat().into()
    }
}

impl FromLexical<GYear> for GYear {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GYear { year, offset }
    }
}

fn offset_string(offset: i16) -> String {
    if offset == 0 {
        "".to_string()
    } else {
        let hours = offset / 60;
        let minutes = offset % 60;
        if hours < 0 {
            format!("-{hours:02}:{minutes:02}")
        } else {
            format!("+{hours:02}:{minutes:02}")
        }
    }
}

impl FromLexical<GYear> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gyear = GYear::from_lexical(b);
        let year = gyear.year;
        let offset = offset_string(gyear.offset);
        format!("{year:04}{offset:}")
    }
}

pub struct GMonth {
    pub month: u8,
    pub offset: i16,
}

impl TdbDataType for GMonth {
    fn datatype() -> Datatype {
        Datatype::GMonth
    }
}

impl ToLexical<GMonth> for GMonth {
    fn to_lexical(&self) -> Bytes {
        let month = self.month.to_lexical();
        let offset = self.offset.to_lexical();
        [month, offset].concat().into()
    }
}

impl FromLexical<GMonth> for GMonth {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let month = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GMonth { month, offset }
    }
}

impl FromLexical<GMonth> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gmonth = GMonth::from_lexical(b);
        let month = gmonth.month;
        let offset = offset_string(gmonth.offset);
        format!("--{month:02}{offset:}")
    }
}

pub struct GDay {
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for GDay {
    fn datatype() -> Datatype {
        Datatype::GDay
    }
}

impl ToLexical<GDay> for GDay {
    fn to_lexical(&self) -> Bytes {
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [day, offset].concat().into()
    }
}

impl FromLexical<GDay> for GDay {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GDay { day, offset }
    }
}

impl FromLexical<GDay> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gday = GDay::from_lexical(b);
        let day = gday.day;
        let offset = offset_string(gday.offset);
        format!("---{day:02}{offset:}")
    }
}

pub struct GYearMonth {
    pub year: i64,
    pub month: u8,
    pub offset: i16,
}

impl TdbDataType for GYearMonth {
    fn datatype() -> Datatype {
        Datatype::GYearMonth
    }
}

impl ToLexical<GYearMonth> for GYearMonth {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let month = self.month.to_lexical();
        let offset = self.offset.to_lexical();
        [year, month, offset].concat().into()
    }
}

impl FromLexical<GYearMonth> for GYearMonth {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let month = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GYearMonth {
            year,
            month,
            offset,
        }
    }
}

impl FromLexical<GYearMonth> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gyearmonth = GYearMonth::from_lexical(b);
        let year = gyearmonth.year;
        let month = gyearmonth.month;
        let offset = offset_string(gyearmonth.offset);
        format!("{year:04}-{month:02}{offset:}")
    }
}

pub struct GMonthDay {
    pub month: u8,
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for GMonthDay {
    fn datatype() -> Datatype {
        Datatype::GMonthDay
    }
}

impl ToLexical<GMonthDay> for GMonthDay {
    fn to_lexical(&self) -> Bytes {
        let month = self.month.to_lexical();
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [month, day, offset].concat().into()
    }
}

impl FromLexical<GMonthDay> for GMonthDay {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let month = u8::from_lexical(&mut b);
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GMonthDay { month, day, offset }
    }
}

impl FromLexical<GMonthDay> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gmonthday = GMonthDay::from_lexical(b);
        let month = gmonthday.month;
        let day = gmonthday.day;
        let offset = offset_string(gmonthday.offset);
        format!("--{month:02}-{day:02}{offset:}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Duration {
    pub sign: i8,
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: f64,
}

impl TdbDataType for Duration {
    fn datatype() -> Datatype {
        Datatype::Duration
    }
}

impl ToLexical<Duration> for Duration {
    fn to_lexical(&self) -> Bytes {
        let sign = self.sign.to_lexical();
        let year = self.year.to_lexical();
        let month = self.month.to_lexical();
        let day = self.day.to_lexical();
        let hour = self.hour.to_lexical();
        let minute = self.minute.to_lexical();
        let second = self.second.to_lexical();
        [sign, year, month, day, hour, minute, second]
            .concat()
            .into()
    }
}

impl FromLexical<Duration> for Duration {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let sign = i8::from_lexical(&mut b);
        let year = i64::from_lexical(&mut b);
        let month = u8::from_lexical(&mut b);
        let day = u8::from_lexical(&mut b);
        let hour = u8::from_lexical(&mut b);
        let minute = u8::from_lexical(&mut b);
        let second: f64 = <f64 as FromLexical<f64>>::from_lexical(&mut b);
        Duration {
            sign,
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }
}

fn duration_string(duration: &Duration) -> String {
    let year = if duration.year != 0 {
        format!("{}Y", duration.year)
    } else {
        "".to_string()
    };
    let month = if duration.month != 0 {
        format!("{}M", duration.month)
    } else {
        "".to_string()
    };
    let day = if duration.day != 0 {
        format!("{}D", duration.day)
    } else {
        "".to_string()
    };
    if duration.hour == 0 && duration.minute == 0 && duration.second == 0.0 {
        format!("P{year}{month}{day}")
    } else {
        let hour = if duration.hour != 0 {
            format!("{}H", duration.hour)
        } else {
            "".to_string()
        };
        let minute = if duration.minute != 0 {
            format!("{}M", duration.minute)
        } else {
            "".to_string()
        };
        let second = if duration.second != 0.0 {
            format!("{}S", duration.second)
        } else {
            "".to_string()
        };
        format!("P{year}{month}{day}T{hour}{minute}{second}")
    }
}

impl FromLexical<Duration> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let duration = Duration::from_lexical(b);
        duration_string(&duration)
    }
}

pub struct YearMonthDuration(pub Duration);

impl TdbDataType for YearMonthDuration {
    fn datatype() -> Datatype {
        Datatype::YearMonthDuration
    }
}

impl ToLexical<YearMonthDuration> for YearMonthDuration {
    fn to_lexical(&self) -> Bytes {
        Duration::to_lexical(&self.0)
    }
}

impl FromLexical<YearMonthDuration> for YearMonthDuration {
    fn from_lexical<B: Buf>(b: B) -> Self {
        YearMonthDuration(Duration::from_lexical(b))
    }
}

impl FromLexical<YearMonthDuration> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let duration = Duration::from_lexical(b);
        duration_string(&duration)
    }
}

pub struct DayTimeDuration(pub Duration);

impl TdbDataType for DayTimeDuration {
    fn datatype() -> Datatype {
        Datatype::DayTimeDuration
    }
}

impl ToLexical<DayTimeDuration> for DayTimeDuration {
    fn to_lexical(&self) -> Bytes {
        Duration::to_lexical(&self.0)
    }
}

impl FromLexical<DayTimeDuration> for DayTimeDuration {
    fn from_lexical<B: Buf>(b: B) -> Self {
        DayTimeDuration(Duration::from_lexical(b))
    }
}

impl FromLexical<DayTimeDuration> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let duration = Duration::from_lexical(b);
        duration_string(&duration)
    }
}

pub struct Base64Binary(pub Vec<u8>);

impl ToLexical<Base64Binary> for Base64Binary {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0[..])
    }
}

impl FromLexical<Base64Binary> for Base64Binary {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        Base64Binary(vec)
    }
}

impl FromLexical<Base64Binary> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        let wrapper = Base64Display::new(&vec, &base64::engine::general_purpose::STANDARD);
        format!("{wrapper}")
    }
}

impl TdbDataType for Base64Binary {
    fn datatype() -> Datatype {
        Datatype::Base64Binary
    }
}

pub struct HexBinary(pub Vec<u8>);

impl ToLexical<HexBinary> for HexBinary {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0[..])
    }
}

impl FromLexical<HexBinary> for HexBinary {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        HexBinary(vec)
    }
}

impl FromLexical<HexBinary> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        hex::encode(vec)
    }
}

impl TdbDataType for HexBinary {
    fn datatype() -> Datatype {
        Datatype::HexBinary
    }
}

/// Component type tag for DateTimeInterval endpoints.
/// 0 = xsd:date, 1 = xsd:dateTime
pub const INTERVAL_COMPONENT_DATE: u8 = 0;
pub const INTERVAL_COMPONENT_DATETIME: u8 = 1;

/// Flag values for DateTimeInterval original input form.
/// 0 = explicit (user gave start/end), 1 = start+duration, 2 = duration+end
pub const INTERVAL_FLAG_EXPLICIT: u8 = 0;
pub const INTERVAL_FLAG_START_DURATION: u8 = 1;
pub const INTERVAL_FLAG_DURATION_END: u8 = 2;

/// A date-time interval with materialized start/end, preserved duration, and
/// a flag recording the original input form.
///
/// Binary encoding (48 bytes fixed-width):
/// - Sorting prefix (24 bytes): start_seconds(i64) + start_nanos(u32) + end_seconds(i64) + end_nanos(u32)
/// - Metadata suffix (24 bytes): start_type(u8) + end_type(u8) + flag(u8) + duration(21 bytes)
#[derive(Debug, Clone, PartialEq)]
pub struct DateTimeInterval {
    pub start_seconds: i64,
    pub start_nanos: u32,
    pub end_seconds: i64,
    pub end_nanos: u32,
    pub start_type: u8,
    pub end_type: u8,
    pub flag: u8,
    pub duration: Duration,
}

impl TdbDataType for DateTimeInterval {
    fn datatype() -> Datatype {
        Datatype::DateTimeInterval
    }
}

impl ToLexical<DateTimeInterval> for DateTimeInterval {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(48);
        // Sorting prefix: start timestamp (12 bytes) + end timestamp (12 bytes)
        buf.extend_from_slice(&self.start_seconds.to_lexical());
        buf.put_u32(self.start_nanos);
        buf.extend_from_slice(&self.end_seconds.to_lexical());
        buf.put_u32(self.end_nanos);
        // Metadata suffix: types + flag + duration
        buf.put_u8(self.start_type);
        buf.put_u8(self.end_type);
        buf.put_u8(self.flag);
        buf.extend_from_slice(&self.duration.to_lexical());
        buf.freeze()
    }
}

impl FromLexical<DateTimeInterval> for DateTimeInterval {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let start_seconds = i64::from_lexical(&mut b);
        let start_nanos = b.get_u32();
        let end_seconds = i64::from_lexical(&mut b);
        let end_nanos = b.get_u32();
        let start_type = b.get_u8();
        let end_type = b.get_u8();
        let flag = b.get_u8();
        let duration = Duration::from_lexical(&mut b);
        DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag,
            duration,
        }
    }
}

fn interval_component_to_iso(seconds: i64, nanos: u32) -> String {
    let ndt = DateTime::from_timestamp(seconds, nanos)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|| {
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        });
    if nanos == 0 {
        ndt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        ndt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
    }
}

fn interval_duration_to_iso(duration: &Duration) -> String {
    let s = duration_string(duration);
    if duration.sign < 0 {
        format!("-{s}")
    } else {
        s
    }
}

impl FromLexical<DateTimeInterval> for String {
    fn from_lexical<B: Buf>(b: B) -> String {
        let iv = DateTimeInterval::from_lexical(b);
        let dur_str = interval_duration_to_iso(&iv.duration);
        match iv.flag {
            INTERVAL_FLAG_START_DURATION => {
                let start = interval_component_to_iso(iv.start_seconds, iv.start_nanos);
                format!("{start}/{dur_str}")
            }
            INTERVAL_FLAG_DURATION_END => {
                let end = interval_component_to_iso(iv.end_seconds, iv.end_nanos);
                format!("{dur_str}/{end}")
            }
            _ => {
                let start = interval_component_to_iso(iv.start_seconds, iv.start_nanos);
                let end = interval_component_to_iso(iv.end_seconds, iv.end_nanos);
                format!("{start}/{end}")
            }
        }
    }
}

macro_rules! stringy_type {
    ($ty:ident) => {
        stringy_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(String);

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl<T: AsRef<str>> ToLexical<$ty> for T {
            fn to_lexical(&self) -> Bytes {
                Bytes::copy_from_slice(self.as_ref().as_bytes())
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                $ty(String::from_utf8(vec).unwrap())
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                String::from_utf8(vec).unwrap()
            }
        }
    };
}

macro_rules! biginty_type {
    ($ty:ident) => {
        biginty_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(pub Integer);

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                $ty(storage_to_bigint(&mut b))
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                storage_to_bigint(&mut b).to_string()
            }
        }

        impl FromLexical<$ty> for Integer {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                storage_to_bigint(&mut b)
            }
        }

        impl ToLexical<$ty> for $ty {
            fn to_lexical(&self) -> Bytes {
                Bytes::from(bigint_to_storage(self.0.clone()))
            }
        }
    };
}

stringy_type!(LangString);
stringy_type!(NCName);
stringy_type!(Name);
stringy_type!(Token);
stringy_type!(NMToken);
stringy_type!(NormalizedString);
stringy_type!(Language);
stringy_type!(AnyURI);
stringy_type!(Notation);
stringy_type!(QName);
stringy_type!(ID);
stringy_type!(IDRef);
stringy_type!(Entity);

stringy_type!(AnySimpleType);

biginty_type!(PositiveInteger);
biginty_type!(NonNegativeInteger);
biginty_type!(NegativeInteger);
biginty_type!(NonPositiveInteger);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tfc::typed::TypedDictEntry;

    fn make_duration(
        sign: i8,
        year: i64,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: f64,
    ) -> Duration {
        Duration {
            sign,
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }

    fn make_interval(
        start_seconds: i64,
        start_nanos: u32,
        end_seconds: i64,
        end_nanos: u32,
        start_type: u8,
        end_type: u8,
        flag: u8,
        duration: Duration,
    ) -> DateTimeInterval {
        DateTimeInterval {
            start_seconds,
            start_nanos,
            end_seconds,
            end_nanos,
            start_type,
            end_type,
            flag,
            duration,
        }
    }

    #[test]
    fn date_time_interval_round_trip_explicit() {
        let iv = make_interval(
            1735689600,
            0, // 2025-01-01T00:00:00Z
            1743465600,
            0, // 2025-04-01T00:00:00Z
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.0),
        );
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn date_time_interval_round_trip_start_duration() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 3, 0, 0, 0, 0.0), // P3M
        );
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn date_time_interval_round_trip_duration_end() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_DURATION_END,
            make_duration(1, 0, 3, 0, 0, 0, 0.0),
        );
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn date_time_interval_round_trip_with_nanos() {
        let iv = make_interval(
            1735689600,
            500_000_000,
            1743465600,
            999_999_999,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.5),
        );
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn date_time_interval_round_trip_negative_timestamps() {
        // Before Unix epoch: 1969-06-15T00:00:00Z
        let iv = make_interval(
            -16070400,
            0,
            -8035200,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 93, 0, 0, 0.0),
        );
        let bytes = iv.to_lexical();
        let iv2 = DateTimeInterval::from_lexical(bytes);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn date_time_interval_preserves_duration_granularity() {
        // P3M and P90D are different durations even if they cover similar spans
        let iv_3m = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 3, 0, 0, 0, 0.0), // P3M
        );
        let iv_90d = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 0, 90, 0, 0, 0.0), // P90D
        );

        let bytes_3m = iv_3m.to_lexical();
        let bytes_90d = iv_90d.to_lexical();

        // Same start/end timestamps but different duration metadata
        let rt_3m = DateTimeInterval::from_lexical(bytes_3m);
        let rt_90d = DateTimeInterval::from_lexical(bytes_90d);

        assert_eq!(rt_3m.duration.month, 3);
        assert_eq!(rt_3m.duration.day, 0);
        assert_eq!(rt_90d.duration.month, 0);
        assert_eq!(rt_90d.duration.day, 90);
        assert_ne!(rt_3m, rt_90d);
    }

    #[test]
    fn date_time_interval_sorting_by_start_then_end() {
        // Earlier start sorts first
        let iv_early = make_interval(
            1000000,
            0,
            2000000,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );
        let iv_late = make_interval(
            1500000,
            0,
            2000000,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 5, 0, 0, 0.0),
        );

        let entry_early = DateTimeInterval::make_entry(&iv_early);
        let entry_late = DateTimeInterval::make_entry(&iv_late);
        assert!(entry_early < entry_late);

        // Same start, earlier end sorts first
        let iv_short = make_interval(
            1000000,
            0,
            1500000,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 5, 0, 0, 0.0),
        );
        let iv_long = make_interval(
            1000000,
            0,
            2000000,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );

        let entry_short = DateTimeInterval::make_entry(&iv_short);
        let entry_long = DateTimeInterval::make_entry(&iv_long);
        assert!(entry_short < entry_long);
    }

    #[test]
    fn date_time_interval_sorting_nanos_tiebreaker() {
        let iv_a = make_interval(
            1000000,
            100,
            2000000,
            0,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );
        let iv_b = make_interval(
            1000000,
            200,
            2000000,
            0,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );

        let entry_a = DateTimeInterval::make_entry(&iv_a);
        let entry_b = DateTimeInterval::make_entry(&iv_b);
        assert!(entry_a < entry_b);
    }

    #[test]
    fn date_time_interval_sorting_negative_before_positive() {
        let iv_neg = make_interval(
            -1000000,
            0,
            0,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );
        let iv_pos = make_interval(
            1000000,
            0,
            2000000,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 11, 0, 0, 0.0),
        );

        let entry_neg = DateTimeInterval::make_entry(&iv_neg);
        let entry_pos = DateTimeInterval::make_entry(&iv_pos);
        assert!(entry_neg < entry_pos);
    }

    #[test]
    fn date_time_interval_flag_values_preserved() {
        for flag in [
            INTERVAL_FLAG_EXPLICIT,
            INTERVAL_FLAG_START_DURATION,
            INTERVAL_FLAG_DURATION_END,
        ] {
            let iv = make_interval(
                1735689600,
                0,
                1743465600,
                0,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_COMPONENT_DATE,
                flag,
                make_duration(1, 0, 3, 0, 0, 0, 0.0),
            );
            let rt = DateTimeInterval::from_lexical(iv.to_lexical());
            assert_eq!(flag, rt.flag, "flag {flag} not preserved in round-trip");
        }
    }

    #[test]
    fn date_time_interval_component_types_preserved() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            500_000,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.0),
        );
        let rt = DateTimeInterval::from_lexical(iv.to_lexical());
        assert_eq!(INTERVAL_COMPONENT_DATE, rt.start_type);
        assert_eq!(INTERVAL_COMPONENT_DATETIME, rt.end_type);
    }

    #[test]
    fn date_time_interval_mixed_in_typed_dict_sort() {
        let mut entries: Vec<TypedDictEntry> = vec![
            DateTimeInterval::make_entry(&make_interval(
                2000000,
                0,
                3000000,
                0,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_FLAG_EXPLICIT,
                make_duration(1, 0, 0, 11, 0, 0, 0.0),
            )),
            DateTimeInterval::make_entry(&make_interval(
                1000000,
                0,
                3000000,
                0,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_FLAG_EXPLICIT,
                make_duration(1, 0, 0, 23, 0, 0, 0.0),
            )),
            DateTimeInterval::make_entry(&make_interval(
                1000000,
                0,
                2000000,
                0,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_COMPONENT_DATE,
                INTERVAL_FLAG_START_DURATION,
                make_duration(1, 0, 0, 11, 0, 0, 0.0),
            )),
        ];
        entries.sort();

        // Expected order: (1M,2M) < (1M,3M) < (2M,3M)
        let iv0 = entries[0].as_val::<DateTimeInterval, DateTimeInterval>();
        let iv1 = entries[1].as_val::<DateTimeInterval, DateTimeInterval>();
        let iv2 = entries[2].as_val::<DateTimeInterval, DateTimeInterval>();

        assert_eq!(1000000, iv0.start_seconds);
        assert_eq!(2000000, iv0.end_seconds);
        assert_eq!(1000000, iv1.start_seconds);
        assert_eq!(3000000, iv1.end_seconds);
        assert_eq!(2000000, iv2.start_seconds);
        assert_eq!(3000000, iv2.end_seconds);
    }

    #[test]
    fn date_time_interval_iso_string_explicit_dates() {
        let iv = make_interval(
            1735689600,
            0, // 2025-01-01T00:00:00Z
            1743465600,
            0, // 2025-04-01T00:00:00Z
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn date_time_interval_iso_string_explicit_datetimes_with_time() {
        // 2025-01-01T10:30:00Z and 2025-04-01T15:45:00Z
        let iv = make_interval(
            1735727400,
            0, // 2025-01-01T10:30:00Z
            1743522300,
            0, // 2025-04-01T15:45:00Z
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 5, 15, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T10:30:00Z/2025-04-01T15:45:00Z", s);
    }

    #[test]
    fn date_time_interval_iso_string_explicit_with_nanos() {
        let iv = make_interval(
            1735689600,
            500_000_000,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00.500Z/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn date_time_interval_iso_string_start_duration() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 3, 0, 0, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/P3M", s);
    }

    #[test]
    fn date_time_interval_iso_string_duration_end() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_DURATION_END,
            make_duration(1, 0, 3, 0, 0, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("P3M/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn date_time_interval_iso_string_duration_with_time_parts() {
        let iv = make_interval(
            1735689600,
            0,
            1735693200,
            0, // 1 hour later
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 0, 0, 1, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/PT1H", s);
    }

    #[test]
    fn date_time_interval_iso_string_midnight_datetime_renders_time() {
        let iv = make_interval(
            1735689600,
            0, // 2025-01-01T00:00:00Z (midnight)
            1743465600,
            0, // 2025-04-01T00:00:00Z (midnight)
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 90, 0, 0, 0.0),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/2025-04-01T00:00:00Z", s);
    }

    #[test]
    fn duration_to_string_tenth_second() {
        let duration = Duration {
            sign: 1,
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0.1,
        };
        let s = duration_string(&duration);
        assert_eq!("PT0.1S", s);
    }

    #[test]
    fn date_time_interval_iso_string_with_tenth_second_period() {
        let iv = make_interval(
            1735689600,
            0,
            1735689600,
            100_000_000,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 0, 0, 0, 0, 0.1),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/2025-01-01T00:00:00.100Z", s);
    }

    #[test]
    fn date_time_interval_iso_string_start_duration_tenth_second() {
        let iv = make_interval(
            1735689600,
            0,
            1735689600,
            100_000_000,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_COMPONENT_DATETIME,
            INTERVAL_FLAG_START_DURATION,
            make_duration(1, 0, 0, 0, 0, 0, 0.1),
        );
        let s = <String as FromLexical<DateTimeInterval>>::from_lexical(iv.to_lexical());
        assert_eq!("2025-01-01T00:00:00Z/PT0.1S", s);
    }

    #[test]
    fn date_time_interval_encoding_is_48_bytes() {
        let iv = make_interval(
            1735689600,
            0,
            1743465600,
            0,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_COMPONENT_DATE,
            INTERVAL_FLAG_EXPLICIT,
            make_duration(1, 0, 3, 0, 0, 0, 0.0),
        );
        let bytes = iv.to_lexical();
        // 8(i64) + 4(u32) + 8(i64) + 4(u32) + 1 + 1 + 1 + duration(1+8+1+1+1+1+8 = 21) = 48
        assert_eq!(48, bytes.len());
    }
}
