#![allow(clippy::precedence, clippy::verbose_bit_mask)]

//! Code for storing, loading, and using log arrays.
//!
//! A log array is a contiguous sequence of N unsigned integers, with each value occupying exactly
//! W bits. By choosing W as the minimal bit width required for the largest value in the array, the
//! whole sequence can be compressed while increasing the constant cost of indexing by a few
//! operations over the typical byte-aligned array.
//!
//! The log array operations in this module use the following implementation:
//!
//! 1. The input buffer can be evenly divided into L+1 words, where a word is 64 bits.
//! 2. The first L words are the data buffer, a contiguous sequence of elements, where an element
//!    is an unsigned integer represented by W bits.
//! 3. The L+1 word is the control word and contains the following sequence:
//!    1. a 32-bit unsigned integer representing N, the number of elements,
//!    2. an 8-bit unsigned integer representing W, the number of bits used to store each element,
//!       and
//!    3. 24 unused bits.
//!
//! # Notes
//!
//! * All integers are stored in a standard big-endian encoding.
//! * The maximum bit width W is 64.
//! * The maximum number of elements is 2^32-1.
//!
//! # Naming
//!
//! Because of the ambiguity of the English language and possibility to confuse the meanings of the
//! words used to describe aspects of this code, we try to use the following definitions
//! consistently throughout:
//!
//! * buffer: a contiguous sequence of bytes
//!
//! * size: the number of bytes in a buffer
//!
//! * word: a 64-bit contiguous sequence aligned on 8-byte boundaries starting at the beginning of
//!     the input buffer
//!
//! * element: a logical unsigned integer value that is a member of the log array
//!
//! * index: the logical address of an element in the data buffer. A physical index is preceded by
//!     word, byte, or bit to indicate the address precision of the index.
//!
//! * offset: the number of bits preceding the msb of an element within the first word containing
//!     that element
//!
//! * width: the number of bits that every element occupies in the log array
//!
//! * length: the number of elements in the log array

use crate::storage::{FileLoad, SyncableFile};

use super::util::{self, calculate_width};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use std::{cmp::Ordering, convert::TryFrom, error, fmt, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, FramedRead};

use itertools::Itertools;

// Static assertion: We expect the system architecture bus width to be >= 32 bits. If it is not,
// the following line will cause a compiler error. (Ignore the unrelated error message itself.)
const _: usize = 0 - !(std::mem::size_of::<usize>() >= 32 >> 3) as usize;

/// An in-memory log array
#[derive(Clone)]
pub struct LogArray {
    /// Index of the first accessible element
    ///
    /// For an original log array, this is initialized to 0. For a slice, this is the index to the
    /// first element of the slice.
    first: u64,

    /// Number of accessible elements
    ///
    /// For an original log array, this is initialized to the value read from the control word. For
    /// a slice, it is the length of the slice.
    len: u64,

    /// Bit width of each element
    width: u8,

    /// Shared reference to the input buffer
    ///
    /// Index 0 points to the first byte of the first element. The last word is the control word.
    input_buf: Bytes,
}

impl std::fmt::Debug for LogArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogArray([{}])", self.iter().format(", "))
    }
}

/// An error that occurred during a log array operation.
#[derive(Debug, PartialEq)]
pub enum LogArrayError {
    InputBufferTooSmall(usize),
    WidthTooLarge(u8),
    UnexpectedInputBufferSize(u64, u64, u64, u8),
}

impl LogArrayError {
    /// Validate the input buffer size.
    ///
    /// It must have at least the control word.
    fn validate_input_buf_size(input_buf_size: usize) -> Result<(), Self> {
        if input_buf_size < 8 {
            return Err(LogArrayError::InputBufferTooSmall(input_buf_size));
        }
        Ok(())
    }

    /// Validate the number of elements and bit width against the input buffer size.
    ///
    /// The bit width should no greater than 64 since each word is 64 bits.
    ///
    /// The input buffer size should be the appropriate multiple of 8 to include the exact number
    /// of encoded elements plus the control word.
    fn validate_len_and_width(input_buf_size: usize, len: u64, width: u8) -> Result<(), Self> {
        if width > 64 {
            return Err(LogArrayError::WidthTooLarge(width));
        }

        // Calculate the expected input buffer size. This includes the control word.
        // To avoid overflow, convert `len: u32` to `u64` and do the addition in `u64`.
        let expected_buf_size = len * u64::from(width) + 127 >> 6 << 3;
        let input_buf_size = u64::try_from(input_buf_size).unwrap();

        if input_buf_size != expected_buf_size {
            return Err(LogArrayError::UnexpectedInputBufferSize(
                input_buf_size,
                expected_buf_size,
                len,
                width,
            ));
        }

        Ok(())
    }

    /// Validate the number of elements and bit width against the input buffer size.
    ///
    /// The bit width should no greater than 64 since each word is 64 bits.
    ///
    /// The input buffer size should be at least the appropriate
    /// multiple of 8 to include the exact number of encoded elements
    /// plus the control word. It is allowed to be larger.
    fn validate_len_and_width_trailing(
        input_buf_size: usize,
        len: u64,
        width: u8,
    ) -> Result<(), Self> {
        if width > 64 {
            return Err(LogArrayError::WidthTooLarge(width));
        }

        // Calculate the expected input buffer size. This includes the control word.
        // To avoid overflow, convert `len: u32` to `u64` and do the addition in `u64`.
        let expected_buf_size = len * u64::from(width) + 127 >> 6 << 3;
        let input_buf_size = u64::try_from(input_buf_size).unwrap();

        if input_buf_size < expected_buf_size {
            return Err(LogArrayError::UnexpectedInputBufferSize(
                input_buf_size,
                expected_buf_size,
                len,
                width,
            ));
        }

        Ok(())
    }
}

impl fmt::Display for LogArrayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use LogArrayError::*;
        match self {
            InputBufferTooSmall(input_buf_size) => {
                write!(f, "expected input buffer size ({}) >= 8", input_buf_size)
            }
            WidthTooLarge(width) => write!(f, "expected width ({}) <= 64", width),
            UnexpectedInputBufferSize(input_buf_size, expected_buf_size, len, width) => write!(
                f,
                "expected input buffer size ({}) to be {} for {} elements and width {}",
                input_buf_size, expected_buf_size, len, width
            ),
        }
    }
}

impl error::Error for LogArrayError {}

impl From<LogArrayError> for io::Error {
    fn from(err: LogArrayError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

#[derive(Clone)]
pub struct LogArrayIterator {
    logarray: LogArray,
    pos: usize,
    end: usize,
}

impl Iterator for LogArrayIterator {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.pos == self.end {
            None
        } else {
            let result = self.logarray.entry(self.pos);
            self.pos += 1;

            Some(result)
        }
    }
}

const MAX_LOGARRAY_LEN: u64 = (1 << 56) - 1;

pub fn parse_control_word(buf: &[u8]) -> (u64, u8) {
    let len_1 = BigEndian::read_u32(buf) as u64;
    let width = buf[4];
    let len_2 = (BigEndian::read_u32(&buf[4..]) & 0xFFFFFF) as u64; // ignore width byte
    let len: u64 = (len_2 << 32) + len_1;

    (len, width)
}

/// Read the length and bit width from the control word buffer. `buf` must start at the first word
/// after the data buffer. `input_buf_size` is used for validation.
fn read_control_word(buf: &[u8], input_buf_size: usize) -> Result<(u64, u8), LogArrayError> {
    let (len, width) = parse_control_word(buf);
    LogArrayError::validate_len_and_width(input_buf_size, len, width)?;
    Ok((len, width))
}

/// Read the length and bit width from the control word buffer. `buf` must start at the first word
/// after the data buffer. `input_buf_size` is used for validation, where it is allowed to be larger than expected.
fn read_control_word_trailing(
    buf: &[u8],
    input_buf_size: usize,
) -> Result<(u64, u8), LogArrayError> {
    let (len, width) = parse_control_word(buf);
    LogArrayError::validate_len_and_width_trailing(input_buf_size, len, width)?;
    Ok((len, width))
}

fn logarray_length_from_len_width(len: u64, width: u8) -> usize {
    let num_bits = width as usize * len as usize;
    let num_u64 = num_bits / 64 + (if num_bits % 64 == 0 { 0 } else { 1 });
    let num_bytes = num_u64 * 8;

    num_bytes
}

pub fn logarray_length_from_control_word(buf: &[u8]) -> usize {
    let (len, width) = parse_control_word(buf);

    logarray_length_from_len_width(len, width)
}

impl LogArray {
    /// Construct a `LogArray` by parsing a `Bytes` buffer.
    pub fn parse(input_buf: Bytes) -> Result<LogArray, LogArrayError> {
        let input_buf_size = input_buf.len();
        LogArrayError::validate_input_buf_size(input_buf_size)?;
        let (len, width) = read_control_word(&input_buf[input_buf_size - 8..], input_buf_size)?;
        Ok(LogArray {
            first: 0,
            len,
            width,
            input_buf,
        })
    }

    pub fn parse_header_first(mut input_buf: Bytes) -> Result<(LogArray, Bytes), LogArrayError> {
        let input_buf_size = input_buf.len();
        LogArrayError::validate_input_buf_size(input_buf_size)?;
        let (len, width) = read_control_word_trailing(&input_buf[..8], input_buf_size)?;
        let num_bytes = logarray_length_from_len_width(len, width);
        input_buf.advance(8);
        let rest = input_buf.split_off(num_bytes);
        Ok((
            LogArray {
                first: 0,
                len,
                width,
                input_buf,
            },
            rest,
        ))
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        // `usize::try_from` succeeds if `std::mem::size_of::<usize>()` >= 4.
        usize::try_from(self.len).unwrap()
    }

    /// Returns `true` if there are no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the bit width.
    pub fn width(&self) -> u8 {
        self.width
    }

    /// Reads the data buffer and returns the element at the `index`.
    ///
    /// Panics if `index` is >= the length of the log array.
    pub fn entry(&self, index: usize) -> u64 {
        debug_assert!(
            index < self.len(),
            "expected index ({}) < length ({})",
            index,
            self.len
        );

        // `usize::try_from` succeeds if `std::mem::size_of::<usize>()` >= 4.
        let bit_index = usize::from(self.width) * (usize::try_from(self.first).unwrap() + index);

        // Calculate the byte index from the bit index.
        let byte_index = bit_index >> 6 << 3;

        let buf = &self.input_buf;

        // Read the first word.
        let first_word = BigEndian::read_u64(&buf[byte_index..]);

        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - self.width;

        // Get the bit offset in `first_word`.
        let offset = (bit_index & 0b11_1111) as u8;

        // If the element fits completely in `first_word`, we can return it immediately.
        if offset + self.width <= 64 {
            // Decode by introducing leading zeros and shifting all the way to the right.
            return first_word << offset >> leading_zeros;
        }

        // At this point, we have an element split over `first_word` and `second_word`. The bottom
        // bits of `first_word` become the upper bits of the decoded value, and the top bits of
        // `second_word` become the lower bits of the decoded value.

        // Read the second word
        let second_word = BigEndian::read_u64(&buf[byte_index + 8..]);

        // These are the bit widths of the important parts in `first_word` and `second_word`.
        let first_width = 64 - offset;
        let second_width = self.width - first_width;

        // These are the parts of the element with the unimportant parts removed.

        // Introduce leading zeros and trailing zeros where the `second_part` will go.
        let first_part = first_word << offset >> offset << second_width;

        // Introduce leading zeros where the `first_part` will go.
        let second_part = second_word >> 64 - second_width;

        // Decode by combining the first and second parts.
        first_part | second_part
    }

    pub fn iter(&self) -> LogArrayIterator {
        LogArrayIterator {
            logarray: self.clone(),
            pos: 0,
            end: self.len(),
        }
    }

    /// Returns a logical slice of the elements in a log array.
    ///
    /// Panics if `index` + `length` is >= the length of the log array.
    pub fn slice(&self, offset: usize, len: usize) -> LogArray {
        let offset = offset as u64;
        let len = len as u64;
        let slice_end = offset.checked_add(len).unwrap_or_else(|| {
            panic!("overflow from slice offset ({}) + length ({})", offset, len)
        });
        assert!(
            slice_end <= self.len,
            "expected slice offset ({}) + length ({}) <= source length ({})",
            offset,
            len,
            self.len
        );
        LogArray {
            first: self.first + offset,
            len,
            width: self.width,
            input_buf: self.input_buf.clone(),
        }
    }
}

/// write a logarray directly to an AsyncWrite
pub struct LogArrayBufBuilder<B: BufMut> {
    /// Destination of the log array data
    buf: B,
    /// Bit width of an element
    width: u8,
    /// Storage for the next word to be written to the buffer
    current: u64,
    /// Bit offset in `current` for the msb of the next encoded element
    offset: u8,
    /// Number of elements written to the buffer
    count: u64,
}

impl<D: std::ops::DerefMut<Target = BytesMut> + BufMut> LogArrayBufBuilder<D> {
    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional * self.width as usize / 8);
    }
}

impl<B: BufMut> LogArrayBufBuilder<B> {
    pub fn new(buf: B, width: u8) -> Self {
        Self {
            buf,
            width,
            // Zero is needed for bitwise OR-ing new values.
            current: 0,
            // Start at the beginning of `current`.
            offset: 0,
            // No elements have been written.
            count: 0,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn push(&mut self, val: u64) {
        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = u64::BITS - self.width as u32;

        // If `val` does not fit in the `width`, return an error.
        if val.leading_zeros() < u32::from(leading_zeros) {
            panic!("expected value ({}) to fit in {} bits", val, self.width);
        }

        // Otherwise, push `val` onto the log array.
        // Advance the element count since we know we're going to write `val`.
        self.count += 1;

        // Write the first part of `val` to `current`, putting the msb of `val` at the `offset`
        // bit. This may be either the upper bits of `val` only or all of it. We check later.
        self.current |= val << leading_zeros >> self.offset;

        // Increment `offset` past `val`.
        self.offset += self.width;

        // Check if the new `offset` is larger than 64.
        if self.offset >= 64 {
            // We have filled `current`, so write it to the destination.
            //util::write_u64(&mut self.file, self.current).await?;
            self.buf.put_u64(self.current);
            // Wrap the offset with the word size.
            self.offset -= 64;

            // Initialize the new `current`.
            self.current = if self.offset == 0 {
                // Zero is needed for bitwise OR-ing new values.
                0
            } else {
                // This is the second part of `val`: the lower bits.
                val << 64 - self.offset
            };
        }
    }

    pub fn push_vec(&mut self, vals: Vec<u64>) {
        for val in vals {
            self.push(val);
        }
    }

    fn finalize_data(&mut self) {
        if u64::from(self.count) * u64::from(self.width) & 0b11_1111 != 0 {
            self.buf.put_u64(self.current);
        }
    }

    pub fn finalize(mut self) -> B {
        self.finalize_data();

        self.write_control_word();
        self.buf
    }

    pub(crate) fn finalize_without_control_word(mut self) {
        self.finalize_data();
    }

    fn write_control_word(&mut self) {
        let len = self.count;
        let width = self.width;

        let buf = control_word(len, width);
        self.buf.put_slice(&buf);
    }
}

pub(crate) fn control_word(len: u64, width: u8) -> [u8; 8] {
    if len > MAX_LOGARRAY_LEN {
        panic!(
            "length is too large for control word of a logarray: {} (limit is {}",
            len, MAX_LOGARRAY_LEN
        );
    }
    let mut buf = [0; 8];
    let len_1 = (len & 0xFFFFFFFF) as u32;
    let len_2 = ((len >> 32) & 0xFFFFFF) as u32;
    BigEndian::write_u32(&mut buf, len_1);
    BigEndian::write_u32(&mut buf[4..], len_2);
    buf[4] = width;

    buf
}

pub struct LateLogArrayBufBuilder<B: BufMut> {
    /// Destination of the log array data
    buf: B,
    /// NOTE: remove pub
    pub vals: Vec<u64>,
    width: u8,
}

impl<B: BufMut> LateLogArrayBufBuilder<B> {
    pub fn new(buf: B) -> Self {
        Self {
            buf,
            vals: Vec::new(),
            width: 0,
        }
    }

    pub fn count(&self) -> u64 {
        self.vals.len() as u64
    }

    pub fn push(&mut self, val: u64) {
        self.vals.push(val);
        let width = calculate_width(val);
        if self.width < width {
            self.width = width;
        }
    }

    pub fn push_vec(&mut self, vals: Vec<u64>) {
        for val in vals {
            self.push(val)
        }
    }

    pub fn last(&mut self) -> Option<u64> {
        self.vals.last().copied()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.vals.pop()
    }

    pub fn finalize(mut self) -> B {
        let mut builder = LogArrayBufBuilder::new(&mut self.buf, self.width);
        builder.push_vec(self.vals);
        builder.finalize();
        self.buf
    }

    pub fn finalize_header_first(mut self) -> B {
        let control_word = control_word(self.count(), self.width);
        self.buf.put(control_word.as_ref());
        let mut builder = LogArrayBufBuilder::new(&mut self.buf, self.width);
        builder.push_vec(self.vals);
        builder.finalize_without_control_word();
        self.buf
    }
}

/// write a logarray directly to an AsyncWrite
pub struct LogArrayFileBuilder<W: SyncableFile> {
    /// Destination of the log array data
    file: W,
    /// Bit width of an element
    width: u8,
    /// Storage for the next word to be written to the buffer
    current: u64,
    /// Bit offset in `current` for the msb of the next encoded element
    offset: u8,
    /// Number of elements written to the buffer
    count: u64,
}

impl<W: SyncableFile> LogArrayFileBuilder<W> {
    pub fn new(w: W, width: u8) -> LogArrayFileBuilder<W> {
        LogArrayFileBuilder {
            file: w,
            width,
            // Zero is needed for bitwise OR-ing new values.
            current: 0,
            // Start at the beginning of `current`.
            offset: 0,
            // No elements have been written.
            count: 0,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub async fn push(&mut self, val: u64) -> io::Result<()> {
        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - self.width;

        // If `val` does not fit in the `width`, return an error.
        if val.leading_zeros() < u32::from(leading_zeros) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected value ({}) to fit in {} bits", val, self.width),
            ));
        }

        // Otherwise, push `val` onto the log array.
        // Advance the element count since we know we're going to write `val`.
        self.count += 1;

        // Write the first part of `val` to `current`, putting the msb of `val` at the `offset`
        // bit. This may be either the upper bits of `val` only or all of it. We check later.
        self.current |= val << leading_zeros >> self.offset;

        // Increment `offset` past `val`.
        self.offset += self.width;

        // Check if the new `offset` is larger than 64.
        if self.offset >= 64 {
            // We have filled `current`, so write it to the destination.
            util::write_u64(&mut self.file, self.current).await?;
            // Wrap the offset with the word size.
            self.offset -= 64;

            // Initialize the new `current`.
            self.current = if self.offset == 0 {
                // Zero is needed for bitwise OR-ing new values.
                0
            } else {
                // This is the second part of `val`: the lower bits.
                val << 64 - self.offset
            };
        }

        Ok(())
    }

    pub async fn push_vec(&mut self, vals: Vec<u64>) -> io::Result<()> {
        for val in vals {
            self.push(val).await?;
        }

        Ok(())
    }

    pub async fn push_all<S: Stream<Item = io::Result<u64>> + Unpin>(
        &mut self,
        mut vals: S,
    ) -> io::Result<()> {
        while let Some(val) = vals.next().await {
            self.push(val?).await?;
        }

        Ok(())
    }

    async fn finalize_data(&mut self) -> io::Result<()> {
        if self.count * u64::from(self.width) & 0b11_1111 != 0 {
            util::write_u64(&mut self.file, self.current).await?;
        }

        Ok(())
    }

    pub async fn finalize(mut self) -> io::Result<()> {
        let len = self.count;
        let width = self.width;

        // Write the final data word.
        self.finalize_data().await?;

        // Write the control word.
        let buf = control_word(len, width);
        self.file.write_all(&buf).await?;

        self.file.flush().await?;
        self.file.sync_all().await?;

        Ok(())
    }
}

struct LogArrayDecoder {
    /// Storage for the most recent word read from the buffer
    current: u64,
    /// Bit width of an element
    width: u8,
    /// Bit offset from the msb of `current` to the msb of the encoded element
    offset: u8,
    /// Number of elements remaining to decode
    remaining: u64,
}

impl fmt::Debug for LogArrayDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogArrayDecoder {{ current: ")?;
        write!(f, "{:#066b}", self.current)?;
        write!(f, ", width: ")?;
        write!(f, "{:?}", self.width)?;
        write!(f, ", offset: ")?;
        write!(f, "{:?}", self.offset)?;
        write!(f, ", remaining: ")?;
        write!(f, "{:?}", self.remaining)?;
        write!(f, " }}")
    }
}

impl LogArrayDecoder {
    /// Construct a new `LogArrayDecoder`.
    ///
    /// This function does not validate the parameters. Validation of `width` and `remaining` must
    /// be done before calling this function.
    fn new_unchecked(width: u8, remaining: u64) -> Self {
        LogArrayDecoder {
            // The initial value of `current` is ignored by `decode()` because `offset` is 64.
            current: 0,
            // The initial value of `offset` is interpreted in `decode()` to begin reading a new
            // word and ignore the initial value of `current`.
            offset: 64,
            width,
            remaining,
        }
    }
}

impl Decoder for LogArrayDecoder {
    type Item = u64;
    type Error = io::Error;

    /// Decode the next element of the log array.
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<u64>, io::Error> {
        // If we have no elements remaining to decode, clean up and exit.
        if self.remaining == 0 {
            bytes.clear();
            return Ok(None);
        }

        // At this point, we have at least one element to decode.

        // Declare some immutable working values. After this, `self.<field>` only appears on the
        // lhs of `=`.
        let first_word = self.current;
        let offset = self.offset;
        let width = self.width;

        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - width;

        // If the next element fits completely in `first_word`, we can return it immediately.
        if offset + width <= 64 {
            // Increment to the msb of the next element.
            self.offset += width;
            // Decrement since we're returning a decoded element.
            self.remaining -= 1;
            // Decode by introducing leading zeros and shifting all the way to the right.
            return Ok(Some(first_word << offset >> leading_zeros));
        }

        // At this point, we need to read another word because we do not have enough bits in
        // `first_word` to decode.

        // If there isn't a full word available in the buffer, stop until there is.
        if bytes.len() < 8 {
            return Ok(None);
        }

        // Load the `second_word` and advance `bytes` by 1 word.
        let second_word = BigEndian::read_u64(&bytes.split_to(8));
        self.current = second_word;

        // Decrement to indicate we will return another decoded element.
        self.remaining -= 1;

        // If the `offset` is 64, it means that the element is completely included in the
        // `second_word`.
        if offset == 64 {
            // Increment the `offset` to the msb of the next element.
            self.offset = width;

            // Decode by shifting all the way to the right. Since the msb of `second_word` and the
            // encoded value are the same, this naturally introduces leading zeros.
            return Ok(Some(second_word >> leading_zeros));
        }

        // At this point, we have an element split over `first_word` and `second_word`. The bottom
        // bits of `first_word` become the upper bits of the decoded value, and the top bits of
        // `second_word` become the lower bits of the decoded value.

        // These are the bit widths of the important parts in `first_word` and `second_word`.
        let first_width = 64 - offset;
        let second_width = width - first_width;

        // These are the parts of the element with the unimportant parts removed.

        // Introduce leading zeros and trailing zeros where the `second_part` will go.
        let first_part = first_word << offset >> offset << second_width;

        // Introduce leading zeros where the `first_part` will go.
        let second_part = second_word >> 64 - second_width;

        // Increment the `offset` to the msb of the next element.
        self.offset = second_width;

        // Decode by combining the first and second parts.
        Ok(Some(first_part | second_part))
    }
}

pub async fn logarray_file_get_length_and_width<F: FileLoad>(f: F) -> io::Result<(u64, u8)> {
    LogArrayError::validate_input_buf_size(f.size().await?)?;

    let mut buf = [0; 8];
    f.open_read_from(f.size().await? - 8)
        .await?
        .read_exact(&mut buf)
        .await?;
    Ok(read_control_word(&buf, f.size().await?)?)
}

pub async fn logarray_stream_entries<F: 'static + FileLoad>(
    f: F,
) -> io::Result<impl Stream<Item = io::Result<u64>> + Unpin + Send> {
    let (len, width) = logarray_file_get_length_and_width(f.clone()).await?;
    Ok(FramedRead::new(
        f.open_read().await?,
        LogArrayDecoder::new_unchecked(width, len),
    ))
}

#[derive(Clone)]
pub struct MonotonicLogArray(LogArray);

impl std::fmt::Debug for MonotonicLogArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MonotonicLogArray([{}])", self.iter().format(", "))
    }
}

impl MonotonicLogArray {
    pub fn from_logarray(logarray: LogArray) -> MonotonicLogArray {
        if cfg!(debug_assertions) {
            // Validate that the elements are monotonically increasing.
            let mut iter = logarray.iter();
            if let Some(mut pred) = iter.next() {
                for succ in iter {
                    assert!(
                        pred <= succ,
                        "not monotonic: expected predecessor ({}) <= successor ({})",
                        pred,
                        succ
                    );
                    pred = succ;
                }
            }
        }

        MonotonicLogArray(logarray)
    }

    pub fn parse(bytes: Bytes) -> Result<MonotonicLogArray, LogArrayError> {
        let logarray = LogArray::parse(bytes)?;

        Ok(Self::from_logarray(logarray))
    }

    pub fn parse_header_first(bytes: Bytes) -> Result<(MonotonicLogArray, Bytes), LogArrayError> {
        let (logarray, remainder) = LogArray::parse_header_first(bytes)?;

        Ok((Self::from_logarray(logarray), remainder))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn entry(&self, index: usize) -> u64 {
        self.0.entry(index)
    }

    pub fn iter(&self) -> LogArrayIterator {
        self.0.iter()
    }

    pub fn index_of(&self, element: u64) -> Option<usize> {
        let index = self.nearest_index_of(element);
        if index >= self.len() || self.entry(index) != element {
            None
        } else {
            Some(index)
        }
    }

    pub fn nearest_index_of(&self, element: u64) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut min = 0;
        let mut max = self.len() - 1;
        while min <= max {
            let mid = (min + max) / 2;
            match element.cmp(&self.entry(mid)) {
                Ordering::Equal => return mid,
                Ordering::Greater => min = mid + 1,
                Ordering::Less => {
                    if mid == 0 {
                        return 0;
                    }
                    max = mid - 1
                }
            }
        }

        (min + max) / 2 + 1
    }

    pub fn slice(&self, offset: usize, len: usize) -> MonotonicLogArray {
        Self(self.0.slice(offset, len))
    }
}

impl From<LogArray> for MonotonicLogArray {
    fn from(l: LogArray) -> Self {
        Self::from_logarray(l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryBackedStore;
    use crate::storage::FileStore;
    use crate::util::stream_iter_ok;
    use futures::executor::block_on;
    use futures::stream::TryStreamExt;

    #[test]
    fn log_array_error() {
        // Display
        assert_eq!(
            "expected input buffer size (7) >= 8",
            LogArrayError::InputBufferTooSmall(7).to_string()
        );
        assert_eq!(
            "expected width (69) <= 64",
            LogArrayError::WidthTooLarge(69).to_string()
        );
        assert_eq!(
            "expected input buffer size (9) to be 8 for 0 elements and width 17",
            LogArrayError::UnexpectedInputBufferSize(9, 8, 0, 17).to_string()
        );

        // From<LogArrayError> for io::Error
        assert_eq!(
            io::Error::new(
                io::ErrorKind::InvalidData,
                LogArrayError::InputBufferTooSmall(7)
            )
            .to_string(),
            io::Error::from(LogArrayError::InputBufferTooSmall(7)).to_string()
        );
    }

    #[test]
    fn validate_input_buf_size() {
        let val = |buf_size| LogArrayError::validate_input_buf_size(buf_size);
        let err = |buf_size| Err(LogArrayError::InputBufferTooSmall(buf_size));
        assert_eq!(err(7), val(7));
        assert_eq!(Ok(()), val(8));
        assert_eq!(Ok(()), val(9));
        assert_eq!(Ok(()), val(usize::max_value()));
    }

    #[test]
    fn validate_len_and_width() {
        let val =
            |buf_size, len, width| LogArrayError::validate_len_and_width(buf_size, len, width);

        let err = |width| Err(LogArrayError::WidthTooLarge(width));

        // width: 65
        assert_eq!(err(65), val(0, 0, 65));

        let err = |buf_size, expected, len, width| {
            Err(LogArrayError::UnexpectedInputBufferSize(
                buf_size, expected, len, width,
            ))
        };

        // width: 0
        assert_eq!(err(0, 8, 0, 0), val(0, 0, 0));

        // width: 1
        assert_eq!(Ok(()), val(8, 0, 1));
        assert_eq!(err(9, 8, 0, 1), val(9, 0, 1));
        assert_eq!(Ok(()), val(16, 1, 1));

        // width: 64
        assert_eq!(Ok(()), val(16, 1, 64));
        assert_eq!(err(16, 24, 2, 64), val(16, 2, 64));
        assert_eq!(err(24, 16, 1, 64), val(24, 1, 64));

        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            Ok(()),
            val(
                usize::try_from(u64::from(u32::max_value()) + 1 << 3).unwrap(),
                u32::max_value() as u64,
                64
            )
        );

        // width: 5
        assert_eq!(err(16, 24, 13, 5), val(16, 13, 5));
        assert_eq!(Ok(()), val(24, 13, 5));
    }

    #[test]
    pub fn empty() {
        let logarray = LogArray::parse(Bytes::from([0u8; 8].as_ref())).unwrap();
        assert!(logarray.is_empty());
        assert!(MonotonicLogArray::from_logarray(logarray).is_empty());
    }

    #[test]
    pub fn late_logarray_just_zero() {
        let buf = BytesMut::new();
        let mut builder = LateLogArrayBufBuilder::new(buf);
        builder.push(0);
        let logarray_buf = builder.finalize().freeze();
        let logarray = LogArray::parse(logarray_buf).unwrap();
        assert_eq!(logarray.entry(0_usize), 0_u64);
    }

    #[tokio::test]
    #[should_panic(expected = "expected value (8) to fit in 3 bits")]
    async fn log_array_file_builder_panic() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 3);
        block_on(builder.push(8)).unwrap();
    }

    #[tokio::test]
    async fn generate_then_parse_works() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        block_on(async {
            builder
                .push_all(stream_iter_ok(vec![1, 3, 2, 5, 12, 31, 18]))
                .await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();

        assert_eq!(1, logarray.entry(0));
        assert_eq!(3, logarray.entry(1));
        assert_eq!(2, logarray.entry(2));
        assert_eq!(5, logarray.entry(3));
        assert_eq!(12, logarray.entry(4));
        assert_eq!(31, logarray.entry(5));
        assert_eq!(18, logarray.entry(6));
    }

    const TEST0_DATA: [u8; 8] = [
        0b00000000,
        0b00000000,
        0b1_0000000,
        0b00000000,
        0b10_000000,
        0b00000000,
        0b011_00000,
        0b00000000,
    ];
    const TEST0_CONTROL: [u8; 8] = [0, 0, 0, 3, 17, 0, 0, 0];
    const TEST1_DATA: [u8; 8] = [
        0b0100_0000,
        0b00000000,
        0b00101_000,
        0b00000000,
        0b000110_00,
        0b00000000,
        0b0000111_0,
        0b00000000,
    ];

    fn test0_logarray() -> LogArray {
        let mut content = Vec::new();
        content.extend_from_slice(&TEST0_DATA);
        content.extend_from_slice(&TEST0_CONTROL);
        LogArray::parse(Bytes::from(content)).unwrap()
    }

    #[test]
    #[should_panic(expected = "expected index (3) < length (3)")]
    fn entry_panic() {
        let _ = test0_logarray().entry(3);
    }

    #[test]
    #[should_panic(expected = "expected slice offset (2) + length (2) <= source length (3)")]
    fn slice_panic1() {
        let _ = test0_logarray().slice(2, 2);
    }

    #[test]
    #[should_panic(expected = "expected slice offset (4294967296)")]
    #[cfg(target_pointer_width = "64")]
    fn slice_panic2() {
        let _ = test0_logarray().slice(usize::try_from(u32::max_value()).unwrap() + 1, 2);
    }

    #[test]
    #[should_panic(expected = "expected index (2) < length (2)")]
    fn slice_entry_panic() {
        let _ = test0_logarray().slice(1, 2).entry(2);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "not monotonic: expected predecessor (2) <= successor (1)")]
    fn monotonic_panic() {
        let content = [0u8, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 32, 0, 0, 0].as_ref();
        MonotonicLogArray::from_logarray(LogArray::parse(Bytes::from(content)).unwrap());
    }

    #[test]
    fn decode() {
        let mut decoder = LogArrayDecoder::new_unchecked(17, 1);
        let mut bytes = BytesMut::from(TEST0_DATA.as_ref());
        assert_eq!(Some(1), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
        decoder = LogArrayDecoder::new_unchecked(17, 4);
        bytes = BytesMut::from(TEST0_DATA.as_ref());
        assert_eq!(Some(1), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(
            "LogArrayDecoder { current: \
             0b0000000000000000100000000000000010000000000000000110000000000000, width: 17, \
             offset: 17, remaining: 3 }",
            format!("{:?}", decoder)
        );
        assert_eq!(Some(2), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(Some(3), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
        bytes.extend(TEST1_DATA.iter());
        assert_eq!(Some(4), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
    }

    #[tokio::test]
    async fn logarray_file_get_length_and_width_errors() {
        let store = MemoryBackedStore::new();
        let mut writer = store.open_write().await.unwrap();
        writer.write_all(&[0, 0, 0]).await.unwrap();
        writer.sync_all().await.unwrap();
        assert_eq!(
            io::Error::from(LogArrayError::InputBufferTooSmall(3)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let mut writer = store.open_write().await.unwrap();
        writer.write_all(&[0, 0, 0, 0, 65, 0, 0, 0]).await.unwrap();
        writer.sync_all().await.unwrap();
        assert_eq!(
            io::Error::from(LogArrayError::WidthTooLarge(65)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let mut writer = store.open_write().await.unwrap();
        writer.write_all(&[0, 0, 0, 1, 17, 0, 0, 0]).await.unwrap();
        writer.sync_all().await.unwrap();
        assert_eq!(
            io::Error::from(LogArrayError::UnexpectedInputBufferSize(8, 16, 1, 17)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );
    }

    #[tokio::test]
    async fn generate_then_stream_works() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        block_on(async {
            builder.push_all(stream_iter_ok(0..31)).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let entries: Vec<u64> = block_on(
            logarray_stream_entries(store)
                .await
                .unwrap()
                .try_collect::<Vec<u64>>(),
        )
        .unwrap();
        let expected: Vec<u64> = (0..31).collect();
        assert_eq!(expected, entries);
    }

    #[tokio::test]
    async fn iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();

        let result: Vec<u64> = logarray.iter().collect();

        assert_eq!(original, result);
    }

    #[tokio::test]
    async fn iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        let original: Vec<u64> = vec![1, 3, 2, 5, 12, 31, 18];
        block_on(async {
            builder.push_all(stream_iter_ok(original)).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let slice = logarray.slice(2, 3);

        let result: Vec<u64> = slice.iter().collect();

        assert_eq!([2, 5, 12], result.as_ref());
    }

    #[tokio::test]
    async fn monotonic_logarray_index_lookup() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        let original = vec![1, 3, 5, 6, 7, 10, 11, 15, 16, 18, 20, 25, 31];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for (i, &val) in original.iter().enumerate() {
            assert_eq!(i, monotonic.index_of(val).unwrap());
        }

        assert_eq!(None, monotonic.index_of(12));
        assert_eq!(original.len(), monotonic.len());
    }

    #[tokio::test]
    async fn monotonic_logarray_near_index_lookup() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 5);
        let original = vec![3, 5, 6, 7, 10, 11, 15, 16, 18, 20, 25, 31];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;
            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for (i, &val) in original.iter().enumerate() {
            assert_eq!(i, monotonic.index_of(val).unwrap());
        }

        let nearest: Vec<_> = (1..=32).map(|i| monotonic.nearest_index_of(i)).collect();
        let expected = vec![
            0, 0, 0, 1, 1, 2, 3, 4, 4, 4, 5, 6, 6, 6, 6, 7, 8, 8, 9, 9, 10, 10, 10, 10, 10, 11, 11,
            11, 11, 11, 11, 12,
        ];
        assert_eq!(expected, nearest);
    }

    #[tokio::test]
    async fn writing_64_bits_of_data() {
        let store = MemoryBackedStore::new();
        let original = vec![1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        let mut builder = LogArrayFileBuilder::new(store.open_write().await.unwrap(), 4);
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();
        let logarray = LogArray::parse(content).unwrap();
        assert_eq!(original, logarray.iter().collect::<Vec<_>>());
        assert_eq!(16, logarray.len());
        assert_eq!(4, logarray.width());
    }

    #[test]
    fn large_control_word() {
        let num: u64 = 0xFF_FFFF_FFFF_FFFF;
        let width: u8 = 32;

        let control_word = control_word(num, width);
        assert_eq!([255, 255, 255, 255, 32, 255, 255, 255], control_word);
        let (out_num, out_width) = parse_control_word(&control_word);
        assert_eq!(num, out_num);
        assert_eq!(width, out_width);
    }
}
