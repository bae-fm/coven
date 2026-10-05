//! Canonical primary keys in SQLite numeric, binary text and blob order.

use crate::error::{bound, require, Error, Rule};
use crate::value::Value;
use crate::wire::{Decoder, Encoder, Wire, MAX_BYTES, MAX_ITEMS};

/// Encode a nonempty list of non-null key components in SQLite value order.
/// Integers and reals share numeric order and equal numbers share bytes. Decoding
/// chooses an integer whenever the number fits exactly in i64. Text uses BINARY
/// collation; schema-specific collations must be normalized by the caller.
/// Composite keys sort lexicographically, with a shorter prefix first.
pub fn encode_key(values: &[Value]) -> Result<Vec<u8>, Error> {
    require(!values.is_empty(), "row key", Rule::Required)?;
    bound(values.len(), MAX_ITEMS, "key components")?;
    let mut out = Encoder::new();
    for value in values {
        value.validate()?;
        match value {
            Value::Null => {
                return Err(Error::Invalid {
                    field: "row key",
                    rule: Rule::NullKey,
                })
            }
            Value::Integer(0) | Value::Real(0) => 0x12u8.put(&mut out)?,
            Value::Integer(n) => {
                let magnitude = n.unsigned_abs();
                let shift = magnitude.leading_zeros();
                number(*n < 0, (63 - shift) as i32, magnitude << shift, &mut out)?;
            }
            Value::Real(bits) => {
                let negative = bits >> 63 != 0;
                let exponent = ((bits >> 52) & 0x7ff) as i32;
                let fraction = bits & ((1u64 << 52) - 1);
                if exponent == 0x7ff {
                    (if negative { 0x10u8 } else { 0x14 }).put(&mut out)?;
                } else if exponent == 0 {
                    let shift = fraction.leading_zeros();
                    number(
                        negative,
                        63 - shift as i32 - 1074,
                        fraction << shift,
                        &mut out,
                    )?;
                } else {
                    number(
                        negative,
                        exponent - 1023,
                        ((1u64 << 52) | fraction) << 11,
                        &mut out,
                    )?;
                }
            }
            Value::Text(text) => escaped(0x20, text.as_bytes(), &mut out)?,
            Value::Blob(bytes) => escaped(0x30, bytes, &mut out)?,
        }
    }
    bound(out.bytes.len(), MAX_BYTES, "key bytes")?;
    Ok(out.bytes)
}

fn number(negative: bool, exponent: i32, mantissa: u64, out: &mut Encoder) -> Result<(), Error> {
    let exponent = (exponent + 1074) as u16;
    if negative {
        0x11u8.put(out)?;
        (!exponent).put(out)?;
        (!mantissa).put(out)
    } else {
        0x13u8.put(out)?;
        exponent.put(out)?;
        mantissa.put(out)
    }
}

fn escaped(tag: u8, bytes: &[u8], out: &mut Encoder) -> Result<(), Error> {
    tag.put(out)?;
    for byte in bytes {
        byte.put(out)?;
        if *byte == 0 {
            255u8.put(out)?;
        }
    }
    out.bytes(&[0, 0])
}

/// Decode and validate the sole canonical representation of a primary key.
/// Equal integral reals and integers decode as integers; SQL values outside
/// keys retain their original storage class in ordinary value encoding.
pub fn decode_key(bytes: &[u8]) -> Result<Vec<Value>, Error> {
    bound(bytes.len(), MAX_BYTES, "key bytes")?;
    let mut input = Decoder::new(bytes)?;
    let mut values = Vec::new();
    while !input.is_empty() {
        bound(values.len() + 1, MAX_ITEMS, "key components")?;
        let tag = u8::get(&mut input)?;
        let value = match tag {
            0x10 => Value::Real(f64::NEG_INFINITY.to_bits()),
            0x14 => Value::Real(f64::INFINITY.to_bits()),
            0x12 => Value::Integer(0),
            0x11 | 0x13 => {
                let mut exponent = u16::get(&mut input)?;
                let mut mantissa = u64::get(&mut input)?;
                if tag == 0x11 {
                    exponent = !exponent;
                    mantissa = !mantissa;
                }
                decode_number(tag == 0x11, i32::from(exponent) - 1074, mantissa)?
            }
            0x20 | 0x30 => {
                let mut value = Vec::new();
                loop {
                    let byte = u8::get(&mut input)?;
                    if byte == 0 {
                        match u8::get(&mut input)? {
                            0 => break,
                            255 => {}
                            _ => return Err(key_error()),
                        }
                    }
                    value.try_reserve(1).map_err(|_| Error::Allocation)?;
                    value.push(byte);
                }
                if tag == 0x20 {
                    Value::Text(String::from_utf8(value).map_err(|_| Error::Utf8)?)
                } else {
                    Value::Blob(value)
                }
            }
            _ => return Err(key_error()),
        };
        values.try_reserve(1).map_err(|_| Error::Allocation)?;
        values.push(value);
    }
    require(encode_key(&values)? == bytes, "key", Rule::KeyEncoding)?;
    Ok(values)
}

fn key_error() -> Error {
    Error::Invalid {
        field: "key",
        rule: Rule::KeyEncoding,
    }
}

fn decode_number(negative: bool, exponent: i32, mantissa: u64) -> Result<Value, Error> {
    if !(-1074..=1023).contains(&exponent) || mantissa >> 63 != 1 {
        return Err(key_error());
    }
    if (0..=63).contains(&exponent) {
        let shift = 63 - exponent as u32;
        let magnitude = mantissa >> shift;
        if magnitude << shift == mantissa {
            if negative && magnitude == 1u64 << 63 {
                return Ok(Value::Integer(i64::MIN));
            }
            if let Ok(n) = i64::try_from(magnitude) {
                return Ok(Value::Integer(if negative { -n } else { n }));
            }
        }
    }
    let bits = if exponent >= -1022 {
        if mantissa & 0x7ff != 0 {
            return Err(key_error());
        }
        (((exponent + 1023) as u64) << 52) | ((mantissa >> 11) & ((1u64 << 52) - 1))
    } else {
        let shift = (63 - (exponent + 1074)) as u32;
        let fraction = mantissa >> shift;
        if fraction << shift != mantissa {
            return Err(key_error());
        }
        fraction
    };
    Ok(Value::Real(bits | (u64::from(negative) << 63)))
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
