//! The fields of a code's storage part (D13). `coven-storage` decides what
//! they mean; this module packs them with the format's own primitives (D2),
//! so a storage part has one encoding and a reader refuses a truncated or
//! malformed one. Every owned buffer erases on drop.

use crate::error::{bound, Error};
use crate::wire::{get_text, Decoder, Wire};
use coven_crypto::{SecretBytes, SecretText};
use zeroize::Zeroizing;

/// The largest storage part a restore or invite code carries.
pub const MAX_STORAGE: usize = 16 * 1024;

/// Writes one storage part's fields in order. [`pack`] runs the same fields
/// twice: once to count their bytes, then into a buffer of exactly that size,
/// so no secret is left behind in a grown allocation.
pub struct FieldWriter {
    bytes: Option<Zeroizing<Vec<u8>>>,
    len: usize,
}

impl FieldWriter {
    fn write(&mut self, part: &[u8]) -> Result<(), Error> {
        self.len = self.len.saturating_add(part.len());
        bound(self.len, MAX_STORAGE, "storage settings")?;
        if let Some(bytes) = &mut self.bytes {
            bytes.extend_from_slice(part);
        }
        Ok(())
    }

    /// A `u8`: a tag or a small number.
    pub fn u8(&mut self, value: u8) -> Result<(), Error> {
        self.write(&[value])
    }

    /// A `u16`, big-endian.
    pub fn u16(&mut self, value: u16) -> Result<(), Error> {
        self.write(&value.to_be_bytes())
    }

    /// A `text`: its UTF-8 length as a `u32`, then its bytes.
    pub fn text(&mut self, value: &str) -> Result<(), Error> {
        bound(value.len(), MAX_STORAGE, "storage text")?;
        self.write(&(value.len() as u32).to_be_bytes())?;
        self.write(value.as_bytes())
    }

    /// An `Option<T>`: `0`, or `1` and the value's fields.
    pub fn option<T>(
        &mut self,
        value: Option<T>,
        put: impl FnOnce(&mut Self, T) -> Result<(), Error>,
    ) -> Result<(), Error> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                put(self, value)
            }
        }
    }
}

/// Pack the fields `put` writes into one buffer erased on drop and
/// allocated once. `put` must write the same fields both times it runs.
pub fn pack(put: impl Fn(&mut FieldWriter) -> Result<(), Error>) -> Result<SecretBytes, Error> {
    let mut count = FieldWriter { bytes: None, len: 0 };
    put(&mut count)?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes.try_reserve_exact(count.len)?;
    let mut writer = FieldWriter { bytes: Some(bytes), len: 0 };
    put(&mut writer)?;
    let mut bytes = writer.bytes.expect("the writing pass keeps its buffer");
    assert_eq!(bytes.len(), count.len, "storage fields changed between passes");
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}

/// Reads one storage part's fields in the order they were packed. Every
/// length is checked against what remains before it is allocated, and
/// [`FieldReader::finish`] refuses trailing bytes.
pub struct FieldReader<'a>(Decoder<'a>);

impl<'a> FieldReader<'a> {
    /// Read a whole storage part, at most [`MAX_STORAGE`] bytes.
    pub fn new(bytes: &'a [u8]) -> Result<Self, Error> {
        bound(bytes.len(), MAX_STORAGE, "storage settings")?;
        Ok(Self(Decoder::new(bytes)?))
    }

    /// A `u8`.
    pub fn u8(&mut self) -> Result<u8, Error> {
        u8::get(&mut self.0)
    }

    /// A big-endian `u16`.
    pub fn u16(&mut self) -> Result<u16, Error> {
        u16::get(&mut self.0)
    }

    /// A `text` that is not secret, such as a bucket or folder id.
    pub fn text(&mut self, field: &'static str) -> Result<String, Error> {
        get_text(&mut self.0, MAX_STORAGE, field)
    }

    /// A secret `text`, such as a key or share token, erased on drop.
    pub fn secret_text(&mut self, field: &'static str) -> Result<SecretText, Error> {
        Ok(SecretText::new(self.text(field)?))
    }

    /// An `Option<T>`: `0`, or `1` and the value's fields. Any other tag is
    /// refused. `get` may fail with the caller's own error, such as a value
    /// that reads but is not valid.
    pub fn option<T, E: From<Error>>(
        &mut self,
        get: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<Option<T>, E> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(get(self)?)),
            tag => Err(Error::UnknownTag { field: "option", tag }.into()),
        }
    }

    /// Refuse bytes after the last field.
    pub fn finish(self) -> Result<(), Error> {
        self.0.finish()
    }
}

#[cfg(test)]
#[path = "code_fields_tests.rs"]
mod tests;
