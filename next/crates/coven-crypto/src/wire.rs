//! Checked reads shared by the secret and ciphertext encodings.

use crate::MaterialError;

pub(crate) fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], MaterialError> {
    let (value, rest) = bytes.split_at_checked(len).ok_or(MaterialError::Encoding)?;
    *bytes = rest;
    Ok(value)
}

pub(crate) fn array<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], MaterialError> {
    take(bytes, N)?
        .try_into()
        .map_err(|_| MaterialError::Encoding)
}

pub(crate) fn number(bytes: &mut &[u8]) -> Result<u64, MaterialError> {
    Ok(u64::from_le_bytes(array(bytes)?))
}

pub(crate) fn prefix(bytes: &mut &[u8], expected: &[u8]) -> Result<(), MaterialError> {
    if take(bytes, expected.len())? != expected {
        return Err(MaterialError::Encoding);
    }
    Ok(())
}

pub(crate) fn end(bytes: &[u8]) -> Result<(), MaterialError> {
    if !bytes.is_empty() {
        return Err(MaterialError::Encoding);
    }
    Ok(())
}
