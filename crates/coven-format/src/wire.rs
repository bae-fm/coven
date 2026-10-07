//! The fixed-width, bounded binary primitives shared by every object.

use crate::error::{bound, require, Error, Rule};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MAX_OBJECT: usize = 16 * 1024 * 1024;
pub(crate) const MAX_ITEMS: usize = 65_536;
pub(crate) const MAX_BYTES: usize = 8 * 1024 * 1024;

pub(crate) trait Wire: Sized {
    fn put(&self, out: &mut Encoder) -> Result<(), Error>;
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error>;
}

pub(crate) struct Encoder {
    pub(crate) bytes: Vec<u8>,
    items: usize,
}

impl Encoder {
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            items: 0,
        }
    }

    pub(crate) fn charge_items(&mut self, count: usize) -> Result<(), Error> {
        self.items = self.items.saturating_add(count);
        bound(self.items, MAX_ITEMS, "total collection items")
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        bound(
            self.bytes.len().saturating_add(bytes.len()),
            MAX_OBJECT,
            "object",
        )?;
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| Error::Allocation)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    items: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, Error> {
        bound(bytes.len(), MAX_OBJECT, "object")?;
        Ok(Self { bytes, items: 0 })
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let result = self.bytes.get(..n).ok_or(Error::Truncated)?;
        self.bytes = &self.bytes[n..];
        Ok(result)
    }

    pub(crate) fn charge_items(&mut self, count: usize) -> Result<(), Error> {
        self.items = self.items.saturating_add(count);
        bound(self.items, MAX_ITEMS, "total collection items")
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn finish(self) -> Result<(), Error> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(Error::TrailingBytes)
        }
    }
}

pub(crate) fn decode_frame(bytes: &[u8]) -> Result<(u8, Decoder<'_>), Error> {
    let length = crate::frame_length(bytes)?;
    if bytes.len() < length {
        return Err(Error::Truncated);
    }
    if bytes.len() > length {
        return Err(Error::TrailingBytes);
    }
    Ok((bytes[0], Decoder::new(&bytes[crate::FRAME_PREFIX_LEN..])?))
}

macro_rules! integer {
    ($($ty:ty),*) => { $(
        impl Wire for $ty {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> { out.bytes(&self.to_be_bytes()) }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
                Ok(Self::from_be_bytes(<[u8; std::mem::size_of::<Self>()]>::get(input)?))
            }
        }
    )* };
}
integer!(u8, u16, u32, u64, i64);

impl<const N: usize> Wire for [u8; N] {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        out.bytes(self)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let mut result = [0; N];
        result.copy_from_slice(input.take(N)?);
        Ok(result)
    }
}

impl<T: Wire> Wire for Vec<T> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        bound(self.len(), MAX_ITEMS, "collection")?;
        out.charge_items(self.len())?;
        (self.len() as u32).put(out)?;
        for item in self {
            item.put(out)?;
        }
        Ok(())
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        get_sequence(input, T::get)
    }
}

impl<K: Wire + Ord, V: Wire> Wire for BTreeMap<K, V> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        bound(self.len(), MAX_ITEMS, "collection")?;
        out.charge_items(self.len())?;
        (self.len() as u32).put(out)?;
        for (key, value) in self {
            key.put(out)?;
            value.put(out)?;
        }
        Ok(())
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        get_map(input, K::get)
    }
}
impl<T: Wire + Ord> Wire for BTreeSet<T> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        bound(self.len(), MAX_ITEMS, "collection")?;
        out.charge_items(self.len())?;
        (self.len() as u32).put(out)?;
        for item in self {
            item.put(out)?;
        }
        Ok(())
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let values = Vec::<T>::get(input)?;
        require(values.windows(2).all(|p| p[0] < p[1]), "set", Rule::Order)?;
        Ok(values.into_iter().collect())
    }
}

pub(crate) fn get_sequence<T>(
    input: &mut Decoder<'_>,
    get: impl Fn(&mut Decoder<'_>) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    let count = u32::get(input)? as usize;
    bound(count, MAX_ITEMS, "collection")?;
    if count > input.bytes.len() {
        return Err(Error::Truncated);
    }
    input.charge_items(count)?;
    let mut result = Vec::new();
    for _ in 0..count {
        // Reserve only after decoding a real item. A hostile count must not
        // allocate space for values whose bytes have not been checked.
        let value = get(input)?;
        result.try_reserve(1).map_err(|_| Error::Allocation)?;
        result.push(value);
    }
    Ok(result)
}

fn get_map<K: Ord, V: Wire>(
    input: &mut Decoder<'_>,
    get_key: impl Fn(&mut Decoder<'_>) -> Result<K, Error>,
) -> Result<BTreeMap<K, V>, Error> {
    let count = u32::get(input)? as usize;
    bound(count, MAX_ITEMS, "collection")?;
    if count > input.bytes.len() / 2 {
        return Err(Error::Truncated);
    }
    input.charge_items(count)?;
    let mut result = BTreeMap::new();
    for _ in 0..count {
        let key = get_key(input)?;
        require(
            result
                .last_key_value()
                .is_none_or(|(previous, _)| previous < &key),
            "map keys",
            Rule::Order,
        )?;
        result.insert(key, V::get(input)?);
    }
    Ok(result)
}

pub(crate) fn get_name_map<V: Wire>(input: &mut Decoder<'_>) -> Result<BTreeMap<String, V>, Error> {
    get_map(input, get_name)
}

pub(crate) fn get_name(input: &mut Decoder<'_>) -> Result<String, Error> {
    let length = u32::get(input)? as usize;
    bound(length, 1024, "name")?;
    let text = std::str::from_utf8(input.take(length)?).map_err(|_| Error::Utf8)?;
    crate::value::name(text)?;
    let mut value = String::new();
    value
        .try_reserve_exact(length)
        .map_err(|_| Error::Allocation)?;
    value.push_str(text);
    Ok(value)
}

pub(crate) fn put_blob(bytes: &[u8], out: &mut Encoder) -> Result<(), Error> {
    bound(bytes.len(), MAX_BYTES, "bytes")?;
    (bytes.len() as u32).put(out)?;
    out.bytes(bytes)
}

pub(crate) fn get_blob(input: &mut Decoder<'_>) -> Result<Vec<u8>, Error> {
    let count = u32::get(input)? as usize;
    bound(count, MAX_BYTES, "bytes")?;
    let bytes = input.take(count)?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|_| Error::Allocation)?;
    result.extend_from_slice(bytes);
    Ok(result)
}

impl Wire for String {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        put_blob(self.as_bytes(), out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        String::from_utf8(get_blob(input)?).map_err(|_| Error::Utf8)
    }
}

macro_rules! wire_struct {
    (@get $input:ident) => { crate::wire::Wire::get($input) };
    (@get $input:ident, $get:path) => { $get($input) };
    ($ty:ty, $($field:ident $(=> $get:path)?),+ $(,)?) => {
        impl crate::wire::Wire for $ty {
            fn put(&self, out: &mut crate::wire::Encoder) -> Result<(), crate::Error> {
                $(crate::wire::Wire::put(&self.$field, out)?;)+
                Ok(())
            }
            fn get(input: &mut crate::wire::Decoder<'_>) -> Result<Self, crate::Error> {
                Ok(Self { $($field: crate::wire::wire_struct!(@get input $(, $get)?)?,)+ })
            }
        }
    };
}
pub(crate) use wire_struct;

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
