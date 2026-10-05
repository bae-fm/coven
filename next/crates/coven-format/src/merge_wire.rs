//! Encodings and format validation of merge's column, parent, cell and loss shapes.

use crate::error::Error;
use crate::value::{name, positive, row, Value};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_merge::{Cell, ColumnValue, LostKey, LostValue, Parent, Rule};
use std::collections::{BTreeMap, BTreeSet};

wire_struct!(Parent, row, generation);
wire_struct!(ColumnValue<Value>, value, parents);
wire_struct!(Cell<Value>, write, value);
wire_struct!(LostKey, column, write);
wire_struct!(LostValue<Value>, incarnation, value, replaced_by);

impl Wire for Rule {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::ForeignKey(n) => {
                0u8.put(out)?;
                n.put(out)
            }
            Self::Check(n) => {
                1u8.put(out)?;
                n.put(out)
            }
            Self::DeletedCircle => 2u8.put(out),
            Self::OtherAudience => 3u8.put(out),
            Self::Unique(n) => {
                4u8.put(out)?;
                n.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::ForeignKey(Wire::get(input)?)),
            1 => Ok(Self::Check(Wire::get(input)?)),
            2 => Ok(Self::DeletedCircle),
            3 => Ok(Self::OtherAudience),
            4 => Ok(Self::Unique(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "removal rule",
                tag,
            }),
        }
    }
}

pub(crate) fn column(value: &ColumnValue<Value>) -> Result<(), Error> {
    value.value.validate()?;
    parents(&value.parents)
}

pub(crate) fn parents(values: &BTreeMap<String, Parent>) -> Result<(), Error> {
    for (constraint, parent) in values {
        name(constraint)?;
        row(&parent.row)?;
    }
    Ok(())
}

pub(crate) fn columns(values: &BTreeMap<String, ColumnValue<Value>>) -> Result<(), Error> {
    for (n, value) in values {
        name(n)?;
        column(value)?;
    }
    Ok(())
}

pub(crate) fn state(state: &coven_merge::RowState<Value>) -> Result<(), Error> {
    row(state.row())?;
    for write in state.generations().values() {
        positive(write.number)?;
    }
    for (n, cell) in state.cells() {
        name(n)?;
        positive(cell.write.number)?;
        column(&cell.value)?;
    }
    for (key, value) in state.lost() {
        name(&key.column)?;
        positive(key.write.number)?;
        positive(value.replaced_by.number)?;
        column(&value.value)?;
    }
    Ok(())
}

pub(crate) fn setters(values: &BTreeMap<String, coven_merge::WriteId>) -> Result<(), Error> {
    for (column, write) in values {
        name(column)?;
        positive(write.number)?;
    }
    Ok(())
}

pub(crate) fn rules(values: &BTreeSet<Rule>) -> Result<(), Error> {
    for rule in values {
        match rule {
            Rule::ForeignKey(n) | Rule::Check(n) | Rule::Unique(n) => name(n)?,
            Rule::DeletedCircle | Rule::OtherAudience => {}
        }
    }
    Ok(())
}
