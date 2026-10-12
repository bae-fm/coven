//! Implicit Rust 2021 imports. These bindings participate in ordinary name
//! resolution and can be shadowed; none grants construction authority.
//!
//! The workspace uses edition 2021. Paths follow std's `prelude::rust_2021`
//! and the language's primitive prelude; macros use a separate namespace.

pub(crate) const IMPORTS: &str = r#"
use std::marker::{Copy, Send, Sized, Sync, Unpin};
use std::ops::{Drop, Fn, FnMut, FnOnce, AsyncFn, AsyncFnMut, AsyncFnOnce};
use std::mem::{drop, align_of, align_of_val, size_of, size_of_val};
use std::clone::Clone;
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::convert::{AsMut, AsRef, From, Into, TryFrom, TryInto};
use std::default::Default;
use std::iter::{DoubleEndedIterator, ExactSizeIterator, Extend, IntoIterator, Iterator, FromIterator};
use std::option::Option::{self, None, Some};
use std::result::Result::{self, Err, Ok};
use std::borrow::ToOwned;
use std::boxed::Box;
use std::string::{String, ToString};
use std::vec::Vec;
use core::primitive::{bool, char, str, f32, f64, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize};
"#;
