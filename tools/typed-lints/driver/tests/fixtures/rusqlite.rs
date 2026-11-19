//! Stands in for rusqlite in the lints' tests: the lints know its raw handles
//! and its value conversions by their paths, which rusqlite declares as here.

pub struct Connection;

impl Connection {
    pub fn open_in_memory() -> Connection {
        Connection
    }

    pub fn changes(&self) -> usize {
        0
    }
}

pub struct Transaction<'a>(pub &'a Connection);

pub struct Savepoint<'a>(pub &'a Connection);

pub mod session {
    pub struct Session<'a>(pub &'a super::Connection);
}

pub struct Row<'a>(pub &'a Connection);

impl Row<'_> {
    pub fn get<T: types::FromSql>(&self, index: usize) -> Result<T> {
        T::column_result(types::ValueRef::Integer(index as i64))
            .map_err(|_| Error::QueryReturnedNoRows)
    }
}

pub mod types {
    pub enum Type {
        Null,
        Integer,
        Text,
    }

    pub enum Value {
        Null,
        Integer(i64),
        Text(String),
    }

    pub enum ValueRef<'a> {
        Null,
        Integer(i64),
        Text(&'a [u8]),
    }

    impl<'a> ValueRef<'a> {
        pub fn as_str(&self) -> FromSqlResult<&'a str> {
            match *self {
                ValueRef::Text(text) => {
                    std::str::from_utf8(text).map_err(|_| FromSqlError::InvalidType)
                }
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    pub enum ToSqlOutput<'a> {
        Borrowed(ValueRef<'a>),
        Owned(Value),
    }

    pub enum FromSqlError {
        InvalidType,
    }

    pub type FromSqlResult<T> = Result<T, FromSqlError>;

    pub trait ToSql {
        fn to_sql(&self) -> crate::Result<ToSqlOutput<'_>>;
    }

    pub trait FromSql: Sized {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self>;
    }

    impl FromSql for String {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            value.as_str().map(str::to_string)
        }
    }
}

pub use types::ToSql;

pub enum Error {
    QueryReturnedNoRows,
    InvalidColumnType(usize, String, types::Type),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SQLite's own functions, which rusqlite re-exports from libsqlite3-sys.
pub mod ffi {
    pub fn sqlite3_initialize() -> i32 {
        0
    }
}
