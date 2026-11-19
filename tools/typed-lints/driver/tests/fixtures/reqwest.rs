//! Stands in for reqwest in the lint's tests: the lint knows reqwest's error by
//! the path `reqwest::Error`, which reqwest declares in its `error` module.

mod error {
    use std::fmt;

    pub struct Error {
        pub url: String,
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "error sending request for url ({})", self.url)
        }
    }

    impl fmt::Debug for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Error").field("url", &self.url).finish()
        }
    }

    impl std::error::Error for Error {}
}

pub use error::Error;
