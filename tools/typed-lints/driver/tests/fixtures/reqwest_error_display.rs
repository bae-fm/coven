//! Where `clippy::reqwest_error_display` finds a `reqwest::Error` printed. A
//! line that ends in `// finding` holds one finding, and no other line holds
//! any.

use reqwest::Error as HttpError;
use std::error::Error as _;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

pub fn format_arguments(error: &reqwest::Error) -> Vec<String> {
    vec![
        format!("{error}"),     // finding
        format!("{:?}", error), // finding
        format!("{}", &&error), // finding
        format!("{}", error.url),
    ]
}

pub fn through_an_alias(error: HttpError) -> String {
    format!("{error}") // finding
}

pub fn printing_calls(error: &reqwest::Error, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let _ = error.to_string(); // finding
    let _ = [error].map(ToString::to_string); // finding
    fmt::Debug::fmt(error, f) // finding
}

pub fn around_the_error(
    boxed: Box<reqwest::Error>,
    shared: Rc<reqwest::Error>,
    synced: Arc<reqwest::Error>,
    maybe: Option<reqwest::Error>,
    several: Vec<reqwest::Error>,
    sent: Result<(), reqwest::Error>,
) -> Vec<String> {
    vec![
        format!("{boxed}"),                 // finding
        format!("{shared}"),                // finding
        format!("{synced}"),                // finding
        format!("{maybe:?}"),               // finding
        format!("{several:?}"),             // finding
        format!("{sent:?}"),                // finding
        format!("{:?}", (1, &several[..])), // finding
    ]
}

/// What thiserror writes for `#[error(transparent)]` and `#[error("{0}")]`.
pub enum Failure {
    Transparent(reqwest::Error),
    Plain(reqwest::Error),
    Chained(reqwest::Error),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transparent(error) => fmt::Display::fmt(error, f), // finding
            Self::Plain(error) => write!(f, "sending: {error}"),     // finding
            Self::Chained(error) => write!(f, "sending: {}", CauseChain(error)),
        }
    }
}

/// What `#[derive(Debug)]` writes for a field: it hands `&dyn Debug` to the
/// formatter.
pub struct Holder(reqwest::Error);

impl fmt::Debug for Holder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Holder").field(&self.0).finish() // finding
    }
}

/// A cause chain takes the error as `&dyn Error`, so its own printing names no
/// `reqwest::Error`.
pub struct CauseChain<'a>(pub &'a (dyn std::error::Error + 'static));

impl fmt::Display for CauseChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cause = Some(self.0);
        while let Some(error) = cause {
            write!(f, "{error}: ")?;
            cause = error.source();
        }
        Ok(())
    }
}

/// A type of another crate's, or of this one, named `Error` is not reqwest's.
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("another error")
    }
}

pub fn other_errors(error: Error, io: std::io::Error) -> String {
    format!("{error} {io}")
}

#[expect(clippy::reqwest_error_display, reason = "the test of a mark")]
pub fn marked(error: &reqwest::Error) -> String {
    format!("{error}")
}
