//! A rule violation and the change it asks the author to make.

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Finding {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) message: String,
    pub(crate) remedy: &'static str,
}

impl Finding {
    pub(crate) fn new(
        path: &str,
        line: usize,
        message: impl Into<String>,
        remedy: &'static str,
    ) -> Self {
        Self {
            path: path.into(),
            line,
            message: message.into(),
            remedy,
        }
    }
}
