//! A lint's finding as rustc's diagnostic: a message and a help line. clippy's
//! own helpers add a link to clippy's documentation, which has no page for
//! these lints.

use rustc_errors::{Diag, DiagCtxtHandle, Diagnostic, Level};

pub(crate) struct Finding {
    pub(crate) message: String,
    pub(crate) help: &'static str,
}

impl<'a> Diagnostic<'a> for Finding {
    fn into_diag(self, dcx: DiagCtxtHandle<'a>, level: Level) -> Diag<'a> {
        let mut diag = Diag::new(dcx, level, self.message);
        diag.help(self.help);
        diag
    }
}
