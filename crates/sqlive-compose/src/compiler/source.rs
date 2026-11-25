//! A crate's macro calls, found by walking its modules from `src/lib.rs`
//! through the source reader the build script hands in.

use std::io;

use syn::{Attribute, Expr, Item, Lit, Meta};

use super::error::{Cause, CompileError, Location};
use super::header::{self, CallKind, Header, HeaderError};

/// A source reader: the text of the file at a path below the crate's folder.
pub(crate) type Reader<'a> = &'a dyn Fn(&str) -> io::Result<String>;

/// One `fragment!` or `read!` call.
pub(crate) struct Call {
    /// Where the call is.
    pub(crate) location: Location,
    /// The modules from the crate's root to the call's.
    pub(crate) module: Vec<String>,
    /// The call's name, which names its file.
    pub(crate) name: String,
    /// The header and SQL, or why the tokens aren't one.
    pub(crate) header: Result<Header, HeaderError>,
}

/// Every call of a crate, and every file read to find them.
pub(crate) struct Walked {
    pub(crate) calls: Vec<Call>,
    pub(crate) read: Vec<String>,
}

/// The calls of the crate whose files `reader` gives.
pub(crate) fn walk(reader: Reader<'_>) -> Result<Walked, Vec<CompileError>> {
    let mut walk = Walk { reader, calls: Vec::new(), read: Vec::new(), errors: Vec::new() };
    walk.file("src/lib.rs", "src", &mut Vec::new());
    if walk.errors.is_empty() {
        Ok(Walked { calls: walk.calls, read: walk.read })
    } else {
        Err(walk.errors)
    }
}

struct Walk<'a> {
    reader: Reader<'a>,
    calls: Vec<Call>,
    read: Vec<String>,
    errors: Vec<CompileError>,
}

impl Walk<'_> {
    fn fail(&mut self, cause: Cause) {
        self.errors.push(CompileError { location: None, concerns: String::new(), cause });
    }

    /// The calls of the module file at `path`, whose child modules' files
    /// are in `dir`.
    fn file(&mut self, path: &str, dir: &str, module: &mut Vec<String>) {
        let text = match (self.reader)(path) {
            Ok(text) => text,
            Err(source) => return self.fail(Cause::SourceRead { path: path.to_owned(), source }),
        };
        self.read.push(path.to_owned());
        match syn::parse_file(&text) {
            Ok(file) => self.items(&file.items, path, dir, module),
            Err(error) => {
                self.fail(Cause::SourceSyntax {
                    path: path.to_owned(),
                    message: error.to_string(),
                });
            }
        }
    }

    fn items(&mut self, items: &[Item], file: &str, dir: &str, module: &mut Vec<String>) {
        for item in items {
            match item {
                Item::Macro(item) => {
                    let segments: Vec<String> = item
                        .mac
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect();
                    let kind = match segments.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                        ["sqlive_compose", "fragment"] => CallKind::Fragment,
                        ["sqlive_compose", "read"] => CallKind::Read,
                        _ => continue,
                    };
                    let start = item.mac.path.segments[0].ident.span().start();
                    let location = Location {
                        file: file.to_owned(),
                        line: start.line,
                        column: start.column + 1,
                    };
                    let name =
                        item.mac.tokens.clone().into_iter().next().map(|token| token.to_string());
                    let header = header::parse(kind, item.mac.tokens.clone());
                    self.calls.push(Call {
                        location,
                        module: module.clone(),
                        name: name.unwrap_or_default(),
                        header,
                    });
                }
                Item::Mod(item) if !is_test_only(&item.attrs) => {
                    let name = item.ident.to_string();
                    module.push(name.clone());
                    match &item.content {
                        Some((_, items)) => {
                            self.items(items, file, &format!("{dir}/{name}"), module);
                        }
                        None => self.module_file(&item.attrs, &name, file, dir, module),
                    }
                    module.pop();
                }
                _ => {}
            }
        }
    }

    /// The file of `mod name;`, declared in `file`, whose modules' files are
    /// in `dir`.
    fn module_file(
        &mut self,
        attributes: &[Attribute],
        name: &str,
        file: &str,
        dir: &str,
        module: &mut Vec<String>,
    ) {
        if let Some(path) = path_attribute(attributes) {
            let folder = file.rsplit_once('/').map_or("", |(folder, _)| folder);
            let path = if folder.is_empty() { path } else { format!("{folder}/{path}") };
            let child_dir =
                path.rsplit_once('/').map_or(String::new(), |(folder, _)| folder.to_owned());
            return self.file(&path, &child_dir, module);
        }
        let flat = format!("{dir}/{name}.rs");
        let nested = format!("{dir}/{name}/mod.rs");
        let child_dir = format!("{dir}/{name}");
        match (self.reader)(&flat) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.file(&nested, &child_dir, module);
            }
            _ => self.file(&flat, &child_dir, module),
        }
    }
}

/// Whether `attributes` hold `#[cfg(test)]`.
fn is_test_only(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        matches!(&attribute.meta, Meta::List(list)
            if list.path.is_ident("cfg") && list.tokens.to_string() == "test")
    })
}

/// The path of a `#[path = "…"]` attribute.
fn path_attribute(attributes: &[Attribute]) -> Option<String> {
    attributes.iter().find_map(|attribute| match &attribute.meta {
        Meta::NameValue(pair) if pair.path.is_ident("path") => match &pair.value {
            Expr::Lit(literal) => match &literal.lit {
                Lit::Str(path) => Some(path.value()),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    })
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
