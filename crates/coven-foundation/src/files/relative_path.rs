//! A path an app names below a folder the person picked, as names, and the
//! name coven writes for each: one rule, the same on every platform, that
//! makes a name valid on any volume a copy can land on (§17.3).

use std::fmt;
use std::path::PathBuf;

/// A path below a folder, as the names the app gives, outermost first: an
/// album's name, then a track's. Each name is one name on every platform,
/// whatever it holds, `/` and `\` included. Coven writes each through one
/// rule that makes it valid on any volume (§17.3), so joined to the folder
/// the path stays inside it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RelativePath(Vec<String>);

/// Why names can't be a `RelativePath`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelativePathError {
    /// The path has no names, or a name is empty.
    #[error("the path or one of its names is empty")]
    Empty,
}

impl RelativePath {
    /// The path of `names`, outermost first. Each must be non-empty.
    pub fn new<S: Into<String>>(
        names: impl IntoIterator<Item = S>,
    ) -> Result<Self, RelativePathError> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        if names.is_empty() || names.iter().any(String::is_empty) {
            return Err(RelativePathError::Empty);
        }
        Ok(Self(names))
    }

    /// Its names as the app gave them, outermost first.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    /// The path coven writes for it below a folder: each name as the one
    /// rule for every platform makes it (§17.3), joined by this platform's
    /// separator. An app writing a file of its own under a name it stores
    /// writes it here, so its names land as coven's copies do.
    pub fn local_path(&self) -> PathBuf {
        self.names().map(local_name).collect()
    }

    /// The path of its first `count` names.
    pub(super) fn first(&self, count: usize) -> Self {
        Self(self.0[..count].to_vec())
    }
}

/// The names joined by `/`, for showing only: a name may hold `/` itself.
impl fmt::Display for RelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join("/"))
    }
}

/// The longest name coven writes: 255 bytes of UTF-8, the limit of ext4,
/// f2fs and Android's shared storage, which also covers the 255 UTF-16
/// units of NTFS, exFAT, FAT and APFS, since no character takes more UTF-16
/// units than UTF-8 bytes. HFS+ counts 255 UTF-16 units of the name
/// decomposed, where `Ǖ`, two bytes of UTF-8, takes three, so that counts
/// too.
const NAME_LIMIT: usize = 255;

/// The name coven writes for `name`, the same on every platform, since what
/// a name may hold is the volume's rule, not the platform's (§17.3):
///
/// 1. `" * : < > ? \ | /`, which Windows, Android's shared storage and FAT
///    volumes refuse, become their full-width lookalikes; control characters
///    and DEL become their pictures, `␀`–`␟` and `␡`.
/// 2. A last `.` becomes `．` and a last space `␠`, since Windows and FAT
///    drop them. So `.` and `..` are ordinary names, `．` and `.．`.
/// 3. A Windows device name, `CON.flac` among them, has its letters in full
///    width: `ＣＯＮ.flac`.
/// 4. A name past `NAME_LIMIT` keeps its extension and loses whole
///    characters, as a person reads them, from the end of the part before
///    it, with `…` where it was cut.
///
/// The result is never empty, `.` or `..`, and holds no separator, so it is
/// one name inside the folder on every platform.
pub(super) fn local_name(name: &str) -> String {
    let mut local: String = name.chars().map(lookalike).collect();
    if let Some(last) = local.pop() {
        local.push(match last {
            '.' => '．',
            ' ' => '␠',
            last => last,
        });
    }
    let device = local[..local.find('.').unwrap_or(local.len())].trim_end_matches(' ');
    if windows_device(device) {
        let wide: String = device.chars().map(full_width).collect();
        let end = device.len();
        local.replace_range(..end, &wide);
    }
    fit(local)
}

/// Whether `base`, a name's part before its first `.` without trailing
/// spaces, names a Windows device: Windows opens the device for it, with any
/// extension on some versions, and in any ASCII case. `¹`, `²` and `³` count
/// as digits.
pub(super) fn windows_device(base: &str) -> bool {
    let base = base.to_ascii_uppercase();
    if matches!(
        base.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) {
        return true;
    }
    let Some(number) = base
        .strip_prefix("COM")
        .or_else(|| base.strip_prefix("LPT"))
    else {
        return false;
    };
    let mut number = number.chars();
    matches!(
        (number.next(), number.next()),
        (Some('1'..='9' | '¹' | '²' | '³'), None)
    )
}

/// A character no volume of the four platforms refuses in a name.
fn lookalike(character: char) -> char {
    match character {
        '"' | '*' | ':' | '<' | '>' | '?' | '\\' | '|' | '/' => full_width(character),
        '\0'..='\x1f' => char::from_u32(0x2400 + u32::from(character))
            .expect("Control Pictures has a picture for each control character"),
        '\x7f' => '␡',
        _ => character,
    }
}

/// An ASCII character's full-width form; anything else as it is.
fn full_width(character: char) -> char {
    match character {
        '!'..='~' => char::from_u32(u32::from(character) + 0xFEE0)
            .expect("Halfwidth and Fullwidth Forms has each printable ASCII character"),
        _ => character,
    }
}

/// `name`, cut to `NAME_LIMIT` if it is longer. The extension, from the
/// last `.`, stays when at least one character before it fits too;
/// otherwise the name is cut from its own end. A cut falls between
/// characters as a person reads them (grapheme clusters), so an accent is
/// never cut from its letter, except within one such character too long
/// for a name by itself. The ending `…` is neither dot nor space, so a cut
/// name stays valid.
fn fit(name: String) -> String {
    if fits(&name) {
        return name;
    }
    let (stem, extension) = match name.rfind('.') {
        Some(dot) if dot > 0 => name.split_at(dot),
        _ => (name.as_str(), ""),
    };
    let graphemes = |text: &str| -> Vec<usize> {
        icu_segmenter::GraphemeClusterSegmenter::new()
            .segment_str(text)
            .collect()
    };
    let characters = |text: &str| -> Vec<usize> {
        text.char_indices()
            .map(|(index, _)| index)
            .chain([text.len()])
            .collect()
    };
    cut(stem, extension, &graphemes(stem))
        .or_else(|| cut(&name, "", &graphemes(&name)))
        .or_else(|| cut(&name, "", &characters(&name)))
        .expect("one character, `…` and nothing else fit in a name")
}

/// The longest of `text` up to one of `ends`, then `…` and `extension`,
/// that fits; none when no end past the start does.
fn cut(text: &str, extension: &str, ends: &[usize]) -> Option<String> {
    let room = NAME_LIMIT.checked_sub('…'.len_utf8() + extension.len())?;
    ends.iter()
        .rev()
        .filter(|&&end| end > 0 && end <= room)
        .map(|&end| format!("{}…{extension}", &text[..end]))
        .find(|candidate| fits(candidate))
}

/// Whether `name` is within `NAME_LIMIT` in UTF-8 bytes and, decomposed,
/// in UTF-16 units.
fn fits(name: &str) -> bool {
    name.len() <= NAME_LIMIT
        && icu_normalizer::DecomposingNormalizerBorrowed::new_nfd()
            .normalize(name)
            .encode_utf16()
            .count()
            <= NAME_LIMIT
}

#[cfg(test)]
#[path = "relative_path_tests.rs"]
mod tests;
