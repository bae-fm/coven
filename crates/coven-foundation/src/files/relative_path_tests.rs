use super::*;

/// The part of `name` Windows checks for a device: before its first `.`,
/// without trailing spaces.
fn base(name: &str) -> &str {
    name[..name.find('.').unwrap_or(name.len())].trim_end_matches(' ')
}

#[test]
fn a_relative_path_is_the_names_the_app_gives() {
    let album = RelativePath::new(["AC/DC", "Back in Black", r"Caf\xE9.flac"]).unwrap();
    assert_eq!(
        album.names().collect::<Vec<_>>(),
        ["AC/DC", "Back in Black", r"Caf\xE9.flac"]
    );
    assert_eq!(album.first(1).names().collect::<Vec<_>>(), ["AC/DC"]);
    assert_eq!(album.to_string(), r"AC/DC/Back in Black/Caf\xE9.flac");
    assert_eq!(
        RelativePath::new(Vec::<String>::new()),
        Err(RelativePathError::Empty)
    );
    assert_eq!(
        RelativePath::new(["album", ""]),
        Err(RelativePathError::Empty)
    );
}

/// A path coven writes is each of its names made valid, so it stays inside
/// the folder it is joined to whatever its names hold.
#[test]
fn a_local_path_is_each_name_as_coven_writes_it() {
    let path = RelativePath::new(["..", r"Caf\xE9", "Why?.flac"]).unwrap();
    assert_eq!(
        path.local_path(),
        [".．", "Caf＼xE9", "Why？.flac"]
            .iter()
            .collect::<std::path::PathBuf>()
    );
    assert_eq!(
        path.local_path().components().count(),
        3,
        "{:?}",
        path.local_path()
    );
}

/// What Windows, Android's shared storage and FAT volumes refuse in a name
/// becomes its lookalike, on every platform.
#[test]
fn each_refused_character_becomes_its_lookalike() {
    for (synced, local) in [
        (r"Caf\xE9.flac", "Caf＼xE9.flac"),
        ("AC/DC", "AC／DC"),
        ("Live: Tokyo.flac", "Live： Tokyo.flac"),
        ("Why?.flac", "Why？.flac"),
        ("\"Heroes\".flac", "＂Heroes＂.flac"),
        ("<3>.flac", "＜3＞.flac"),
        ("A*B|C.flac", "A＊B｜C.flac"),
        ("C:track.flac", "C：track.flac"),
        (r"\\server\share", "＼＼server＼share"),
        ("Tab\tName.flac", "Tab␉Name.flac"),
        ("Rub\x7fout.flac", "Rub␡out.flac"),
    ] {
        assert_eq!(local_name(synced), local, "{synced:?}");
    }
    for control in 0..0x20_u32 {
        let character = char::from_u32(control).unwrap();
        let picture = char::from_u32(0x2400 + control).unwrap();
        assert_eq!(
            local_name(&format!("a{character}b")),
            format!("a{picture}b")
        );
    }
}

/// Windows and FAT drop a last dot or space, so it becomes its lookalike;
/// only the last character changes.
#[test]
fn a_last_dot_or_space_becomes_its_lookalike() {
    for (synced, local) in [
        ("Album.", "Album．"),
        ("So Long...", "So Long..．"),
        (".", "．"),
        ("..", ".．"),
        ("Album ", "Album␠"),
        ("Album. ", "Album.␠"),
        ("Album .", "Album ．"),
    ] {
        assert_eq!(local_name(synced), local, "{synced:?}");
    }
}

/// A name Windows keeps for a device, with any extension and in any case,
/// has its letters in full width; names that only look close stay.
#[test]
fn a_windows_device_name_has_its_letters_in_full_width() {
    for (synced, local) in [
        ("CON.flac", "ＣＯＮ.flac"),
        ("con", "ｃｏｎ"),
        ("Prn.txt", "Ｐｒｎ.txt"),
        ("nul.tar.gz", "ｎｕｌ.tar.gz"),
        ("aux .flac", "ａｕｘ .flac"),
        ("Com1.mp3", "Ｃｏｍ１.mp3"),
        ("LPT9", "ＬＰＴ９"),
        ("LPT².flac", "ＬＰＴ².flac"),
        ("com¹", "ｃｏｍ¹"),
        ("CONIN$", "ＣＯＮＩＮ＄"),
        ("conout$.log", "ｃｏｎｏｕｔ＄.log"),
    ] {
        assert_eq!(local_name(synced), local, "{synced:?}");
    }
    for name in [
        "COM0",
        "COM10",
        "LPT",
        "CONSOLE.flac",
        "ACON",
        "CON.",
        "CON ",
        "CON:x",
        "ＣＯＮ",
        ".CON",
    ] {
        assert!(!windows_device(base(&local_name(name))), "{name:?}");
    }
    assert_eq!(local_name("CON."), "CON．");
}

#[test]
fn other_names_stay_as_given() {
    for name in [
        "01 Come Together.flac",
        "Björk",
        "Bjo\u{308}rk",
        "Sigur Rós",
        "なぜ？",
        "Straße",
        ".hidden",
        " leading space",
        "...And Justice for All",
        "👨‍👩‍👧",
        &"a".repeat(251),
        &format!("{}.flac", "a".repeat(250)),
    ] {
        assert_eq!(local_name(name), name, "{name:?}");
    }
}

/// A long name keeps its extension and loses whole characters before it,
/// with `…` where it was cut, within 255 bytes of UTF-8 and 255 UTF-16
/// units decomposed.
#[test]
fn a_long_name_is_cut_before_its_extension() {
    let cut = local_name(&format!("{}.flac", "a".repeat(300)));
    assert_eq!(cut, format!("{}….flac", "a".repeat(247)));
    assert_eq!(cut.len(), 255);
    // Mapped characters count as they are written: each `?` takes 3 bytes.
    assert_eq!(
        local_name(&format!("{}.flac", "?".repeat(100))),
        format!("{}….flac", "？".repeat(82))
    );
    // `é` as `e` and a combining accent is never split.
    let accented = local_name(&format!("{}.flac", "e\u{301}".repeat(150)));
    assert_eq!(accented, format!("{}….flac", "e\u{301}".repeat(82)));
    // Nor is a family, three people joined into one character.
    let family = "👨‍👩‍👧";
    let families = local_name(&format!("{}.flac", family.repeat(20)));
    assert_eq!(families, format!("{}….flac", family.repeat(13)));
    // `Ǖ` is two bytes of UTF-8 and three UTF-16 units decomposed, as HFS+
    // counts it: there the decomposed limit comes first.
    assert_eq!(
        local_name(&format!("{}.flac", "\u{1d5}".repeat(200))),
        format!("{}….flac", "\u{1d5}".repeat(83))
    );
    // An extension too long to keep: the name is cut from its own end.
    let long_extension = format!("a.{}", "b".repeat(300));
    assert_eq!(
        local_name(&long_extension),
        format!("a.{}…", "b".repeat(250))
    );
    // One character as a person reads it, too long for a name by itself,
    // is cut between its code points.
    assert_eq!(
        local_name(&format!("e{}", "\u{301}".repeat(200))),
        format!("e{}…", "\u{301}".repeat(125))
    );
}

/// Every name coven writes is one name every volume holds, and the same
/// name gives the same result every time.
#[test]
fn every_written_name_is_valid_everywhere() {
    let pieces = [
        "a",
        ".",
        " ",
        ":",
        "\\",
        "/",
        "?",
        "CON",
        "com1",
        "lpt²",
        "$",
        "é",
        "e\u{301}",
        "\u{1d5}",
        "👨‍👩‍👧",
        "\0",
        "\x1f",
        "\x7f",
        "？",
        "．",
    ];
    let mut names: Vec<String> = Vec::new();
    for first in pieces {
        for second in pieces {
            for third in pieces {
                names.push(format!("{first}{second}{third}"));
            }
        }
        names.push(first.repeat(300));
        names.push(format!("{}.flac", first.repeat(300)));
        names.push(format!("x.{}", first.repeat(300)));
    }
    for name in names {
        let local = local_name(&name);
        assert_eq!(local, local_name(&name), "{name:?}");
        assert!(!local.is_empty(), "{name:?}");
        assert!(local != "." && local != "..", "{name:?}");
        assert!(
            !local.chars().any(|character| matches!(
                character,
                '"' | '*' | ':' | '<' | '>' | '?' | '\\' | '|' | '/' | '\0'..='\x1f' | '\x7f'
            )),
            "{name:?}: {local:?}"
        );
        assert!(
            !local.ends_with('.') && !local.ends_with(' '),
            "{name:?}: {local:?}"
        );
        assert!(!windows_device(base(&local)), "{name:?}: {local:?}");
        assert!(fits(&local), "{name:?}: {local:?}");
    }
}
