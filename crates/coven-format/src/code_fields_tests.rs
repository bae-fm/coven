use super::*;

fn fields(writer: &mut FieldWriter) -> Result<(), Error> {
    writer.u8(3)?;
    writer.u16(0x0102)?;
    writer.text("bucket")?;
    writer.option(Some("secret"), |writer, value| writer.text(value))?;
    writer.option(None::<&str>, |writer, value| writer.text(value))
}

#[test]
fn packed_fields_use_the_format_primitives_in_one_exact_allocation() {
    let packed = pack(fields).unwrap();
    assert_eq!(
        packed.as_bytes(),
        b"\x03\x01\x02\0\0\0\x06bucket\x01\0\0\0\x06secret\0"
    );
    assert_eq!(packed.capacity(), packed.as_bytes().len());
    let mut reader = FieldReader::new(packed.as_bytes()).unwrap();
    assert_eq!(reader.u8().unwrap(), 3);
    assert_eq!(reader.u16().unwrap(), 0x0102);
    assert_eq!(reader.text("bucket").unwrap(), "bucket");
    let secret = reader.option(|reader| reader.secret_text("key")).unwrap();
    assert_eq!(secret.unwrap().as_str(), "secret");
    assert!(reader
        .option(|reader| reader.text("label"))
        .unwrap()
        .is_none());
    reader.finish().unwrap();
}

#[test]
fn truncated_trailing_and_malformed_fields_are_refused() {
    let packed = pack(fields).unwrap();
    let bytes = packed.as_bytes();
    let read = |bytes: &[u8]| -> Result<(), Error> {
        let mut reader = FieldReader::new(bytes)?;
        reader.u8()?;
        reader.u16()?;
        reader.text("bucket")?;
        reader.option(|reader| reader.secret_text("key"))?;
        reader.option(|reader| reader.text("label"))?;
        reader.finish()
    };
    read(bytes).unwrap();
    for end in 0..bytes.len() {
        assert_eq!(read(&bytes[..end]), Err(Error::Truncated), "{end}");
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert_eq!(read(&trailing), Err(Error::TrailingBytes));
    let mut tag = bytes.to_vec();
    tag[13] = 2;
    assert_eq!(
        read(&tag),
        Err(Error::UnknownTag {
            field: "option",
            tag: 2
        })
    );
    let mut length = bytes.to_vec();
    length[3..7].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(read(&length), Err(Error::Limit { .. })));
    let mut utf8 = bytes.to_vec();
    utf8[7] = 0xff;
    assert_eq!(read(&utf8), Err(Error::Utf8));
}

#[test]
fn a_storage_part_is_bounded_on_both_sides() {
    let long = "x".repeat(MAX_STORAGE);
    assert!(matches!(
        pack(|writer| writer.text(&long)),
        Err(Error::Limit { .. })
    ));
    pack(|writer| writer.text(&long[..MAX_STORAGE - 4])).unwrap();
    assert!(matches!(
        FieldReader::new(&vec![0; MAX_STORAGE + 1]),
        Err(Error::Limit { .. })
    ));
}
