use super::*;

#[test]
fn uuid_and_circle_readers_refuse_noncanonical_text_and_other_sql_types() {
    for text in [
        "F47AC10B-58CC-4372-A567-0E02B2C3D479",
        "f47ac10b58cc4372a5670e02b2c3d479",
        "{f47ac10b-58cc-4372-a567-0e02b2c3d479}",
        "store",
        "",
    ] {
        let value = ValueRef::Text(text.as_bytes());
        assert!(UuidText::column_result(value).is_err(), "{text}");
        assert!(CircleId::column_result(value).is_err(), "{text}");
    }
    for value in [
        ValueRef::Null,
        ValueRef::Integer(1),
        ValueRef::Blob(&[0; 16]),
    ] {
        assert!(matches!(
            UuidText::column_result(value),
            Err(FromSqlError::InvalidType)
        ));
        assert!(matches!(
            CircleId::column_result(value),
            Err(FromSqlError::InvalidType)
        ));
    }
}

#[test]
fn member_file_and_device_ids_round_trip_their_sql_forms() {
    let member = MemberId(Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap());
    let text = member.to_sql().unwrap();
    let ToSqlOutput::Owned(Value::Text(text)) = text else {
        panic!("a member id binds as text")
    };
    assert_eq!(text, "f47ac10b-58cc-4372-a567-0e02b2c3d479");
    assert_eq!(
        MemberId::column_result(ValueRef::Text(text.as_bytes())).unwrap(),
        member
    );
    assert!(MemberId::column_result(ValueRef::Blob(member.0.as_bytes())).is_err());
    let file = FileId(member.0);
    assert_eq!(file.to_sql().unwrap(), member.to_sql().unwrap());
    assert_eq!(
        FileId::column_result(ValueRef::Text(text.as_bytes())).unwrap(),
        file
    );
    assert!(FileId::column_result(ValueRef::Blob(file.0.as_bytes())).is_err());

    // The top bit set: an SQL integer would read it as negative.
    let device = DeviceId(0x8000_0000_0000_0001);
    let ToSqlOutput::Owned(Value::Blob(bytes)) = device.to_sql().unwrap() else {
        panic!("a device id binds as a blob")
    };
    assert_eq!(bytes, [0x80, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(
        DeviceId::column_result(ValueRef::Blob(&bytes)).unwrap(),
        device
    );
    assert!(matches!(
        DeviceId::column_result(ValueRef::Blob(&[1; 7])),
        Err(FromSqlError::InvalidBlobSize {
            expected_size: 8,
            blob_size: 7
        })
    ));
    assert!(matches!(
        DeviceId::column_result(ValueRef::Integer(1)),
        Err(FromSqlError::InvalidType)
    ));
}
