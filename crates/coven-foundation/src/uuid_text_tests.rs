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
