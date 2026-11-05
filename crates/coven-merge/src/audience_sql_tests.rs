use super::*;
use rusqlite::types::Value;

#[test]
fn an_audience_reads_back_only_from_the_text_it_writes() {
    let circle =
        Audience::Circle(CircleId(uuid::Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff)));
    for audience in [Audience::Store, circle.clone()] {
        let text = audience.to_string();
        assert_eq!(text.parse::<Audience>(), Ok(audience.clone()));
        assert_eq!(Audience::column_result(ValueRef::Text(text.as_bytes())).unwrap(), audience);
    }
    for other in [
        "Store",
        "STORE",
        "",
        "00112233-4455-6677-8899-AABBCCDDEEFF",
        "00112233445566778899aabbccddeeff",
        "{00112233-4455-6677-8899-aabbccddeeff}",
    ] {
        assert_eq!(other.parse::<Audience>(), Err(InvalidAudience), "{other}");
        let value = Value::Text(other.to_owned());
        assert!(Audience::column_result(ValueRef::from(&value)).is_err(), "{other}");
    }
}

/// An audience binds as the canonical text it reads back from (§15).
#[test]
fn an_audience_binds_as_the_text_it_reads_back() {
    let id = uuid::Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff);
    let circle = Audience::Circle(CircleId(id));
    for (audience, text) in
        [(Audience::Store, "store"), (circle, "00112233-4455-6677-8899-aabbccddeeff")]
    {
        let output = audience.to_sql().unwrap();
        let ToSqlOutput::Owned(Value::Text(bound)) = output else {
            panic!("an audience binds as owned text, got {output:?}");
        };
        assert_eq!(bound, text);
        assert_eq!(Audience::column_result(ValueRef::Text(bound.as_bytes())).unwrap(), audience);
    }
}
