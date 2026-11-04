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
