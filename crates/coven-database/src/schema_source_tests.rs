use super::*;

#[test]
fn source_ranges_keep_constraint_spelling() {
    let mut source =
        SchemaSource::new("CREATE TABLE t (id TEXT, CHECK /* a */ (id <> 'A,B')) STRICT");
    let body = source.body();
    let parts = body.parts();
    assert_eq!(parts[0].text(), "id TEXT");
    assert_eq!(parts[1].text(), " CHECK /* a */ (id <> 'A,B')");
    assert_eq!(source.rest(), "STRICT");
}
