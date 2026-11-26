use crate::generated::{optional_json_column, sqlive_reads::rusqlite::Connection};

#[test]
fn an_optional_json_column_reads_null_as_none() {
    let connection = Connection::open_in_memory().unwrap();
    for (sql, expected) in
        [("SELECT NULL", None), ("SELECT 'null'", None), ("SELECT '[1]'", Some(vec![1]))]
    {
        let read =
            connection.query_row(sql, [], |row| optional_json_column::<Vec<i64>>(row, 0)).unwrap();
        assert_eq!(read, expected, "{sql}");
    }
}
