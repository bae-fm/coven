//! Random live queries and writes over a schema whose joins, views,
//! subqueries and conditions sqlive narrows by, so a test can check that a
//! live query runs again whenever a commit changes what it returns. sqlive's
//! own test runs them over plain tables, and a host's test runs them over the
//! host's own writes. A seed reproduces a case.

use crate::SqlValue;

/// A small deterministic generator, so a seed reproduces a case.
pub struct Rng(u64);

impl Rng {
    /// The generator of `seed`'s case.
    pub fn seeded(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number below `bound`.
    pub fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    /// One of `items`.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    /// True `percent` times in a hundred.
    pub fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// Tables keyed by integer, and a captured one keyed by text, with columns
/// whose small domains make values collide, and text values that name the
/// captured table's keys, so joins find rows.
pub const SCHEMA: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, n TEXT COLLATE NOCASE);
CREATE TABLE s(id TEXT NOT NULL PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
CREATE VIEW tv AS SELECT id, a, b, c FROM t;
CREATE VIEW tu AS SELECT t.id, t.a, u.b, u.c FROM t JOIN u ON u.a = t.id;
CREATE VIEW tuv AS SELECT id, a, b, c FROM t UNION ALL SELECT * FROM (SELECT id, a, b, c FROM u);
CREATE VIEW grouped AS SELECT a AS id, count(*) AS a, b, min(c) AS c FROM t GROUP BY a;
CREATE VIEW lefty AS SELECT u.id, u.a, t.b, t.c FROM u LEFT JOIN t ON t.id = u.c;
CREATE VIEW sv(id, a, b, c) AS SELECT s.id, s.a, s.b, s.c FROM s WHERE s.c < 8;
CREATE INDEX u_a ON u(a);
CREATE INDEX s_b ON s(b);";

/// Views of the generated tables, each with the columns id, a, b and c, and
/// the table whose ids theirs are.
const VIEWS: [(&str, &str); 6] =
    [("tv", "t"), ("tu", "t"), ("tuv", "t"), ("grouped", "t"), ("lefty", "u"), ("sv", "s")];

/// The generated tables.
pub const TABLES: [&str; 3] = ["t", "u", "s"];

/// The table keyed by text, which a host captures, or syncs.
pub const CAPTURED: &str = "s";

/// The key `id` of a row of `table`, as SQL.
pub fn key(table: &str, id: usize) -> String {
    if table == CAPTURED {
        format!("'k{id}'")
    } else {
        id.to_string()
    }
}

/// A value of the column `a`, as SQL.
pub fn a_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "0", "1", "2", "3", "4"])).to_owned()
}

/// A value of the column `b`, as SQL.
pub fn b_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "'x'", "'k1'", "'k2'", "'k3'", "'1'"])).to_owned()
}

/// One generated live query.
pub struct Generated {
    /// Its SQL.
    pub sql: String,
    /// The values it binds.
    pub parameters: Vec<SqlValue>,
    /// Whether its WHERE holds a condition sqlive narrows by.
    pub narrows: bool,
}

/// A bound value of any type, among them ones a column would convert.
fn any_value(rng: &mut Rng) -> SqlValue {
    match rng.below(10) {
        0 => SqlValue::Null,
        1..=3 => SqlValue::Integer(rng.below(5) as i64),
        4 => SqlValue::Real(rng.below(5) as f64 + 0.5),
        5 => SqlValue::Real(rng.below(5) as f64),
        6..=8 => SqlValue::Text((*rng.pick(&["x", "X", "k*", "*1", "k1", "2", "k?"])).into()),
        _ => SqlValue::Blob(vec![b'x']),
    }
}

/// A condition on one row of `table`, named `name` in the statement, and
/// whether sqlive can narrow by it.
fn condition(
    rng: &mut Rng,
    table: &str,
    name: &str,
    parameters: &mut Vec<SqlValue>,
    depth: usize,
) -> (String, bool) {
    if depth < 2 && rng.chance(30) {
        let (left, left_narrows) = condition(rng, table, name, parameters, depth + 1);
        let (right, right_narrows) = condition(rng, table, name, parameters, depth + 1);
        return if rng.chance(60) {
            (format!("({left} AND {right})"), left_narrows || right_narrows)
        } else {
            (format!("({left} OR {right})"), left_narrows && right_narrows)
        };
    }
    let id = rng.below(10);
    match rng.below(23) {
        0 => (format!("{name}.id = {}", key(table, id)), true),
        1 => (format!("{name}.a = {}", rng.below(5)), true),
        2 => (format!("{name}.b = {}", b_value(rng)), true),
        3 => (format!("{name}.c >= {}", rng.below(10)), true),
        4 => (format!("{name}.a IN ({}, {})", rng.below(5), rng.below(5)), true),
        5 => {
            // A parameter, sometimes of a type the column would convert.
            parameters.push(if rng.chance(70) {
                SqlValue::Integer(rng.below(5) as i64)
            } else {
                SqlValue::Text(rng.below(5).to_string().into_bytes())
            });
            let narrows = matches!(parameters.last(), Some(SqlValue::Integer(_)));
            (format!("{name}.a = ?{}", parameters.len()), narrows)
        }
        6 => (format!("{name}.b <> {}", b_value(rng)), true),
        7 => {
            let column = *rng.pick(&["a", "b", "c"]);
            let spelling = *rng.pick(&["IS NULL", "ISNULL"]);
            (format!("{name}.{column} {spelling}"), true)
        }
        11 => {
            let column = *rng.pick(&["a", "b", "c"]);
            let spelling = *rng.pick(&["IS NOT NULL", "NOTNULL", "NOT NULL"]);
            (format!("{name}.{column} {spelling}"), true)
        }
        12 => {
            // A parameter that is NULL, or a value the column may convert.
            parameters.push(match rng.below(10) {
                0..=3 => SqlValue::Null,
                4..=8 => SqlValue::Integer(rng.below(5) as i64),
                _ => SqlValue::Text(rng.below(5).to_string().into_bytes()),
            });
            let narrows = !matches!(parameters.last(), Some(SqlValue::Text(_)));
            (format!("{name}.a IS ?{}", parameters.len()), narrows)
        }
        13 => (format!("{name}.a IS {}", rng.below(5)), true),
        14 => {
            let value = b_value(rng);
            let narrows = value == "NULL";
            (format!("{name}.b IS NOT {value}"), narrows)
        }
        15..=17 => {
            // The truth of a condition, compared with a value of any type.
            parameters.push(match rng.below(10) {
                0..=2 => SqlValue::Null,
                3..=7 => SqlValue::Integer(rng.below(3) as i64),
                _ => SqlValue::Text(rng.below(3).to_string().into_bytes()),
            });
            let index = parameters.len();
            let condition = match rng.below(3) {
                0 => format!("({name}.a IS NULL) = ?{index}"),
                1 => format!("({name}.b = 'x') < ?{index}"),
                _ => format!("({name}.c >= 3) IS ?{index}"),
            };
            (condition, true)
        }
        8 => (format!("abs({name}.c) = {}", rng.below(10)), true),
        9 => (format!("{name}.b LIKE {}", b_value(rng)), true),
        18 => {
            parameters.push(any_value(rng));
            (format!("lower({name}.b) = ?{}", parameters.len()), true)
        }
        19 => {
            parameters.push(any_value(rng));
            (format!("{name}.a + 1 > ?{}", parameters.len()), true)
        }
        20 => {
            parameters.push(any_value(rng));
            parameters.push(any_value(rng));
            let (first, second) = (parameters.len() - 1, parameters.len());
            (format!("CASE WHEN {name}.a = ?{first} THEN 0 ELSE 1 END = ?{second}"), true)
        }
        21 => {
            parameters.push(any_value(rng));
            parameters.push(any_value(rng));
            let (first, second) = (parameters.len() - 1, parameters.len());
            (format!("({name}.a, {name}.c) >= (?{first}, ?{second})"), true)
        }
        22 => {
            parameters.push(any_value(rng));
            (format!("{name}.b GLOB ?{}", parameters.len()), true)
        }
        _ => (format!("{name}.c < {}", rng.below(10)), true),
    }
}

/// The joins of a generated statement: its FROM clause, the qualifiers it
/// names, and conditions the joins themselves add to the WHERE.
fn joins(
    rng: &mut Rng,
    parameters: &mut Vec<SqlValue>,
) -> (String, Vec<(&'static str, &'static str)>) {
    match rng.below(22) {
        20 | 21 => {
            ("u LEFT JOIN t ON t.id = u.c WHERE t.a IS NULL".into(), vec![("u", "u"), ("t", "t")])
        }
        14..=16 => (
            "t JOIN u ON u.a = t.id JOIN s ON s.id = u.b".into(),
            vec![("t", "t"), ("u", "u"), ("s", "s")],
        ),
        17..=19 => (
            "s JOIN t ON t.b = s.id JOIN u ON u.c = t.id".into(),
            vec![("s", "s"), ("t", "t"), ("u", "u")],
        ),
        12 => ("tv JOIN s ON s.id = tv.b".into(), vec![("t", "tv"), ("s", "s")]),
        13 => (
            "s JOIN tuv ON tuv.b = s.id LEFT JOIN lefty ON lefty.c = tuv.id".into(),
            vec![("s", "s"), ("t", "tuv"), ("u", "lefty")],
        ),
        10 => ("t JOIN u ON u.n = t.b".into(), vec![("t", "t"), ("u", "u")]),
        11 => ("u JOIN t ON t.b = u.n".into(), vec![("u", "u"), ("t", "t")]),
        0 => ("t JOIN u ON u.a = t.id".into(), vec![("t", "t"), ("u", "u")]),
        1 => ("u JOIN t ON t.id = u.c".into(), vec![("u", "u"), ("t", "t")]),
        2 => ("t, s WHERE s.id = t.b".into(), vec![("t", "t"), ("s", "s")]),
        3 => ("u LEFT JOIN t ON t.id = u.c".into(), vec![("u", "u"), ("t", "t")]),
        4 => (
            "s JOIN t ON t.b = s.id LEFT JOIN u ON u.a = t.id AND u.b = 'x'".into(),
            vec![("s", "s"), ("t", "t"), ("u", "u")],
        ),
        5 => {
            let ids: Vec<String> =
                (0..1 + rng.below(3)).map(|_| rng.below(10).to_string()).collect();
            parameters.push(SqlValue::Text(format!("[{}]", ids.join(",")).into_bytes()));
            (
                format!("json_each(?{}) AS w JOIN t ON t.id = w.value", parameters.len()),
                vec![("t", "t")],
            )
        }
        6 => ("t JOIN u ON u.c = t.id + 1".into(), vec![("t", "t"), ("u", "u")]),
        7 => ("t JOIN u ON t.b = u.a".into(), vec![("t", "t"), ("u", "u")]),
        8 => (
            "t, json_each(json_array(t.b, 'k1')) AS w JOIN s ON s.id = w.value".into(),
            vec![("t", "t"), ("s", "s")],
        ),
        _ => ("t a JOIN t b ON b.c = a.c".into(), vec![("t", "a"), ("t", "b")]),
    }
}

/// A subquery over a generated table named `name`, with a condition of its
/// own and, sometimes, equalities with columns of `outer`, the sources
/// around it, and conditions on those alone, which narrow nothing; `select`
/// gives what it selects. Also whether its own conditions narrow.
fn subquery(
    rng: &mut Rng,
    name: &str,
    outer: &[(&str, &str)],
    select: &str,
    parameters: &mut Vec<SqlValue>,
) -> (String, bool) {
    let table = *rng.pick(&TABLES);
    let mut conditions = Vec::new();
    let mut narrows = false;
    if rng.chance(70) {
        let (condition, narrowing) = condition(rng, table, name, parameters, 1);
        narrows = narrowing;
        conditions.push(condition);
    }
    if !outer.is_empty() && rng.chance(80) {
        let (outer_table, outer_name) = *rng.pick(outer);
        let (correlation, narrowing) = match rng.below(5) {
            0 => (format!("{name}.a = {outer_name}.a"), true),
            1 => (format!("{name}.b = {outer_name}.b"), true),
            2 if table == "s" => (format!("{name}.id = {outer_name}.b"), true),
            3 if outer_table == "s" => (format!("{name}.b = {outer_name}.id"), true),
            3 => (format!("{name}.c = {outer_name}.id + 1"), true),
            // Columns of different affinities narrow nothing.
            _ => (format!("{name}.a = {outer_name}.b"), false),
        };
        narrows |= narrowing;
        conditions.push(correlation);
    }
    if !outer.is_empty() && rng.chance(25) {
        let (outer_table, outer_name) = *rng.pick(outer);
        let (_, other_name) = *rng.pick(outer);
        conditions.push(if other_name != outer_name && rng.chance(50) {
            format!("{outer_name}.a = {other_name}.a")
        } else {
            condition(rng, outer_table, outer_name, parameters, 1).0
        });
    }
    if rng.chance(20) {
        // A subquery inside this one, which sees this one's rows too.
        let inner = format!("{name}{name}");
        let mut around = outer.to_vec();
        around.push((table, name));
        let (nested, _) = subquery(rng, &inner, &around, "1", parameters);
        conditions.push(format!("EXISTS ({nested})"));
    }
    let filter = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    (format!("SELECT {} FROM {table} {name}{filter}", select.replace("{}", name)), narrows)
}

/// A condition holding a subquery, correlated with `sources`, with the
/// source it tests alone, or with none.
fn subquery_condition(
    rng: &mut Rng,
    sources: &[(&str, &str)],
    parameters: &mut Vec<SqlValue>,
) -> (String, bool) {
    let (tested_table, name) = *rng.pick(sources);
    let tested = [(tested_table, name)];
    let outer = match rng.below(10) {
        0..=5 => sources,
        6 | 7 => &tested,
        _ => &[],
    };
    match rng.below(6) {
        0 => {
            let (sql, narrows) = subquery(rng, "q", outer, "1", parameters);
            (format!("EXISTS ({sql})"), narrows)
        }
        1 => {
            let (sql, narrows) = subquery(rng, "q", outer, "1", parameters);
            (format!("NOT EXISTS ({sql})"), narrows)
        }
        2 => {
            // The subquery's column narrows the tested one.
            let (sql, _) = subquery(rng, "q", outer, "{}.a", parameters);
            (format!("{name}.a IN ({sql})"), true)
        }
        3 => {
            let (sql, narrows) = subquery(rng, "q", outer, "count(*)", parameters);
            (format!("{name}.c >= ({sql})"), narrows)
        }
        4 => {
            let (sql, _) = subquery(rng, "q", outer, "{}.a", parameters);
            (format!("{name}.a = ({sql} LIMIT 1)"), true)
        }
        _ => {
            let (sql, _) = subquery(rng, "q", outer, "{}.b", parameters);
            (format!("{name}.b IN ({sql})"), true)
        }
    }
}

/// A live query over the generated tables and views.
pub fn query(rng: &mut Rng) -> Generated {
    let mut parameters = Vec::new();
    match rng.below(10) {
        0 => {
            // A compound: each arm narrows by its own condition.
            let mut arms = Vec::new();
            let mut narrows = true;
            for _ in 0..2 {
                let table = *rng.pick(&TABLES);
                let (condition, narrowing) = condition(rng, table, "x", &mut parameters, 0);
                narrows &= narrowing;
                arms.push(format!("SELECT x.id, x.a FROM {table} x WHERE {condition}"));
            }
            let operator = *rng.pick(&["UNION", "UNION ALL", "EXCEPT", "INTERSECT"]);
            return Generated {
                sql: format!("{} {operator} {} ORDER BY 1, 2", arms[0], arms[1]),
                parameters,
                narrows,
            };
        }
        1 => {
            // No FROM: only what its subqueries read.
            let (exists, narrows) = subquery(rng, "q", &[], "1", &mut parameters);
            let (count, _) = subquery(rng, "p", &[], "count(*)", &mut parameters);
            return Generated {
                sql: format!("SELECT EXISTS ({exists}), ({count})"),
                parameters,
                narrows,
            };
        }
        _ => {}
    }
    let mut with = String::new();
    let (from, sources) = match rng.below(10) {
        0..=3 => {
            let table = *rng.pick(&TABLES);
            (table.to_owned(), vec![(table, table)])
        }
        4 | 5 => {
            let (view, table) = *rng.pick(&VIEWS);
            (view.to_owned(), vec![(table, view)])
        }
        6 => {
            // A subquery in FROM, with a condition of its own.
            let table = *rng.pick(&TABLES);
            let (condition, _) = condition(rng, table, "x", &mut parameters, 1);
            (
                format!("(SELECT x.id, x.a, x.b, x.c FROM {table} x WHERE {condition}) AS d"),
                vec![(table, "d")],
            )
        }
        7 => {
            // A CTE, sometimes kept apart as SQLite reads it.
            let table = *rng.pick(&TABLES);
            let materialized = *rng.pick(&["", "MATERIALIZED "]);
            with = format!("WITH d AS {materialized}(SELECT * FROM {table}) ");
            ("d".to_owned(), vec![(table, "d")])
        }
        _ => joins(rng, &mut parameters),
    };
    let mut narrows = false;
    let mut conditions = Vec::new();
    if rng.chance(85) {
        let (table, name) = *rng.pick(&sources);
        let (condition, narrowing) = condition(rng, table, name, &mut parameters, 0);
        narrows = narrowing;
        conditions.push(condition);
    }
    if rng.chance(35) {
        let (condition, narrowing) = subquery_condition(rng, &sources, &mut parameters);
        narrows |= narrowing;
        conditions.push(condition);
    }
    // A join of the form `t, s WHERE …` brings its own WHERE.
    let joined = !from.starts_with('(') && from.contains(" WHERE ");
    let filter = match (joined, conditions.is_empty()) {
        (_, true) => String::new(),
        (true, false) => format!(" AND {}", conditions.join(" AND ")),
        (false, false) => format!(" WHERE {}", conditions.join(" AND ")),
    };
    let (_, last) = sources[sources.len() - 1];
    let (_, first) = sources[0];
    let sql = match rng.below(7) {
        0 => format!("SELECT count(*) FROM {from}{filter}"),
        1 => format!("SELECT sum({last}.c), count({first}.b) FROM {from}{filter}"),
        2 => format!(
            "SELECT {first}.a, count(*) FROM {from}{filter} GROUP BY {first}.a ORDER BY 1"
        ),
        3 => format!(
            "SELECT {first}.id, {last}.b FROM {from}{filter} ORDER BY {first}.c, 1, 2 LIMIT {}",
            1 + rng.below(3)
        ),
        4 => {
            let (sql, _) = subquery(rng, "q", &sources, "max({}.c)", &mut parameters);
            format!("SELECT {first}.id, ({sql}) FROM {from}{filter} ORDER BY 1, 2")
        }
        _ => format!(
            "SELECT {first}.id, {first}.a, {last}.id, {last}.b, {last}.c FROM {from}{filter} ORDER BY 1, 2, 3, 4, 5"
        ),
    };
    Generated { sql: format!("{with}{sql}"), parameters, narrows }
}

/// One generated write.
pub struct Write {
    /// Its statements, one after another.
    pub sql: String,
    /// Whether a statement changes [`CAPTURED`].
    pub captured: bool,
}

/// A write of one to three statements.
pub fn write(rng: &mut Rng) -> Write {
    let mut statements = Vec::new();
    let mut captured = false;
    for _ in 0..1 + rng.below(3) {
        // Each statement its own table, so one write can change a joined
        // table and its partners together.
        let table = *rng.pick(&TABLES);
        captured |= table == CAPTURED;
        let id = key(table, rng.below(10));
        statements.push(match rng.below(8) {
            // Text that a NOCASE comparison folds together.
            7 if table == "u" => format!(
                "UPDATE u SET n = {} WHERE id = {id}",
                rng.pick(&["NULL", "'x'", "'X'", "'k1'", "'K1'", "'k2'"])
            ),
            0 | 1 => format!(
                "INSERT OR IGNORE INTO {table}(id, a, b, c) VALUES ({id}, {}, {}, {})",
                a_value(rng),
                b_value(rng),
                rng.below(10)
            ),
            2 => format!("UPDATE {table} SET a = {} WHERE id = {id}", a_value(rng)),
            3 => format!("UPDATE {table} SET b = {}, c = c + 1 WHERE id = {id}", b_value(rng)),
            4 => format!("UPDATE {table} SET c = {} WHERE a = {}", rng.below(10), rng.below(5)),
            5 if table != CAPTURED => {
                format!("UPDATE OR IGNORE {table} SET id = {} WHERE id = {id}", rng.below(10))
            }
            _ => format!("DELETE FROM {table} WHERE id = {id}"),
        });
    }
    Write { sql: statements.join("; "), captured }
}
