//! A statement's scope plan as the compiler of sqlive-compose builds it, so
//! that sqlive reads no SQL when a live query first runs the statement
//! (SPEC.md section 4): the plan, with what building it found in the schema
//! it was built against, which the open database's schema must match.

use std::collections::BTreeMap;
use std::fmt;

use rusqlite::{Connection, Statement};
use serde::{Deserialize, Serialize};

use crate::read_capture::WholeViews;
use crate::read_report::ReadReport;
use crate::schema_objects::MainSchema;
use crate::scope_plan::{Found, ScopePlan};
use crate::select_syntax::{Definition, Functions};
use crate::unreported_columns::UnreportedColumns;

/// A read the compiler of sqlive-compose built: its SQL, and its scope plan
/// as the compiler encoded it (`CompiledPlan::encode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompiledStatement {
    /// The statement.
    pub sql: &'static str,
    /// Its plan, encoded.
    pub plan: &'static str,
}

/// A statement's scope plan, with what building it found in the schema.
#[derive(Serialize, Deserialize)]
pub struct CompiledPlan {
    plan: ScopePlan,
    schema: PlanSchema,
}

/// What building a plan found in the schema, which the plan holds true of
/// any schema where it finds the same.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlanSchema {
    /// Each name the build looked up among the main schema's tables and
    /// views, lowercase, with the object it found, if any.
    objects: BTreeMap<String, Option<FoundObject>>,
    /// Each function name the build asked about, lowercase, with what the
    /// connection defines under it.
    functions: BTreeMap<String, Vec<Definition>>,
}

/// An object of the main schema that a name found.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FoundObject {
    /// Its name as the schema stores it.
    stored: String,
    /// Its kind, as an error names it (`MainSchema::definition`).
    kind: String,
    /// The SQL that made it, then each of its unique indexes: every other
    /// fact a plan reads of the object follows from those.
    definition: Vec<String>,
    /// Whether the host names it among those a live query reads whole.
    whole: bool,
}

/// Why sqlive can't analyze a statement, which so has no compiled plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unanalyzable {
    /// The shape that stops sqlive, or the parser's message.
    pub why: String,
}

/// What a schema shows otherwise than the schema a compiled plan was built
/// against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaDiffers {
    /// Each object and function that differs, as its kind and name, such as
    /// `table notes`.
    pub differ: Vec<String>,
}

impl fmt::Display for SchemaDiffers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a compiled statement's scope plan was built against a schema that differs from the \
             database's in {}",
            self.differ.join(", ")
        )
    }
}

impl std::error::Error for SchemaDiffers {}

/// A compiled plan's text that this sqlive can't decode.
#[derive(Debug)]
pub struct UndecodablePlan(String);

impl fmt::Display for UndecodablePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sqlive can't decode a compiled scope plan: {}", self.0)
    }
}

impl std::error::Error for UndecodablePlan {}

impl CompiledPlan {
    /// The plan of `sql`, prepared on `connection` as `statement`, with what
    /// building it found in `schema`, the connection's; or why sqlive can't
    /// analyze the statement: its plan would depend on every row of every
    /// table the statement reads, or count every column of the schema it was
    /// built against as read.
    pub(crate) fn build(
        connection: &Connection,
        schema: &MainSchema,
        sql: &str,
        statement: &Statement<'_>,
        whole: &WholeViews,
    ) -> rusqlite::Result<Result<Self, Unanalyzable>> {
        let (plan, found) = ScopePlan::read(connection, schema, sql, statement, whole)?;
        if let ReadReport::Whole { why } = ReadReport::of(&plan) {
            return Ok(Err(Unanalyzable { why }));
        }
        if let UnreportedColumns::Unknown { why, .. } = &*plan.unreported {
            return Ok(Err(Unanalyzable { why: why.clone() }));
        }
        let functions = Functions::read(connection)?;
        let schema = PlanSchema::of(schema, &functions, whole, &found);
        Ok(Ok(Self { plan, schema }))
    }

    /// The plan's text, which `decode` reads back.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("a plan holds only strings, numbers and lists")
    }

    /// The plan `text` encodes.
    ///
    /// # Errors
    ///
    /// Fails when `text` is no plan this sqlive's `encode` writes.
    pub fn decode(text: &str) -> Result<Self, UndecodablePlan> {
        serde_json::from_str(text).map_err(|error| UndecodablePlan(error.to_string()))
    }

    /// What the plan narrows.
    pub fn report(&self) -> ReadReport {
        ReadReport::of(&self.plan)
    }

    /// Whether `schema`, `connection`'s, where a live query reads `whole`
    /// whole, and the functions `connection` defines show each object and
    /// function as building the plan found it; or else what they show
    /// otherwise.
    pub fn check(
        &self,
        connection: &Connection,
        schema: &MainSchema,
        whole: &WholeViews,
    ) -> rusqlite::Result<Result<(), SchemaDiffers>> {
        let functions = Functions::read(connection)?;
        let found = Found {
            objects: self.schema.objects.keys().cloned().collect(),
            functions: self.schema.functions.keys().cloned().collect(),
        };
        let now = PlanSchema::of(schema, &functions, whole, &found);
        let differ = self.schema.differences(&now);
        Ok(if differ.is_empty() { Ok(()) } else { Err(SchemaDiffers { differ }) })
    }

    /// The scope plan, for a schema `check` found as the plan's.
    pub fn into_plan(self) -> ScopePlan {
        self.plan
    }
}

impl PlanSchema {
    /// What `schema` and `functions`, where a live query reads `whole`
    /// whole, show of the names `found` holds.
    fn of(schema: &MainSchema, functions: &Functions, whole: &WholeViews, found: &Found) -> Self {
        let objects = found
            .objects
            .iter()
            .map(|name| {
                let object =
                    schema.definition(name).map(|(stored, kind, definition)| FoundObject {
                        whole: whole.contains(&stored),
                        stored,
                        kind: kind.to_owned(),
                        definition,
                    });
                (name.clone(), object)
            })
            .collect();
        let functions = found
            .functions
            .iter()
            .map(|name| (name.clone(), functions.definitions(name)))
            .collect();
        Self { objects, functions }
    }

    /// Each object and function `now` shows otherwise than this does, as
    /// its kind and name.
    fn differences(&self, now: &Self) -> Vec<String> {
        let mut differ = Vec::new();
        for (name, object) in &self.objects {
            let current = now.objects.get(name).and_then(Option::as_ref);
            if object.as_ref() != current {
                let kind = object.as_ref().or(current).map_or("object", |found| &found.kind);
                differ.push(format!("{kind} {name}"));
            }
        }
        for (name, definitions) in &self.functions {
            if now.functions.get(name) != Some(definitions) {
                differ.push(format!("function {name}"));
            }
        }
        differ
    }
}

/// A float in a plan's text, as its bits: JSON's numbers hold no infinity.
pub(crate) mod float_bits {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.to_bits())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        Ok(f64::from_bits(u64::deserialize(deserializer)?))
    }
}

/// An ordering in a plan's text, as -1, 0 or 1.
pub(crate) mod ordering {
    use serde::{de::Error as _, Deserialize, Deserializer, Serializer};
    use std::cmp::Ordering;

    pub(crate) fn serialize<S: Serializer>(
        ordering: &Ordering,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_i8(*ordering as i8)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Ordering, D::Error> {
        match i8::deserialize(deserializer)? {
            -1 => Ok(Ordering::Less),
            0 => Ok(Ordering::Equal),
            1 => Ok(Ordering::Greater),
            other => Err(D::Error::custom(format!("{other} is no ordering"))),
        }
    }
}

#[cfg(test)]
#[path = "compiled_plan_tests.rs"]
mod tests;
