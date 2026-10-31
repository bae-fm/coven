use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Boundary, BoundaryCoverage, DbError, StoreVersion};
use coven_format::admin::{SnapshotId, StoreChange};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Slot};

#[test]
fn an_established_version_does_not_select_a_later_preparation() {
    let first = SnapshotId { audience: Audience::Store, device: DeviceId(1), number: 1 };
    let mut state = crate::StoreState::default();
    Boundary::SchemaChange(StoreVersion { number: 2, snapshot: first, entry: Slot(4) })
        .apply(&mut state);
    let later = SnapshotId { audience: Audience::Store, device: DeviceId(2), number: 1 };
    assert!(Boundary::schema_in_place(&state, 2, &later));
    assert!(!Boundary::schema_in_place(&state, 3, &later));
}

/// Breaking version 2 explicitly drops late old writes: an index preserves the notes table,
/// while a table drop exercises frozen losses without the old app schema.
pub(crate) async fn version_two(store: &TestStore, notes_table: bool) -> crate::Database {
    let (tables, change) = if notes_table {
        (notes(), "CREATE INDEX notes_title ON notes(title)")
    } else {
        (Vec::new(), "DROP TABLE notes")
    };
    store
        .builder(
            tables,
            vec![
                crate::Migration::addition(1, "notes", NOTES),
                crate::Migration::breaking(2, "change", change, crate::OldWrites::Drop),
            ],
        )
        .open()
        .await
        .unwrap()
}

impl crate::Database {
    pub(crate) async fn check_boundary_fixture(&self, boundary: Boundary) -> Result<Slot, DbError> {
        let entry = boundary.entry();
        let change = match &boundary {
            Boundary::SchemaChange(v) => {
                StoreChange::RaiseSchema { version: v.number, announcement: v.entry }
            }
            Boundary::Reset { entry, .. } => StoreChange::Reset { announcement: *entry },
        };
        let mut state = self.store_state().await.unwrap();
        boundary.apply(&mut state);
        self.admin_fixture(change, state, Some(boundary)).await?;
        Ok(entry)
    }
    pub(crate) async fn schema_fixture(
        &self,
        audience: Audience,
        version: u32,
    ) -> Result<Boundary, DbError> {
        if let Some(v) =
            self.store_state().await.unwrap().schema.get(&audience).filter(|v| v.number == version)
        {
            return Ok(Boundary::SchemaChange(v.clone()));
        }
        let slot = Slot(self.applied_slot().await? + 1);
        Ok(Boundary::SchemaChange(StoreVersion {
            number: version,
            snapshot: SnapshotId { audience, device: DeviceId(98), number: slot.0 },
            entry: slot,
        }))
    }
    pub(crate) async fn install_schema_fixture(
        &self,
        audience: Audience,
        version: u32,
        included: u64,
    ) -> Result<bool, DbError> {
        let boundary = self.schema_fixture(audience.clone(), version).await?;
        let entry = self.check_boundary_fixture(boundary).await?;
        self.coverage_fixture(entry, audience, included)
    }
    fn coverage_fixture(
        &self,
        entry: Slot,
        audience: Audience,
        included: u64,
    ) -> Result<bool, DbError> {
        self.inspect_writer(|db| {
            db.transaction(|db| {
                let boundary =
                    Boundary::load_selected(db)?.into_iter().find(|b| b.entry() == entry).unwrap();
                let changed = BoundaryCoverage { boundary, included }.record_inside(db)?;
                crate::reload_gate::clear_reload(db, &audience)?;
                Ok(changed)
            })
        })
    }
    /// Cover the consumed prefix with a raise. Later older-schema writes
    /// are excluded only if they cross a breaking step with an explicit drop choice.
    pub(crate) async fn exclude_later_writes(&self, version: u32) {
        self.install_schema_fixture(Audience::Store, version, self.applied_slot().await.unwrap())
            .await
            .unwrap();
    }
    pub(crate) async fn install_reset_fixture(
        &self,
        entry: Slot,
        audience: Audience,
        included: u64,
    ) -> Result<bool, DbError> {
        self.check_boundary_fixture(Boundary::Reset {
            entry,
            snapshot: SnapshotId {
                audience: audience.clone(),
                device: DeviceId(99),
                number: entry.0,
            },
        })
        .await?;
        self.coverage_fixture(entry, audience, included)
    }
}

#[tokio::test]
async fn coverage_survives_restart_and_rejects_replacement() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert!(db.install_reset_fixture(Slot(1), Audience::Store, 0).await.unwrap());
    assert!(!db.coverage_fixture(Slot(1), Audience::Store, 0).unwrap());
    assert!(db.coverage_fixture(Slot(1), Audience::Store, 1).is_err());
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let boundaries = db.inspect_writer(super::WriteBoundary::load).unwrap();
    assert_eq!(boundaries.len(), 1);
    assert_eq!(boundaries[0].rule, super::BoundaryRule::Reset(Slot(1)));
    assert_eq!(boundaries[0].included, 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn schema_boundaries_are_independent_for_each_audience() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')").await.unwrap();
    let record = records(&db).remove(0);
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(1)));
    let boundary = super::WriteBoundary {
        audience: circle.clone(),
        rule: super::BoundaryRule::Raise(2),
        included: 0,
    };
    assert!(boundary.excludes(&record.header, &Audience::Store, None).is_none());
    assert_eq!(
        boundary.excludes(&record.header, &circle, None),
        Some(coven_format::snapshot_rows::ChangeVerdict::Excluded(2))
    );
    assert!(db.install_schema_fixture(circle, 2, 0).await.unwrap());
    assert!(db.install_schema_fixture(Audience::Store, 2, 0).await.unwrap());
    assert_eq!(count(&db, "_coven_boundaries"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_uses_the_authored_prefix_and_precedes_schema_exclusion() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')").await.unwrap();
    let mut header = records(&db).remove(0).header;
    let reset = super::WriteBoundary {
        audience: Audience::Store,
        rule: super::BoundaryRule::Reset(Slot(5)),
        included: 3,
    };
    let raise = super::WriteBoundary {
        audience: Audience::Store,
        rule: super::BoundaryRule::Raise(2),
        included: 3,
    };
    let boundaries = [raise, reset];
    assert_eq!(
        super::WriteBoundary::part_verdict(&boundaries, &header, &Audience::Store, Some(Slot(3))),
        None
    );
    assert_eq!(
        super::WriteBoundary::part_verdict(&boundaries, &header, &Audience::Store, Some(Slot(6))),
        Some(coven_format::snapshot_rows::ChangeVerdict::Ignored(Slot(5)))
    );
    header.read_through = 5;
    assert_eq!(
        super::WriteBoundary::part_verdict(&boundaries, &header, &Audience::Store, Some(Slot(6))),
        Some(coven_format::snapshot_rows::ChangeVerdict::Excluded(2))
    );
    db.close().await.unwrap();
}
