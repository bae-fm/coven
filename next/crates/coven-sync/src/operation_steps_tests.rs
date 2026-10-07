use super::*;
use coven_database::{Migration, RowIdentity, SyncedTable};

async fn rows(d: &mut Device) {
    d.db.close().await.unwrap();
    d.db = DatabaseBuilder::new(d.directory.clone()).synced_tables(vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")])
        .migrations(vec![Migration::sql(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, audience TEXT NOT NULL)")])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending).clock(d.clock.clone()).open().await.unwrap();
    d.sync.database = d.db.clone();
}
async fn write(d: &Device, circle: CircleId, n: u128) {
    d.db.write(move |sql| {
        sql.execute(
            "INSERT INTO notes VALUES(?1,?2,?3)",
            (
                Uuid::from_u128(0x00000000000070008000000000000000 | n).to_string(),
                n.to_string(),
                circle.to_string(),
            ),
        )?;
        Ok(())
    })
    .await
    .unwrap();
}
async fn count(d: &Device) -> i64 {
    d.db.read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap()
}
async fn transfer(a: &Device, b: &Device) {
    for record in a.db.test_queued_writes().await.unwrap() {
        let position = record.header.position;
        assert!(!matches!(
            b.db.apply_downloaded(record.into()).await.unwrap(),
            coven_database::ApplyOutcome::Waiting(_)
        ));
        a.db.test_acknowledge_write(position).await.unwrap();
    }
}

#[tokio::test]
async fn deletion_write_and_operation_commit_together_and_wait_for_upload() {
    let storage = google();
    let [mut a, mut b, _c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    let id = begin(
        &mut a,
        Command::AddCircleMember(circle, b.member.member_id()),
    )
    .await;
    finish(&mut a, id).await;
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    assert_eq!(count(&b).await, 1);
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    assert_eq!(count(&a).await, 0);
    assert!(!a.log().await.replay.state.circles[&circle].deleted);
    assert_eq!(a.db.operations().await.unwrap().len(), 1);
    assert_eq!(a.db.operations().await.unwrap()[0].last_step, 1);
    assert_eq!(a.db.test_queued_writes().await.unwrap().len(), 1);
    assert!(matches!(
        step(&mut a, deletion).await.unwrap(),
        Progress::Waiting
    ));
    transfer(&a, &b).await;
    assert_eq!(count(&b).await, 0);
    finish(&mut a, deletion).await;
    b.sync().await;
    assert!(b.log().await.replay.state.circles[&circle].deleted);
}

#[tokio::test]
async fn dropped_circle_deletion_restores_hidden_rows_but_keeps_explicit_deletes() {
    let storage = google();
    let [mut a, mut b, _c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    let id = begin(
        &mut a,
        Command::AddCircleMember(circle, b.member.member_id()),
    )
    .await;
    finish(&mut a, id).await;
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    // Ben knows another row that Ana's explicit deletion cannot name.
    write(&b, circle, 2).await;
    // Ben's earlier removal is fixed before Ana's deletion write arrives.
    let removal = begin(
        &mut b,
        Command::RemoveCircleMember(circle, a.member.member_id()),
    )
    .await;
    step(&mut b, removal).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    transfer(&a, &b).await;
    assert!(matches!(
        step(&mut a, deletion).await.unwrap(),
        Progress::Advanced
    ));
    let delete_entry = a.db.local_store_log().await.unwrap().upload.unwrap().entry;
    finish(&mut a, deletion).await;
    transfer(&b, &a).await;
    assert_eq!(count(&a).await, 0); // replay's deleted-circle rule hides Ben's row
    finish(&mut b, removal).await;
    a.sync().await;
    assert!(matches!(
        a.log().await.replay.entries[&delete_entry.position],
        coven_database::EntryOutcome::Dropped(_)
    ));
    assert_eq!(count(&a).await, 1); // the explicit delete of row 1 remains a delete
    assert_eq!(count(&b).await, 1);
}

#[tokio::test]
async fn restarted_circle_deletion_deletes_rows_that_arrived_during_its_first_attempt() {
    let storage = google();
    let [mut a, mut b, c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    for member in [&b.member, &c.member] {
        let id = begin(&mut a, Command::AddCircleMember(circle, member.member_id())).await;
        finish(&mut a, id).await;
    }
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    write(&b, circle, 2).await;
    let removal = begin(
        &mut b,
        Command::RemoveCircleMember(circle, c.member.member_id()),
    )
    .await;
    step(&mut b, removal).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    transfer(&a, &b).await;
    step(&mut a, deletion).await.unwrap(); // fix deletion against the original members
    finish(&mut b, removal).await;
    transfer(&b, &a).await;
    assert_eq!(count(&a).await, 1);
    for _ in 0..12 {
        match step(&mut a, deletion).await.unwrap() {
            Progress::Waiting => break,
            Progress::Advanced => (),
            _ => panic!("restarted deletion must wait for its new row-delete write"),
        }
    }
    assert_eq!(count(&a).await, 0);
    let mut writes = a.db.test_queued_writes().await.unwrap();
    assert_eq!(writes.len(), 1);
    assert!(matches!(
        b.db.apply_downloaded(writes.remove(0).into())
            .await
            .unwrap(),
        coven_database::ApplyOutcome::Waiting(coven_database::WriteWait::StoreLog(_))
    ));
    // The replacement write read the dropped deletion entry as well as the removal.
    b.sync().await;
    transfer(&a, &b).await;
    finish(&mut a, deletion).await;
    b.sync().await;
    assert_eq!(count(&b).await, 0);
}

#[tokio::test]
async fn resumed_removal_uses_access_from_the_kept_remote_removal() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Admin).await;
    b.sync().await;
    c.sync().await;
    let waiting = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    b.sync
        .make_and_upload_entry(StoreChange::SetAccess {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "replacement-key".into(),
            },
        })
        .await
        .unwrap();
    c.sync().await;
    let removal = begin(&mut c, Command::RemoveMember(b.member.member_id())).await;
    finish(&mut c, removal).await;
    assert!(matches!(finish(&mut a, waiting).await,
        Output::Removal(MemberRemoval::DeleteAccessKey { access_key_id }) if access_key_id == "replacement-key"));
    assert_eq!(
        a.db.access_keys_to_delete().await.unwrap(),
        [AccessKeyToDelete {
            access_key_id: "replacement-key".into(),
            member: Some(b.member.member_id()),
        }]
    );
}
