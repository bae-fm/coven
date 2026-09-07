use super::*;

#[tokio::test]
async fn blocked_read_does_not_delay_an_independent_read() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Readable").await;
    let pause = SnapshotPause::new();
    let blocked_pause = pause.clone();
    let reader = handle.clone();
    let blocked = tokio::spawn(async move {
        reader
            .read(move |sql| read_note_around_pause(sql, &blocked_pause))
            .await
    });
    pause.first_read.notified().await;
    let independent = tokio::time::timeout(
        Duration::from_secs(1),
        handle.read(|sql| {
            sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                row.get::<_, String>(0)
            })
            .map_err(CovenError::from)
        }),
    )
    .await;
    pause.release();
    blocked.await.expect("blocked task").expect("blocked read");
    assert_eq!(
        independent
            .expect("an independent reader must run while another read is blocked")
            .expect("independent read"),
        "Readable"
    );
}

#[tokio::test]
async fn processing_releases_every_database_connection() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Readable").await;
    let mut paused = Vec::new();
    let mut running = Vec::new();
    for _ in 0..4 {
        let pause = SnapshotPause::new();
        let processor_pause = pause.clone();
        let reader = handle.clone();
        running.push(tokio::spawn(async move {
            reader
                .read_processed(
                    |sql| {
                        Ok(sql.query_row(
                            "SELECT body FROM notes WHERE id = ?1",
                            [NOTE_ONE],
                            |row| row.get::<_, String>(0),
                        )?)
                    },
                    move |body| {
                        processor_pause.pause_after_first_read();
                        Ok(body)
                    },
                )
                .await
        }));
        pause.first_read.notified().await;
        paused.push(pause);
    }
    let independent = tokio::time::timeout(
        Duration::from_secs(1),
        handle.read(|sql| {
            Ok(sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))?)
        }),
    )
    .await;
    for pause in paused {
        pause.release();
    }
    for task in running {
        assert_eq!(task.await.unwrap().unwrap(), "Readable");
    }
    assert_eq!(
        independent
            .expect("processing must not retain a read connection")
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn a_commit_during_processing_remains_pending() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let processor_pause = pause.clone();
    let calls = AtomicUsize::new(0);
    let mut query = handle.subscribe_processed(
        |sql| {
            Ok(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        },
        move |body| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                processor_pause.pause_after_first_read();
            }
            Ok(body)
        },
    );
    let mut first = Box::pin(query.next());
    tokio::select! { () = pause.first_read.notified() => {}, value = &mut first => panic!("processor returned prematurely: {value:?}") }
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'After' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .unwrap();
    pause.release();
    assert_eq!(first.await.unwrap(), "Before");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), query.next())
            .await
            .unwrap()
            .unwrap(),
        "After"
    );
}

#[tokio::test]
async fn superseded_processing_does_not_publish_an_old_request() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let pause = SnapshotPause::new();
    let processor_pause = pause.clone();
    let calls = AtomicUsize::new(0);
    let mut query = handle.subscribe_reconfigurable_processed(
        NOTE_ONE.to_string(),
        |id, sql| {
            Ok(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        },
        move |_, body| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                processor_pause.pause_after_first_read();
            }
            Ok(body)
        },
    );
    let requests = query.requests();
    let mut next = Box::pin(query.next());
    tokio::select! { () = pause.first_read.notified() => {}, _ = &mut next => panic!("processor returned prematurely") }
    let revision = requests.set(NOTE_TWO.to_string()).unwrap();
    pause.release();
    let event = next.await;
    assert_eq!(event.revision(), revision);
    assert_eq!(event.request(), NOTE_TWO);
    assert_eq!(event.into_result().unwrap(), "Two");
}

#[tokio::test]
async fn cancellation_during_processing_preserves_the_pending_change() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let processor_pause = pause.clone();
    let calls = AtomicUsize::new(0);
    let mut query = handle.subscribe_processed(
        |sql| {
            Ok(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        },
        move |body| {
            if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                processor_pause.pause_after_first_read();
            }
            Ok(body)
        },
    );
    assert_eq!(query.next().await.unwrap(), "Before");
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'After' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .unwrap();
    let mut cancelled = Box::pin(query.next());
    tokio::select! { () = pause.first_read.notified() => {}, value = &mut cancelled => panic!("processor returned prematurely: {value:?}") }
    drop(cancelled);
    pause.release();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), query.next())
            .await
            .unwrap()
            .unwrap(),
        "After"
    );
}

#[tokio::test]
async fn processing_errors_retain_their_read_dependencies() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let processing_calls = calls.clone();
    let mut query = handle.subscribe_processed(
        |sql| {
            Ok(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        },
        move |body| {
            processing_calls.fetch_add(1, Ordering::SeqCst);
            if body == "Before" {
                Err(CovenError::TestFailure("processing refused"))
            } else {
                Ok(body)
            }
        },
    );
    assert!(query.next().await.is_err());
    insert_note(&handle, NOTE_TWO, "Unrelated").await;
    assert_does_not_wake(&mut query).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'After' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), query.next())
            .await
            .unwrap()
            .unwrap(),
        "After"
    );
}

#[tokio::test]
async fn simultaneous_tracked_reads_keep_their_dependencies_independent() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut queries = Vec::new();
    let mut pauses = Vec::new();
    for id in [NOTE_ONE, NOTE_TWO] {
        let pause = SnapshotPause::new();
        let read_pause = pause.clone();
        let calls = AtomicUsize::new(0);
        queries.push(handle.subscribe(move |sql| {
            let body = sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
                row.get::<_, String>(0)
            })?;
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                read_pause.pause_after_first_read();
            }
            Ok(body)
        }));
        pauses.push(pause);
    }
    let mut second = queries.pop().unwrap();
    let mut first = queries.pop().unwrap();
    let mut first_run = Box::pin(first.next());
    let mut second_run = Box::pin(second.next());
    tokio::select! { () = pauses[0].first_read.notified() => {}, value = &mut first_run => panic!("read returned prematurely: {value:?}") }
    tokio::select! { () = pauses[1].first_read.notified() => {}, value = &mut second_run => panic!("read returned prematurely: {value:?}") }
    for pause in pauses {
        pause.release();
    }
    assert_eq!(first_run.await.unwrap(), "One");
    assert_eq!(second_run.await.unwrap(), "Two");
    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(first.next().await.unwrap(), "Changed");
    assert_does_not_wake(&mut second).await;
}

#[tokio::test]
async fn cancelled_queued_read_never_runs_its_sql_closure() {
    let (_temp, handle) = open_handle();
    let mut pauses = Vec::new();
    let mut running = Vec::new();
    for _ in 0..4 {
        let pause = SnapshotPause::new();
        let read_pause = pause.clone();
        let reader = handle.clone();
        running.push(tokio::spawn(async move {
            reader
                .read(move |sql| {
                    let value = sql.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))?;
                    read_pause.pause_after_first_read();
                    Ok(value)
                })
                .await
        }));
        pause.first_read.notified().await;
        pauses.push(pause);
    }
    let ran = Arc::new(AtomicBool::new(false));
    let queued_ran = ran.clone();
    let mut queued = Box::pin(handle.read(move |sql| {
        queued_ran.store(true, Ordering::SeqCst);
        Ok(sql.query_row("SELECT 2", [], |row| row.get::<_, i64>(0))?)
    }));
    assert!(futures_util::poll!(&mut queued).is_pending());
    drop(queued);
    for pause in pauses {
        pause.release();
    }
    for task in running {
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }
    handle.read(|_| Ok(())).await.unwrap();
    assert!(!ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn cancelling_initial_processing_preserves_the_initial_event() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let processor_pause = pause.clone();
    let calls = AtomicUsize::new(0);
    let mut query = handle.subscribe_reconfigurable_processed(
        (),
        |(), sql| {
            Ok(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        },
        move |(), body| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                processor_pause.pause_after_first_read();
            }
            Ok(body)
        },
    );
    let mut cancelled = Box::pin(query.next());
    tokio::select! { () = pause.first_read.notified() => {}, _ = &mut cancelled => panic!("processor returned prematurely") }
    drop(cancelled);
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'After' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .unwrap();
    pause.release();
    let event = tokio::time::timeout(Duration::from_secs(1), query.next())
        .await
        .unwrap();
    assert_eq!(event.cause(), ReconfigurableLiveQueryCause::Initial);
    assert_eq!(event.into_result().unwrap(), "After");
}
