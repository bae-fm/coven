//! A pin's download on a metered network: the network holds it, as the
//! stored setting says, and a read goes on.

use super::*;

/// Drive `pin` until its download waits for the network to stop holding
/// it, which only the network or the setting changing, or the store
/// closing, ends.
async fn until_held<P>(f: &Fixture, pin: &mut std::pin::Pin<&mut P>)
where
    P: std::future::Future,
    P::Output: std::fmt::Debug,
{
    let mut held = f.files.inner.held_downloads.subscribe();
    tokio::time::timeout(HANG_GUARD, async {
        tokio::select! {
            result = pin.as_mut() => panic!("the pin was not held: {result:?}"),
            held = held.wait_for(|held| *held > 0) => {
                held.expect("the store holds the count");
            }
        }
    })
    .await
    .expect("the pin waits for the network");
}

/// How many requests the provider has seen.
fn requests(f: &Fixture) -> usize {
    f.storage.request_trace().len()
}

/// Ben's phone is on cellular, and his app moves files only on unmetered
/// networks. Pinning a song asks storage for nothing, lists no download and
/// records nothing; a connection change goes on meanwhile, and playing the
/// song still streams it. Once the phone joins Wi-Fi the pin downloads it
/// (§17.8, §17.9).
#[tokio::test]
async fn a_metered_network_holds_a_pins_download_but_not_a_read() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("song", vec![5; CHUNK * 3]).await;
    f.files.stop_background_for_counts().await;
    f.set_unmetered_only(true).await;
    f.network.set_metered(true);
    let before = requests(&f);
    let pin = f.files.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    until_held(&f, &mut pin).await;
    assert_eq!(requests(&f), before, "a held pin asks storage for nothing");
    assert!(downloading(&f).await.is_empty());
    assert!(file_records(&f).await.is_empty());
    // The waiting pin holds no lock a connection change needs.
    tokio::time::timeout(
        HANG_GUARD,
        f.files.set_storage(Some(f.storage.clone()), std::future::ready(Ok(()))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(f.files.read_file(&file, &lease().await).await.unwrap(), vec![5; CHUNK * 3]);
    f.network.join(false);
    tokio::time::timeout(HANG_GUARD, pin).await.unwrap().unwrap();
    assert_eq!(f.database.files_pinned(vec![file.id()]).await.unwrap(), vec![Some(true)]);
    f.close().await;
}

/// A pin's download under way stops before its next request once the
/// network turns metered, and starts the file again once the setting goes
/// off; closing the store ends a pin still waiting (§17.8).
#[tokio::test]
async fn a_pins_download_stops_before_its_next_request_on_a_metered_network() {
    let f = Fixture::new(Provenance::AppProvided).await;
    // Twenty chunks take two requests of up to 1 MiB.
    let bytes = (0..CHUNK * 20).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let file = f.uploaded("album", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    f.set_unmetered_only(true).await;
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_body_at(f.path(&file), 1, observed, resumed).await;
    let pin = f.files.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    f.network.set_metered(true);
    resume.send(()).unwrap();
    // The first request finishes, and the download stops: the next never
    // starts.
    let first = requests(&f);
    let mut listed: coven_database::LiveQuery<_, DbError> = f.database.subscribe(
        coven_database::test_utils::LiveSql::of("SELECT count(*) FROM coven_downloading", |r| {
            r.get::<_, i64>(0)
        })
        .one(),
    );
    tokio::select! {
        result = &mut pin => panic!("the pin went on over a metered network: {result:?}"),
        () = async {
            while listed.next_inline().await.expect("the live query answers").unwrap() != 0 {}
        } => {}
    }
    until_held(&f, &mut pin).await;
    assert_eq!(requests(&f), first);
    f.set_unmetered_only(false).await;
    tokio::time::timeout(HANG_GUARD, pin).await.unwrap().unwrap();
    assert_eq!(f.files.read_file(&file, &lease().await).await.unwrap(), bytes);
    // A pin waiting as the store closes ends with it.
    let other = f.uploaded("other", vec![3; CHUNK]).await;
    f.set_unmetered_only(true).await;
    let waiting = f.files.pin(std::slice::from_ref(&other));
    tokio::pin!(waiting);
    until_held(&f, &mut waiting).await;
    f.files.close().await;
    assert!(matches!(
        tokio::time::timeout(HANG_GUARD, waiting).await.unwrap(),
        Err(FileReadError::Database(DbError::StoreClosed))
    ));
    f.database.close().await.unwrap();
}

/// The setting persists: a pin prepared once the store reopens on a metered
/// network waits (§17.8).
#[tokio::test]
async fn a_pin_prepared_after_reopening_holds_as_the_stored_setting_says() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("song", vec![6; CHUNK]).await;
    f.set_unmetered_only(true).await;
    f.network.set_metered(true);
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    let file = f.database.file_ref(file.id()).await.unwrap();
    let before = requests(&f);
    let pin = f.files.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    until_held(&f, &mut pin).await;
    assert_eq!(requests(&f), before);
    f.network.set_metered(false);
    tokio::time::timeout(HANG_GUARD, pin).await.unwrap().unwrap();
    f.close().await;
}
