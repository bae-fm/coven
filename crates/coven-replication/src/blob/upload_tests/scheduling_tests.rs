use super::*;

#[tokio::test]
async fn later_root_completion_keeps_filling_slots_until_the_head_completes() {
    let home = Arc::new(InstrumentedHome::new());
    let fixture = UploadFixture::with_home(2, home.clone(), FixtureSchema::RowBlobs).await;
    let long_file = vec![7; 128];
    fixture
        .plant_uploads_for("first-root", &[("first001", &long_file)], false)
        .await;
    fixture
        .plant_uploads_for("second-root", &[("second01", b"x")], false)
        .await;
    fixture
        .plant_uploads_for("third-root", &[("third001", b"y")], false)
        .await;
    home.slow_creates(1, std::time::Duration::from_millis(10));

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();

    assert_eq!(outcome.uploaded(), 3);
    assert!(outcome.yielded_for_publish());
    assert_eq!(home.max_inflight(), 2);
    assert!(is_created(&fixture.journal("first001").await));
    assert!(is_created(&fixture.journal("second01").await));
    assert!(is_created(&fixture.journal("third001").await));
}
