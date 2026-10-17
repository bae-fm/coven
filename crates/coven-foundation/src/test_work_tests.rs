use super::*;

#[tokio::test]
async fn concurrent_measurements_follow_bound_work_and_restore_the_callers_scope() {
    let first = Counter::default();
    let second = Counter::default();
    let run = |counter: Counter, count: usize| async move {
        counter
            .run(async move {
                for _ in 0..count {
                    decode();
                    tokio::task::yield_now().await;
                }
                tokio::task::spawn_blocking(bind(|| {
                    writing(|| {
                        snapshot_record_decode();
                        custody_derivation();
                    })
                }))
                .await
                .unwrap();
            })
            .await;
    };
    tokio::join!(run(first.clone(), 2), run(second.clone(), 3));
    // Outside a measurement, unrelated work contributes to neither counter.
    decode();
    custody_derivation();
    for (counter, decodes) in [(first, 2), (second, 3)] {
        assert_eq!(
            counter.take(),
            Counts {
                decodes,
                custody_derivations: 1,
                snapshot_decodes: 1,
                snapshot_decodes_with_writer: 1
            }
        );
        assert_eq!(counter.take(), Counts::default());
    }
}
