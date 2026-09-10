use super::*;

#[test]
fn commit_parsers_require_an_author_from_the_expected_store() {
    let expected = fixture();
    let foreign = fixture();
    assert_ne!(
        expected.root_ref.store_root_hash,
        foreign.root_ref.store_root_hash
    );
    for author in [&expected, &foreign] {
        let mut commit = expected.commit.clone();
        commit.body_mut().author_registration = author.registration_ref.clone();
        let coord = StoreCommitCoord {
            stream_id: StreamActivation::device_authorized_stream_id(
                expected.root_ref.store_root_hash,
                &author.registration_ref,
                StreamAnchorDomain::StoreAnnouncements,
            ),
            sequence: commit.seq(),
        };
        let family = commit.candidate_family();
        let StoreCommitBody::Operations(operations) = &mut commit.body_mut().body else {
            panic!("fixture commit carries operations");
        };
        let package = operations
            .store_package
            .as_mut()
            .expect("fixture Store package");
        package.candidate_family = family;
        package.object = exact(
            format!(
                "{}.pkg",
                package_semantic_prefix(
                    family,
                    &coord.stream_id.to_string(),
                    coord.sequence(),
                    package.content_hash,
                )
            ),
            &expected.package,
        );
        let manifest =
            candidate_manifest(family, &commit.body).expect("build exact candidate graph");
        commit.body_mut().candidate_objects = manifest;
        author.resign(&mut commit);
        let bytes = commit.to_bytes();
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            coord.clone(),
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        family,
                        &coord.stream_id.to_string(),
                        coord.sequence(),
                        commit.commit_hash(),
                    )
                ),
                &bytes,
            ),
        )
        .expect("bind the signed commit to its exact bytes");

        for (entry, result) in [
            (
                "stored",
                VerifiedStoreBatchCommit::parse(
                    &bytes,
                    expected.root_ref.store_root_hash,
                    &reference,
                    &author.registration,
                ),
            ),
            (
                "prepared",
                VerifiedStoreBatchCommit::parse_prepared(
                    &bytes,
                    expected.root_ref.store_root_hash,
                    coord.clone(),
                    reference.object.clone(),
                    &author.registration,
                ),
            ),
        ] {
            if author.root_ref == expected.root_ref {
                let parsed = result.expect("accept the author's own Store");
                assert_eq!(parsed.reference(), &reference);
                assert_eq!(parsed.author(), &author.registration);
            } else {
                assert!(matches!(
                    result,
                    Err(StoreProtocolError::StoreRootMismatch { expected: root, actual })
                        if root == expected.root_ref.store_root_hash
                            && actual == author.root_ref.store_root_hash
                ), "{entry} parser accepted a foreign author or failed for another reason: {result:?}");
            }
        }
    }
}
