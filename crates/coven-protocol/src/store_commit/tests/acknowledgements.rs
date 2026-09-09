use super::*;

impl Fixture {
    fn signed_ack(&self, last_sync: &str) -> StoreAck {
        StoreAck::signed(
            self.root_ref.store_root_hash,
            1,
            StoreAckAssertion {
                registration: self.registration_ref.clone(),
                store_cut: StoreHistoryCut(BTreeMap::new()),
                device_state: self.commit.device_state.clone(),
            },
            last_sync.to_string(),
            SuccessorLink {
                activation: self
                    .registration
                    .store_acknowledgement_activation(&self.registration_ref)
                    .expect("derive acknowledgement activation")
                    .activation_id(),
                predecessor: None,
                next_slot: slot("store-v1/acks/founder/2.json".to_string()),
            },
            &self
                .registration
                .device_signer(&self.signer)
                .expect("derive device signer"),
        )
        .expect("sign Store acknowledgement")
    }
}

#[test]
fn store_ack_semantic_hash_is_distinct_from_its_stored_json_hash() {
    let ack = fixture().signed_ack("2026-07-16T00:00:00Z");
    let bytes = ack.to_bytes();
    let semantic_hash = StoreAck::semantic_hash_from_bytes(&bytes).unwrap();

    assert_eq!(semantic_hash, ack.ack_hash());
    assert_ne!(semantic_hash, ObjectHash::digest(&bytes));
}

#[test]
fn store_ack_wire_shape_binds_activation_state_without_a_parallel_predecessor_ref() {
    let ack = fixture().signed_ack("2026-07-18T00:00:00Z");
    let envelope = serde_json::to_value(ack).unwrap();
    let value = envelope
        .get("body")
        .expect("a signed acknowledgement carries its body");

    assert!(value.get("registration").is_some());
    assert!(value.get("sequence").is_some());
    assert!(value.get("device_state").is_some());
    assert!(value.get("exclusions").is_none());
    assert!(value.get("author_registration").is_none());
    assert!(value.get("revision").is_none());
    assert!(value.get("predecessor").is_none());
}

/// A standing acknowledgement forgives exactly one advance in the Store's
/// history: the commit that published it. Everything else is news.
mod standing_acknowledgement {
    use super::*;

    fn commit(stream: &str, sequence: u64) -> StoreBatchCommitRef {
        let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(stream.as_bytes()));
        let commit_hash = ObjectHash::digest(format!("{stream}/{sequence}").as_bytes());
        StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id,
                sequence,
            },
            commit_hash,
            object: exact(
                format!("store-v1/commits/{stream}/{sequence}.json"),
                format!("{stream}/{sequence}").as_bytes(),
            ),
        }
    }

    fn cut(commits: &[StoreBatchCommitRef]) -> StoreHistoryCut {
        StoreHistoryCut(
            commits
                .iter()
                .map(|commit| (commit.coord.stream_id, commit.clone()))
                .collect(),
        )
    }

    /// A device that acknowledged `mine@1` and `theirs@1`, whose acknowledgement
    /// landed at `mine@2`.
    fn standing(fixture: &Fixture) -> StandingStoreAck {
        StandingStoreAck {
            assertion: StoreAckAssertion {
                registration: fixture.registration_ref.clone(),
                store_cut: cut(&[commit("mine", 1), commit("theirs", 1)]),
                device_state: fixture.commit.device_state.clone(),
            },
            activating_commit: Some(commit("mine", 2)),
        }
    }

    fn assertion_over(
        standing: &StandingStoreAck,
        store_cut: StoreHistoryCut,
    ) -> StoreAckAssertion {
        StoreAckAssertion {
            store_cut,
            ..standing.assertion.clone()
        }
    }

    #[test]
    fn holds_when_the_only_new_commit_is_the_one_that_published_it() {
        let fixture = fixture();
        let standing = standing(&fixture);
        assert!(standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 2), commit("theirs", 1)]),
        )));
    }

    #[test]
    fn does_not_hold_once_this_device_commits_anything_further() {
        let fixture = fixture();
        let standing = standing(&fixture);
        assert!(!standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 3), commit("theirs", 1)]),
        )));
    }

    #[test]
    fn does_not_hold_once_another_device_commits() {
        let fixture = fixture();
        let standing = standing(&fixture);
        assert!(!standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 2), commit("theirs", 2)]),
        )));
    }

    #[test]
    fn does_not_hold_once_a_new_device_appears_in_the_history() {
        let fixture = fixture();
        let standing = standing(&fixture);
        assert!(!standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 2), commit("theirs", 1), commit("third", 1)]),
        )));
    }

    /// An acknowledgement that activated no commit — it lost the race to another
    /// device's — forgives nothing at all.
    #[test]
    fn a_losing_acknowledgement_forgives_no_commit() {
        let fixture = fixture();
        let standing = StandingStoreAck {
            activating_commit: None,
            ..standing(&fixture)
        };
        assert!(standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 1), commit("theirs", 1)]),
        )));
        assert!(!standing.still_holds(&assertion_over(
            &standing,
            cut(&[commit("mine", 2), commit("theirs", 1)]),
        )));
    }
}
