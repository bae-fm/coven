use crate::DatabaseTestSql;

impl DatabaseTestSql<'_> {
    pub(crate) fn install_test_active_circle(
        &self,
        label: &str,
    ) -> (
        coven_protocol::circle::CircleId,
        coven_protocol::circle::CircleControlCoord,
    ) {
        self.install_test_circle_current_state(label, true)
    }

    pub(crate) fn install_test_inactive_circle(
        &self,
        label: &str,
    ) -> (
        coven_protocol::circle::CircleId,
        coven_protocol::circle::CircleControlCoord,
    ) {
        self.install_test_circle_current_state(label, false)
    }

    /// Plant the founder Circle activation's rows, so a database read derives
    /// the same current state the protocol fixture already holds.
    fn install_test_circle_current_state(
        &self,
        label: &str,
        active: bool,
    ) -> (
        coven_protocol::circle::CircleId,
        coven_protocol::circle::CircleControlCoord,
    ) {
        use coven_protocol::circle_activation_test_fixtures::test_circle_activation;
        use coven_protocol::store_commit::ObjectHash;

        let activation = test_circle_activation(label, active);
        let control_coord = serde_json::to_string(&activation.control.coord)
            .expect("serialize test Circle control coordinate");
        self.install_circle_current_state(
            activation.circle_id,
            &control_coord,
            &format!("{label}-device"),
            ObjectHash::digest(format!("{label} commit").as_bytes()),
            &activation.control.bytes,
            active.then_some(activation.owner_pubkey.as_str()),
            &serde_json::to_vec(&activation.current).expect("serialize test Circle current state"),
        )
        .expect("install test Circle state");
        (activation.circle_id, activation.control.coord)
    }
}

impl crate::Database {
    pub async fn bind_circle_row_blob_for_test(
        &self,
        binding: coven_protocol::audience_package::RowBlobLocatorBinding,
        package: coven_protocol::store_commit::CirclePackageRef,
        owner: coven_protocol::store_commit::StoreBatchCommitRef,
    ) {
        let record = coven_protocol::remote_object::RemoteObjectRecord::activated_blob(
            binding.blob(),
            owner,
        )
        .expect("construct activated Circle blob")
        .into_record();
        let audience = coven_protocol::audience_package::PackageAudience::Circle {
            circle_id: package.circle_id,
            control: package.control,
            key_fingerprint: package.key_fingerprint,
        };
        assert_eq!(
            binding.blob().locator().audience(),
            audience.remote_audience()
        );
        self.test_sql(move |database| {
            database.install_blob_binding(
                &record.object_id().to_string(),
                &serde_json::to_string(&record)?,
                &binding.blob().locator().locator_hash().to_string(),
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp(),
                &serde_json::to_string(&audience)?,
            )
        })
        .await
        .expect("bind Circle row blob");
    }
}
