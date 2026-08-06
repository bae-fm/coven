use crate::database::DatabaseTestSql;

impl DatabaseTestSql<'_> {
    pub(crate) fn install_test_active_circle(
        &self,
        label: &str,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        self.install_test_circle_current_state(label, true)
    }

    pub(crate) fn install_test_inactive_circle(
        &self,
        label: &str,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
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
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        use crate::protocol::circle_activation_test_fixtures::test_circle_activation;
        use crate::protocol::store_commit::ObjectHash;

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
