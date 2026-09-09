use crate::*;
use coven_protocol::store_commit::{
    StoreAck, StoreAckRef, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
};

use super::device_registration_journal::LocalRegistrationRecord;
use super::*;

impl StoreDatabase {
    /// Install an already activated recovery together with its current exact
    /// acknowledgement. Preparation performs every remote read before this call.
    pub async fn install_adopted_owner_recovery(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
        activation: StoreDeviceRegistrationActivation,
        latest_ack: (StoreAckRef, StoreAck),
    ) -> Result<(), DbError> {
        const SUBJECT: &str = "adopted Owner recovery registration graph";
        let record = LocalRegistrationRecord::checked_owner_recovery(
            registration,
            initial_ack_ref,
            initial_ack,
            &activation,
            SUBJECT,
        )?;
        self.call_store(move |session| {
            session.install_adopted_owner_recovery(record, activation, latest_ack, SUBJECT)
        })
        .await
    }
}

impl StoreSession<'_> {
    fn install_adopted_owner_recovery(
        &mut self,
        record: LocalRegistrationRecord,
        activation: StoreDeviceRegistrationActivation,
        (latest_ack_ref, latest_ack): (StoreAckRef, StoreAck),
        subject: &str,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction()?;
        let root = self.required_root_authority()?;
        record.require_installed_store_root(&root, subject)?;
        let activated = self.activated_store_device_registration_with_authority(
            root.clone(),
            record.reference().clone(),
        )?;
        if activated.value() != record.registration() || activated.activation() != &activation {
            return Err(DbError::Message(
                "adopted recovery differs from its installed registration authority".into(),
            ));
        }
        let accepted = self.activated_store_ack(record.reference())?;
        let expected = match &accepted {
            Some(accepted) => &accepted.reference,
            None => record.initial_ack_ref(),
        };
        if &latest_ack_ref != expected {
            return Err(DbError::Message(
                "adopted recovery acknowledgement changed while its exact object was prepared"
                    .into(),
            ));
        }
        let verified = StoreAck::parse_at(
            &latest_ack.to_bytes(),
            &root,
            &latest_ack_ref,
            activated.value(),
        )
        .map_err(DbError::from)?;
        if verified != latest_ack {
            return Err(DbError::Message(
                "resumed acknowledgement head changed during exact verification".into(),
            ));
        }
        let recorded = load_published_store_ack_on(&tx)?;
        if let Some(recorded) = &recorded {
            if recorded.reference.registration == *record.reference()
                && (recorded.reference.sequence > latest_ack_ref.sequence
                    || (recorded.reference.sequence == latest_ack_ref.sequence
                        && (recorded.reference != latest_ack_ref
                            || recorded.successor_slot != latest_ack.successor.next_slot)))
            {
                return Err(DbError::Message(
                    "adopted recovery acknowledgement conflicts with its existing local stream"
                        .into(),
                ));
            }
        }
        let standing = coven_protocol::store_commit::StandingStoreAck {
            assertion: latest_ack.assertion(),
            activating_commit: accepted.map(|accepted| accepted.activating_commit),
        };
        let ack_ref = serde_json::to_string(&latest_ack_ref)
            .map_err(|error| DbError::context("adopted acknowledgement head", error))?;
        let successor = serde_json::to_string(&latest_ack.successor.next_slot)
            .map_err(|error| DbError::context("adopted acknowledgement successor", error))?;
        let standing = serde_json::to_string(&standing)
            .map_err(|error| DbError::context("adopted standing acknowledgement", error))?;
        record.replace_journal_on(
            &tx,
            LocalDeviceRegistrationState::Activated {
                authority: activation,
            },
            subject,
        )?;
        tx.execute(
            "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot, standing) \
             VALUES (1, ?1, ?2, ?3) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot, \
                 standing = excluded.standing",
            (&ack_ref, &successor, &standing),
        )?;
        crate::set_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY, &record.device_id())?;
        tx.commit().map_err(DbError::from)
    }
}
