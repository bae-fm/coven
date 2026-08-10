use crate::*;
use coven_protocol::provider::{
    DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
};

use super::*;

impl StoreSession<'_> {
    fn prepare_device_join_challenge_publication(
        &self,
        key: &str,
        value: &str,
        prepared: &coven_protocol::provider::DeviceJoinChallengePublicationRecord,
    ) -> Result<coven_protocol::provider::DeviceJoinChallengePublicationRecord, DbError> {
        let actual = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .begin_protocol_state(key, value)?;
        let actual: coven_protocol::provider::DeviceJoinChallengePublicationRecord =
            serde_json::from_str(&actual).map_err(|error| {
                DbError::context("parse device join challenge publication", error)
            })?;
        if actual.challenge != prepared.challenge {
            return Err(DbError::Message(
                "device join challenge probe id was reused with different bytes".to_string(),
            ));
        }
        Ok(actual)
    }

    fn publish_device_join_challenge(
        &self,
        key: &str,
        authorization: coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: coven_protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), DbError> {
        use coven_protocol::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

        let previous_json =
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
                .required_protocol_state(key)?;
        let previous: DeviceJoinChallengePublicationRecord = serde_json::from_str(&previous_json)
            .map_err(|error| {
            DbError::context("parse device join challenge publication", error)
        })?;
        if previous.challenge != challenge {
            return Err(DbError::Message(
                "device join challenge publication differs from prepared bytes".to_string(),
            ));
        }
        match &previous.progress {
            DeviceJoinChallengePublicationProgress::Prepared => {
                let next = DeviceJoinChallengePublicationRecord {
                    challenge,
                    progress: DeviceJoinChallengePublicationProgress::Published { authorization },
                };
                let next_json = serde_json::to_string(&next).map_err(|error| {
                    DbError::context("serialize device join challenge publication", error)
                })?;
                if !crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
                    .compare_exchange_protocol_state(key, &previous_json, &next_json)?
                {
                    return Err(DbError::Message(
                        "device join challenge publication lost its exact predecessor".to_string(),
                    ));
                }
            }
            DeviceJoinChallengePublicationProgress::Published {
                authorization: existing,
            } if existing == &authorization => {}
            DeviceJoinChallengePublicationProgress::Published { .. } => {
                return Err(DbError::Message(
                    "device join challenge publication authorization changed".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn prepare_device_join_challenge_publication(
        &self,
        challenge: coven_protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<coven_protocol::provider::DeviceJoinChallengePublicationRecord, DbError> {
        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        let prepared = DeviceJoinChallengePublicationRecord {
            challenge,
            progress: DeviceJoinChallengePublicationProgress::Prepared,
        };
        let value = serde_json::to_string(&prepared).map_err(|error| {
            DbError::context("serialize device join challenge publication", error)
        })?;
        self.call_store(move |session| {
            session.prepare_device_join_challenge_publication(&key, &value, &prepared)
        })
        .await
    }

    pub async fn publish_device_join_challenge(
        &self,
        authorization: coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: coven_protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), DbError> {
        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        self.call_store(move |session| {
            session.publish_device_join_challenge(&key, authorization, challenge)
        })
        .await
    }
}

#[async_trait::async_trait]
impl coven_protocol::provider::DeviceJoinChallengePublicationJournal for StoreDatabase {
    async fn prepare(
        &self,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<
        coven_protocol::provider::DeviceJoinChallengePublicationRecord,
        coven_protocol::objects::StorageError,
    > {
        self.prepare_device_join_challenge_publication(challenge.clone())
            .await
            .map_err(|error| coven_protocol::objects::StorageError::Storage(error.to_string()))
    }

    async fn claim_published(
        &self,
        authorization: &coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.publish_device_join_challenge(authorization.clone(), challenge.clone())
            .await
            .map_err(|error| coven_protocol::objects::StorageError::Storage(error.to_string()))
    }
}
