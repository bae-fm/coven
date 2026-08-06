use crate::database::*;

use super::*;

impl StoreDatabase {
    pub(crate) async fn prepare_device_join_challenge_publication(
        &self,
        challenge: crate::protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<crate::protocol::provider::DeviceJoinChallengePublicationRecord, DbError> {
        use crate::protocol::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

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
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (&key, &value),
                )
                .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(&tx, &key)?;
                tx.commit().map_err(DbError::from)?;
                let actual: DeviceJoinChallengePublicationRecord = serde_json::from_str(&actual)
                    .map_err(|error| {
                        DbError::context("parse device join challenge publication", error)
                    })?;
                if actual.challenge != prepared.challenge {
                    return Err(DbError::Message(
                        "device join challenge probe id was reused with different bytes"
                            .to_string(),
                    ));
                }
                Ok(actual)
            })
            .await
    }

    pub(crate) async fn publish_device_join_challenge(
        &self,
        authorization: crate::protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: crate::protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), DbError> {
        use crate::protocol::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let previous_json = crate::database::required_protocol_state_on(&tx, &key)?;
                let previous: DeviceJoinChallengePublicationRecord =
                    serde_json::from_str(&previous_json).map_err(|error| {
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
                            progress: DeviceJoinChallengePublicationProgress::Published {
                                authorization,
                            },
                        };
                        let next_json = serde_json::to_string(&next).map_err(|error| {
                            DbError::context("serialize device join challenge publication", error)
                        })?;
                        let changed = tx
                        .execute(
                            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                            (&next_json, &key, &previous_json),
                        )
                        .map_err(DbError::from)?;
                        if changed != 1 {
                            return Err(DbError::Message(
                                "device join challenge publication lost its exact predecessor"
                                    .to_string(),
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
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}

#[async_trait::async_trait]
impl crate::protocol::provider::DeviceJoinChallengePublicationJournal for StoreDatabase {
    async fn prepare(
        &self,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<
        crate::protocol::provider::DeviceJoinChallengePublicationRecord,
        crate::protocol::objects::StorageError,
    > {
        self.prepare_device_join_challenge_publication(challenge.clone())
            .await
            .map_err(|error| crate::protocol::objects::StorageError::Storage(error.to_string()))
    }

    async fn claim_published(
        &self,
        authorization: &crate::protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.publish_device_join_challenge(authorization.clone(), challenge.clone())
            .await
            .map_err(|error| crate::protocol::objects::StorageError::Storage(error.to_string()))
    }
}
