use crate::database::*;

use super::*;

impl StoreDatabase {
    pub(crate) async fn prepare_device_join_challenge_publication(
        &self,
        challenge: crate::sync::provider::CrossPrincipalProbeChallenge,
    ) -> Result<crate::sync::provider::DeviceJoinChallengePublicationRecord, DbError> {
        use crate::sync::provider::{
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
            DbError::Message(format!(
                "serialize device join challenge publication: {error}"
            ))
        })?;
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (&key, &value),
                )
                .map_err(DbError::from)?;
                let actual: String = tx
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [&key],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)?;
                let actual: DeviceJoinChallengePublicationRecord = serde_json::from_str(&actual)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "parse device join challenge publication: {error}"
                        ))
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
        authorization: crate::sync::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: crate::sync::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), DbError> {
        use crate::sync::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let previous_json: String = tx
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [&key],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let previous: DeviceJoinChallengePublicationRecord =
                    serde_json::from_str(&previous_json).map_err(|error| {
                        DbError::Message(format!(
                            "parse device join challenge publication: {error}"
                        ))
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
                            DbError::Message(format!(
                                "serialize device join challenge publication: {error}"
                            ))
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
                    DeviceJoinChallengePublicationProgress::ProducerClosed { .. }
                    | DeviceJoinChallengePublicationProgress::CancelledBeforeCreate { .. } => {
                        return Err(DbError::Message(
                            "device join challenge producer is closed".to_string(),
                        ));
                    }
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn close_published_device_join_challenge(
        &self,
        authorization: crate::sync::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: crate::sync::provider::CrossPrincipalProbeChallenge,
    ) -> Result<(), DbError> {
        use crate::sync::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let previous_json: String = tx
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [&key],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let previous: DeviceJoinChallengePublicationRecord =
                    serde_json::from_str(&previous_json).map_err(|error| {
                        DbError::Message(format!(
                            "parse device join challenge publication: {error}"
                        ))
                    })?;
                if previous.challenge != challenge {
                    return Err(DbError::Message(
                        "device join challenge closure differs from prepared bytes".to_string(),
                    ));
                }
                match &previous.progress {
                    DeviceJoinChallengePublicationProgress::Published {
                        authorization: existing,
                    } if existing == &authorization => {
                        let next = DeviceJoinChallengePublicationRecord {
                            challenge,
                            progress: DeviceJoinChallengePublicationProgress::ProducerClosed {
                                authorization,
                            },
                        };
                        let next_json = serde_json::to_string(&next).map_err(|error| {
                            DbError::Message(format!(
                                "serialize device join challenge closure: {error}"
                            ))
                        })?;
                        let changed = tx
                        .execute(
                            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                            (&next_json, &key, &previous_json),
                        )
                        .map_err(DbError::from)?;
                        if changed != 1 {
                            return Err(DbError::Message(
                                "device join challenge closure lost its exact predecessor"
                                    .to_string(),
                            ));
                        }
                    }
                    DeviceJoinChallengePublicationProgress::ProducerClosed {
                        authorization: existing,
                    } if existing == &authorization => {}
                    _ => {
                        return Err(DbError::Message(
                            "device join challenge cannot close from its current state".to_string(),
                        ));
                    }
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn cancel_unpublished_device_join_challenge(
        &self,
        authorization: crate::sync::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: crate::sync::provider::CrossPrincipalProbeChallenge,
        cancellation: crate::sync::store_commit::DeviceJoinOutcomeRef,
    ) -> Result<(), DbError> {
        use crate::sync::provider::{
            DeviceJoinChallengePublicationProgress, DeviceJoinChallengePublicationRecord,
        };

        if cancellation.attempt() != &authorization.attempt {
            return Err(DbError::Message(
                "device join challenge cancellation names another attempt".to_string(),
            ));
        }
        let key = format!(
            "device_join_challenge_publication/{}",
            hex::encode(challenge.probe_id.as_bytes())
        );
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let previous_json: String = tx
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [&key],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let previous: DeviceJoinChallengePublicationRecord =
                    serde_json::from_str(&previous_json).map_err(|error| {
                        DbError::Message(format!(
                            "parse device join challenge publication: {error}"
                        ))
                    })?;
                if previous.challenge != challenge {
                    return Err(DbError::Message(
                        "device join challenge cancellation differs from prepared bytes"
                            .to_string(),
                    ));
                }
                match &previous.progress {
                    DeviceJoinChallengePublicationProgress::Prepared => {
                        let next = DeviceJoinChallengePublicationRecord {
                            challenge,
                            progress:
                                DeviceJoinChallengePublicationProgress::CancelledBeforeCreate {
                                    authorization,
                                    cancellation,
                                },
                        };
                        let next_json = serde_json::to_string(&next).map_err(|error| {
                            DbError::Message(format!(
                                "serialize device join challenge cancellation: {error}"
                            ))
                        })?;
                        let changed = tx
                        .execute(
                            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                            (&next_json, &key, &previous_json),
                        )
                        .map_err(DbError::from)?;
                        if changed != 1 {
                            return Err(DbError::Message(
                                "device join challenge cancellation lost its exact predecessor"
                                    .to_string(),
                            ));
                        }
                    }
                    DeviceJoinChallengePublicationProgress::CancelledBeforeCreate {
                        authorization: existing_authorization,
                        cancellation: existing_cancellation,
                    } if existing_authorization == &authorization
                        && existing_cancellation == &cancellation => {}
                    _ => {
                        return Err(DbError::Message(
                        "device join challenge cannot cancel before create from its current state"
                            .to_string(),
                    ));
                    }
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}
