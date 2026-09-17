//! Which key opens a Circle's content.
//!
//! A Circle package, bootstrap blob, or spooled row blob is sealed to one epoch
//! key. These reads resolve the key a control issued, fall back to the
//! historical keyring a covering control still retains, and refuse a blob whose
//! claimed authority sits outside the control history that published it.

use super::activation::verified_circle_activation_with_prefix_on;
use super::lineage::verified_circle_control_covers_with_prefix_on;
use crate::store::store_session::verified_store_authority::VerifiedStoreLookup;
use crate::store::store_session::{StoreRecords, StoreSession};
use crate::*;
use coven_keys::encryption::EncryptionService;

/// Package keys resolved against the installed state and verified prepared controls.
/// Successor keys still require the historical roster to authorize the package author.
pub enum CirclePackageAccess {
    Exact(coven_protocol::circle_activation::CircleEpochAccess),
    Historical(String),
}

impl StoreSession<'_> {
    fn circle_epoch_access(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.verified_store_authority.retained_replay_inputs_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            root,
        )?;
        let Some(activation) = self
            .verified_store_authority
            .verified_circle_activation_on(
                crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
                circle_id,
                expected_control,
            )?
        else {
            return Ok(None);
        };
        activation.epoch_access().map_err(DbError::from)
    }

    fn circle_package_access(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<CirclePackageAccess>, DbError> {
        let Some(state) =
            self.circle_current_state_with_activations(root, circle_id, activations)?
        else {
            return Ok(None);
        };
        if state.is_deleted() {
            return Ok(None);
        }
        let exact = match activations.iter().find(|activation| {
            activation.circle_id == circle_id && &activation.control.coord == expected_control
        }) {
            Some(activation) => activation.epoch_access().map_err(DbError::from)?,
            None => self.circle_epoch_access(root, circle_id, expected_control)?,
        };
        if let Some(access) = exact {
            // Exact access remains valid for packages within the accepted epoch
            // cutoff even after a successor removes the local member.
            return Ok(Some(CirclePackageAccess::Exact(access)));
        }
        self.circle_historical_package_keyring(
            root,
            state,
            expected_control,
            expected_key_fingerprint,
            activations,
        )
        .map(|keyring| keyring.map(CirclePackageAccess::Historical))
    }

    fn circle_historical_package_keyring(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        state: coven_protocol::circle_activation::CircleCurrentState,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<String>, DbError> {
        let circle_id = state.circle_id();
        if !state.verify() {
            return Err(DbError::Message(
                "invalid projected Circle package state".to_string(),
            ));
        }
        let Some(current) = state
            .authoring_state()
            .or_else(|| state.closing_authoring_state())
        else {
            return Ok(None);
        };
        let Some(historical) = verified_circle_activation_with_prefix_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            expected_control,
            activations,
        )?
        else {
            return Ok(None);
        };
        if !verified_circle_control_covers_with_prefix_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &current.control,
            expected_control,
            activations,
        )? || current.control.value.epoch_id() != historical.control.value.epoch_id()
            || current.control.value.key_fingerprint() != expected_key_fingerprint
            || historical.control.value.key_fingerprint() != expected_key_fingerprint
        {
            return Ok(None);
        }
        let coven_protocol::circle::CircleAccessDisposition::Active { keyring, .. } =
            &current.access.disposition
        else {
            return Ok(None);
        };
        let parsed =
            coven_keys::encryption::MasterKeyring::from_serialized(keyring).map_err(|error| {
                DbError::context(
                    format!("parse Circle {circle_id} historical package keyring"),
                    error,
                )
            })?;
        let encryption = EncryptionService::from(parsed);
        if encryption
            .service_for_fingerprint(expected_key_fingerprint.as_bytes())
            .is_err()
        {
            return Ok(None);
        }
        Ok(Some(keyring.clone()))
    }

    fn circle_blob_opening_protection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        circle_blob_opening_protection_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }

    fn verify_circle_bootstrap_blob_authority(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        current: &coven_protocol::circle::PreparedCircleControl,
        blobs: &[coven_protocol::blob::RowBlobRef],
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.conn, self.store_dir);
        for binding in blobs {
            let coven_protocol::blob::RowBlobAuthority::Remote(
                coven_protocol::audience_package::PackageAudience::Circle {
                    circle_id,
                    control,
                    key_fingerprint,
                },
            ) = binding.authority()
            else {
                return Err(DbError::Message(
                    "Circle bootstrap row blob lacks Circle package authority".to_string(),
                ));
            };
            let activation = verified_circle_activation_with_prefix_on(
                records,
                self.verified_store_authority,
                root,
                *circle_id,
                control,
                activations,
            )?
            .ok_or_else(|| {
                DbError::Message("Circle bootstrap blob authority is not retained".to_string())
            })?;
            if *key_fingerprint != activation.control.value.key_fingerprint()
                || !verified_circle_control_covers_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    *circle_id,
                    current,
                    control,
                    activations,
                )?
            {
                return Err(DbError::Message(
                    "Circle bootstrap blob authority is outside its control history".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl StoreDatabase {
    /// Verify bootstrap blob authority against retained controls and the caller's
    /// verified candidate predecessor controls in one ancestry traversal owner.
    pub async fn verify_circle_bootstrap_blob_authority(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        current: coven_protocol::circle::PreparedCircleControl,
        blobs: Vec<coven_protocol::blob::RowBlobRef>,
        activations: Vec<coven_protocol::circle_activation::VerifiedCircleReference>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.verify_circle_bootstrap_blob_authority(&root, &current, &blobs, &activations)
        })
        .await
    }

    pub async fn circle_epoch_access(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.call_store(move |session| {
            session.circle_epoch_access(&root, circle_id, &expected_control)
        })
        .await
    }

    pub async fn circle_package_access(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: Vec<coven_protocol::circle_activation::VerifiedCircleReference>,
    ) -> Result<Option<CirclePackageAccess>, DbError> {
        self.call_store(move |session| {
            session.circle_package_access(
                &root,
                circle_id,
                &expected_control,
                expected_key_fingerprint,
                &activations,
            )
        })
        .await
    }

    pub async fn circle_blob_opening_protection(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        self.call_store(move |session| {
            session.circle_blob_opening_protection(
                &root,
                circle_id,
                &expected_control,
                expected_key_fingerprint,
            )
        })
        .await
    }
}

pub(super) fn circle_blob_opening_protection_on(
    records: StoreRecords<'_>,
    verified_store: &mut dyn VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    expected_control: &coven_protocol::circle::CircleControlCoord,
    expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
    let Some(authority) = StoreDatabase::verified_circle_activation_on(
        records,
        verified_store,
        root,
        circle_id,
        expected_control,
    )?
    else {
        return Err(DbError::Message(format!(
            "Circle {circle_id} has no retained authority for control {expected_control:?}"
        )));
    };
    if authority.control.value.key_fingerprint() != expected_key_fingerprint {
        return Err(DbError::Message(format!(
            "Circle {circle_id} blob key {expected_key_fingerprint} differs from \
                 exact control {expected_control:?}"
        )));
    }

    let controls = records.circle_controls(circle_id)?;

    let mut retained_key = None;
    for control in controls {
        let activation = StoreDatabase::verified_circle_activation_on(
            records,
            verified_store,
            root,
            circle_id,
            &control,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} activation index lost control {control:?}"
            ))
        })?;
        let Some((generation, key)) = activation
            .retained_key_entry(expected_key_fingerprint)
            .map_err(DbError::from)?
        else {
            continue;
        };
        let candidate = EncryptionService::from_key_at_generation(generation, key);
        if retained_key
            .as_ref()
            .is_some_and(|existing: &EncryptionService| {
                existing.current_generation() != generation || existing.key_bytes() != key
            })
        {
            return Err(DbError::Message(format!(
                "Circle {circle_id} retains inconsistent key material for fingerprint \
                     {expected_key_fingerprint}"
            )));
        }
        retained_key = Some(candidate);
    }
    retained_key
        .map(coven_protocol::objects::BlobSpoolProtection::Opaque)
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retains no local key for fingerprint \
                     {expected_key_fingerprint}"
            ))
        })
}
