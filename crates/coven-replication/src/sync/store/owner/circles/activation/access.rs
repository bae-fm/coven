use super::heads::*;
use super::*;

pub(super) struct VerifiedAccessPair {
    reference: CircleAccessObjectRef,
    envelope: AccessEnvelope,
    leaf_bytes: Vec<u8>,
}

/// One identity's own resolved access at a verified control: the exact access
/// envelope and the decrypted, context-verified access leaf.
struct ResolvedIdentityAccess {
    envelope: AccessEnvelope,
    prepared_leaf: PreparedAccessLeaf,
}

/// Resolve an identity's own access leaf at an already-verified control, given the
/// control's verified access pairs and membership checkpoint. Returns `None` when
/// the identity is not a current member (removed or never added). This is the
/// identity-specific decryption step of Circle activation, split out so a
/// snapshot-restore selection can resolve its own access without re-verifying the
/// control's lineage — the control is already verified in the retained
/// materialization, and the lineage walk touches covered controls a restore may
/// have reclaimed.
fn resolve_identity_access_leaf(
    verified_access: &[VerifiedAccessPair],
    checkpoint_members: &[(String, coven_protocol::membership::MemberRole)],
    reference: &coven_protocol::store_commit::CircleControlRef,
    control: &PreparedCircleControl,
    commit: &StoreBatchCommit,
    identity: &UserKeypair,
) -> Result<Option<ResolvedIdentityAccess>, CircleOperationError> {
    let own_pubkey = keys::public_key_hex(identity);
    if !checkpoint_members
        .iter()
        .any(|(pubkey, _)| pubkey == &own_pubkey)
    {
        return Ok(None);
    }
    let owner_pubkey = &control.value.author_pubkey;
    let owner = (
        owner_pubkey.clone(),
        recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id()).map_err(
            |error| {
                CircleOperationError::InvalidState(format!(
                    "derive circle Owner recipient slot: {error}"
                ))
            },
        )?,
    );
    let access = verified_access
        .iter()
        .find(|candidate| {
            candidate.reference.envelope.owner_pubkey == owner.0
                && candidate.reference.envelope.recipient_slot == owner.1
                && candidate.reference.envelope.control_hash == reference.control().control_hash()
        })
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle activation lacks the recipient's exact access envelope".to_string(),
            )
        })?;
    let envelope = access.envelope.clone();
    let leaf_bytes = access.leaf_bytes.clone();
    let plaintext =
        keys::seal_box_decrypt(&leaf_bytes, &identity.to_x25519_secret_key()).map_err(|error| {
            CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
        })?;
    let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
        CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
    })?;
    let prepared_leaf = PreparedAccessLeaf {
        bytes: leaf_bytes,
        value: leaf,
        leaf_hash: envelope.leaf_hash,
    };
    let leaf = &prepared_leaf.value;
    if leaf.candidate_family != commit.candidate_family()
        || leaf.owner_pubkey != owner.0
        || leaf.recipient_pubkey != own_pubkey
        || leaf.recipient_slot != owner.1
        || leaf.store_membership != control.value.store_membership_state_ref()
        || leaf.epoch_id != access.reference.leaf.epoch_id
        || leaf.leaf_id != access.reference.leaf.leaf_id
        || !prepared_leaf.verify_envelope(control, &envelope, commit.candidate_family())
    {
        return Err(CircleOperationError::InvalidState(
            "circle access leaf failed context verification".to_string(),
        ));
    }
    Ok(Some(ResolvedIdentityAccess {
        envelope,
        prepared_leaf,
    }))
}

/// The restoring identity's own access at a Circle's head control: no access (the
/// gate to clear a preserved coverage row it must not retain), or active access
/// with the identity's own leaf-named bootstrap image if the leaf carries one.
pub(crate) enum LocalCircleAccess {
    NoAccess,
    Active {
        /// The Circle epoch key the identity's active leaf carries — the key
        /// standalone Circle snapshots (and the leaf bootstrap) are sealed under,
        /// which the restore selection reads their metadata and image with.
        epoch_encryption: EncryptionService,
        leaf_bootstrap: Option<VerifiedCircleImage>,
    },
}

fn consume_public_private_stream_activations(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    consumed: &mut BTreeSet<StreamActivationId>,
) -> Result<(), CircleOperationError> {
    let roster = control.value.roster_state_ref();
    let metadata = control.value.metadata_state_ref();
    for activation in commit.stream_activations() {
        let StreamActivation::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        } = activation
        else {
            continue;
        };
        let valid = match anchor {
            GrantStreamAnchor::CircleRoster {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => roster.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.roster_heads.contains(head)
            }),
            GrantStreamAnchor::CircleMetadata {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => metadata.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.metadata_heads.contains(head)
            }),
            _ => continue,
        };
        if *store_root_hash != commit.store_root_hash
            || author_registration != &commit.author_registration
            || grant_id != &control.value.author_grant_id()
            || !valid
        {
            return Err(CircleOperationError::InvalidState(
                "private Circle stream activation differs from its signed public first-head reference"
                    .to_string(),
            ));
        }
        consumed.insert(activation.activation_id());
    }
    Ok(())
}

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn load_access_pairs(
        &self,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        control: &PreparedCircleControl,
        objects: &CircleActivationObjects,
    ) -> Result<Vec<VerifiedAccessPair>, CircleOperationError> {
        let family = commit.candidate_family();
        let mut verified = Vec::with_capacity(objects.access.len());
        for reference in &objects.access {
            if reference.leaf.owner_pubkey != reference.envelope.owner_pubkey
                || reference.leaf.recipient_slot != reference.envelope.recipient_slot
                || reference.leaf.leaf_id != reference.envelope.leaf_id
                || reference.leaf.leaf_hash != reference.envelope.leaf_hash
                || reference.leaf.leaf_hash != reference.leaf.object.stored_hash()
                || reference.envelope.control_hash != control.coord.control_hash()
            {
                return Err(CircleOperationError::InvalidState(
                    "paired Circle access references differ".to_string(),
                ));
            }
            let envelope_prefix = circle_access_envelope_semantic_prefix(
                circle_id,
                family,
                &reference.envelope.owner_pubkey,
                &reference.envelope.recipient_slot,
                reference.envelope.control_hash,
            );
            let envelope_bytes = self
                .storage
                .read_protocol_object(
                    &ProtocolObjectContext::store_encrypted(
                        commit.store_root_hash,
                        ProtocolObjectDomain::CircleAccessEnvelope,
                    ),
                    &reference.envelope.object,
                    &envelope_prefix,
                )
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let envelope: AccessEnvelope =
                serde_json::from_slice(&envelope_bytes).map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "parse circle access envelope: {error}"
                    ))
                })?;
            if envelope.candidate_family != family
                || envelope.circle_id != circle_id
                || envelope.owner_pubkey != reference.envelope.owner_pubkey
                || envelope.recipient_slot != reference.envelope.recipient_slot
                || envelope.control_hash != reference.envelope.control_hash
                || envelope.leaf_id != reference.envelope.leaf_id
                || envelope.leaf_hash != reference.envelope.leaf_hash
                || !envelope.verify(control, family)
            {
                return Err(CircleOperationError::InvalidState(
                    "circle access envelope failed verification".to_string(),
                ));
            }
            let leaf_prefix = circle_access_leaf_semantic_prefix(
                circle_id,
                family,
                &reference.leaf.owner_pubkey,
                reference.leaf.epoch_id,
                &reference.leaf.recipient_slot,
                reference.leaf.leaf_id,
            );
            let leaf_bytes = self
                .storage
                .read_protocol_object(
                    &ProtocolObjectContext::recipient_sealed(
                        commit.store_root_hash,
                        ProtocolObjectDomain::CircleAccessLeaf,
                    ),
                    &reference.leaf.object,
                    &leaf_prefix,
                )
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            if ObjectHash::digest(&leaf_bytes) != reference.leaf.leaf_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle access leaf bytes differ from the paired leaf hash".to_string(),
                ));
            }
            verified.push(VerifiedAccessPair {
                reference: reference.clone(),
                envelope,
                leaf_bytes,
            });
        }
        Ok(verified)
    }

    /// Download and verify the Circle image named by an access leaf's bootstrap:
    /// the recipient's own baseline for a Circle whose accessible content predates
    /// their join, which no forward replay reconstructs. Shared by pull activation
    /// and snapshot-restore selection so both verify the image against the retained
    /// control and routing key identically.
    pub(super) async fn build_verified_leaf_bootstrap_image(
        &self,
        leaf: &CircleAccessLeaf,
        control: &PreparedCircleControl,
        bootstrap: &coven_protocol::circle::CircleBootstrapRef,
        epoch_encryption: EncryptionService,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<VerifiedCircleImage, CircleOperationError> {
        if bootstrap.schema_version != self.database.schema_version()
            || bootstrap.sync_routing_hash != self.database.sync_routing_hash()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap schema or routing contract differs from the local Store"
                    .to_string(),
            ));
        }
        let image_prefix = coven_protocol::store_commit::circle_bootstrap_image_semantic_prefix(
            leaf.circle_id,
            leaf.candidate_family,
            &leaf.owner_pubkey,
            leaf.epoch_id,
            &leaf.recipient_slot,
            bootstrap.image.image_hash,
        );
        let image_bytes = read_exact_circle_object(
            self.storage,
            &ProtocolObjectContext::circle(
                self.root().store_root_hash,
                ProtocolObjectDomain::CircleBootstrapImage,
                epoch_encryption,
            ),
            &bootstrap.image.object,
            &image_prefix,
        )
        .await?;
        coven_database::verify_circle_bootstrap_image(
            &image_bytes,
            bootstrap,
            leaf.circle_id,
            self.database.synced_tables(),
            routing_key,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        for binding in &bootstrap.blobs {
            let coven_protocol::blob::RowBlobAuthority::Remote(
                coven_protocol::audience_package::PackageAudience::Circle {
                    circle_id,
                    control: blob_control,
                    key_fingerprint,
                },
            ) = binding.authority()
            else {
                return Err(CircleOperationError::InvalidState(
                    "Circle bootstrap row blob lacks Circle package authority".to_string(),
                ));
            };
            let blob_activation = self
                .database
                .verified_circle_activation(self.root().clone(), *circle_id, blob_control.clone())
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "Circle bootstrap blob authority is not retained".to_string(),
                    )
                })?;
            let current_control = control.clone();
            let historical_control = blob_control.clone();
            let lineage_circle_id = *circle_id;
            let lineage_root = self.root().clone();
            let is_in_control_history = self
                .database
                .verified_circle_control_covers(
                    lineage_root,
                    lineage_circle_id,
                    current_control,
                    historical_control,
                )
                .await?;
            if *key_fingerprint != blob_activation.control.value.key_fingerprint()
                || !is_in_control_history
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle bootstrap blob authority is outside its control history".to_string(),
                ));
            }
            let stored = binding.stored().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle bootstrap row blob has no exact locator".to_string(),
                )
            })?;
            self.storage
                .verify_blob_object(stored)
                .await
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "verify Circle bootstrap blob {}: {error}",
                        coven_protocol::remote_object::remote_object_id(stored.object())
                    ))
                })?;
        }
        VerifiedCircleImage::new(
            leaf.circle_id,
            control.coord.clone(),
            leaf,
            bootstrap.clone(),
            image_bytes,
        )
        .map_err(CircleOperationError::from)
    }

    pub(crate) async fn resolve_local_access(
        &mut self,
        commit: &StoreBatchCommit,
        reference: &coven_protocol::store_commit::CircleControlRef,
        control: &PreparedCircleControl,
        identity: &UserKeypair,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<LocalCircleAccess, CircleOperationError> {
        let verified_access = self
            .load_access_pairs(commit, reference.circle_id(), control, reference.objects())
            .await?;
        let checkpoint_members = self.verify_control_membership(control).await?;
        let Some(resolved) = resolve_identity_access_leaf(
            &verified_access,
            checkpoint_members.as_slice(),
            reference,
            control,
            commit,
            identity,
        )?
        else {
            return Ok(LocalCircleAccess::NoAccess);
        };
        let CircleAccessDisposition::Active {
            keyring, bootstrap, ..
        } = &resolved.prepared_leaf.value.disposition
        else {
            return Ok(LocalCircleAccess::NoAccess);
        };
        let epoch_encryption =
            EncryptionService::from(MasterKeyring::from_serialized(keyring).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access keyring: {error}"))
            })?);
        let leaf_bootstrap = match bootstrap {
            Some(bootstrap) => Some(
                self.build_verified_leaf_bootstrap_image(
                    &resolved.prepared_leaf.value,
                    control,
                    bootstrap,
                    epoch_encryption.clone(),
                    routing_key,
                )
                .await?,
            ),
            None => None,
        };
        Ok(LocalCircleAccess::Active {
            epoch_encryption,
            leaf_bootstrap,
        })
    }

    pub(crate) async fn load_payload(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        identity: Option<&UserKeypair>,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        verified_prefix: &VerifiedStreamActivationPrefix,
        verified_membership_prefix: &crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
    ) -> Result<VerifiedCircleActivations, CircleOperationError> {
        let commit = verified.value();
        if commit.circle_controls().is_empty() && commit.stream_activations().is_empty() {
            return VerifiedCircleActivations::none(commit, verified.reference())
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()));
        }
        self.load_with_prefix(
            verified,
            identity,
            routing_key,
            verified_prefix,
            verified_membership_prefix,
        )
        .await
    }

    pub(crate) async fn load(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        identity: &UserKeypair,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<VerifiedCircleActivations, CircleOperationError> {
        let history_verifier = &mut *self.history;
        let commit = verified.value();
        history_verifier
            .verify_refs(crate::sync::store::owner::pull::commit_predecessor_references(commit))
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let verified_membership_prefix = history_verifier
            .verified_membership_prefix(
                crate::sync::store::owner::pull::commit_predecessor_references(commit),
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let verified_prefix = VerifiedStreamActivationPrefix::empty();
        Box::pin(self.load_with_prefix(
            verified,
            Some(identity),
            routing_key,
            &verified_prefix,
            &verified_membership_prefix,
        ))
        .await
    }

    pub(super) async fn load_with_prefix(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        identity: Option<&UserKeypair>,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        verified_prefix: &VerifiedStreamActivationPrefix,
        verified_membership_prefix: &crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
    ) -> Result<VerifiedCircleActivations, CircleOperationError> {
        let database = self.database;
        let commit_ref = verified.reference();
        let commit = verified.value();
        let author = verified.author();
        if self.root().store_root_hash != commit.store_root_hash
            || commit
                .author_registration
                .verify_registration(author)
                .is_err()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle activation authority differs from its exact Store commit".to_string(),
            ));
        }
        let mut activations = Vec::with_capacity(commit.circle_controls().len());
        let mut bootstraps = Vec::new();
        let mut local_exclusions = Vec::new();
        let mut bootstrap_pending_exclusions = Vec::new();
        let local_device_id = match identity {
            Some(_) => {
                database
                    .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
                    .await?
            }
            None => None,
        };
        let mut consumed_stream_activations = BTreeSet::new();
        for reference in commit.circle_controls() {
            let objects = reference.objects();
            let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
                circle_id: reference.circle_id(),
                control: reference.control(),
            });
            let control_bytes = read_exact_circle_object(
                self.storage,
                &ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleControl,
                ),
                &objects.control,
                &control_prefix,
            )
            .await?;
            let control_value: CircleControl =
                serde_json::from_slice(&control_bytes).map_err(|error| {
                    CircleOperationError::InvalidState(format!("parse Circle control: {error}"))
                })?;
            if control_value.control_hash() != reference.control().control_hash() {
                return Err(CircleOperationError::InvalidState(
                    "Circle control identifies itself as another control".to_string(),
                ));
            }
            let declared_coord = control_value.coord();
            if !control_value.verify()
                || verify_circle_semantic_prefix(
                    &control_prefix,
                    CircleSemanticSlot::Control {
                        circle_id: control_value.circle_id,
                        control: &declared_coord,
                    },
                )
                .is_err()
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle control failed exact verification".to_string(),
                ));
            }
            let control = PreparedCircleControl {
                coord: reference.control().clone(),
                bytes: control_bytes,
                value: control_value,
            };
            let circle_id = reference.circle_id;
            let head_hash = reference.head_hash;
            let control_coord = &reference.control;
            let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id,
                control: control_coord,
            });
            let head_object = reference.head_object();
            let bytes = read_exact_circle_object(
                self.storage,
                &ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleControl,
                ),
                head_object,
                &prefix,
            )
            .await?;
            let head: coven_protocol::circle::CircleControlHead = serde_json::from_slice(&bytes)
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "parse exact Circle control head: {error}"
                    ))
                })?;
            let CircleControlCoord {
                stream_id,
                author_pubkey,
                author_owner_grant,
                seq,
                ..
            } = &head.control;
            let authority = self
                .resolve_circle_stream_authority(
                    verified_prefix,
                    commit_ref,
                    commit,
                    head.successor.activation,
                    *stream_id,
                    circle_id,
                    author_owner_grant,
                    |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                        circle_id,
                        first_slot,
                    },
                )
                .await?;
            self.verify_circle_head_chain(
                &ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleControl,
                ),
                CircleHeadKind::Control,
                CircleHeadValue::Control(head.clone()),
                head_object.clone(),
                &authority,
            )
            .await?;
            if !head.verify(author)
                || !head.verify(&authority.registration)
                || authority.registration.author_pubkey != *author_pubkey
                || (authority.activated_here && *seq != 1)
                || head.successor.activation != authority.activation_id
                || (*seq == 1
                    && (head.successor.predecessor.is_some()
                        || head_object.slot() != &authority.first_slot))
                || head.head_hash() != head_hash
                || head.entry != objects.control
                || verify_circle_semantic_prefix(
                    &prefix,
                    CircleSemanticSlot::ControlHead {
                        circle_id: head.circle_id,
                        control: &head.control,
                    },
                )
                .is_err()
                || head.store_root_hash != commit.store_root_hash
                || head.circle_id != circle_id
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle control head failed exact verification".to_string(),
                ));
            }
            if authority.activated_here {
                consumed_stream_activations.insert(authority.activation_id);
            }
            self.verify_covered_control_heads(verified_prefix, commit_ref, commit, &control.value)
                .await?;
            verify_control_context_for_verified_commit(reference, &control, verified)?;
            consume_public_private_stream_activations(
                commit,
                author,
                reference.circle_id(),
                &control,
                objects,
                &mut consumed_stream_activations,
            )?;
            let verified_access = self
                .load_access_pairs(commit, reference.circle_id(), &control, objects)
                .await?;
            let checkpoint_members = self
                .verify_control_membership_at_verified_prefix(&control, verified_membership_prefix)
                .await?;
            if control.value.state().is_deleted() {
                // A deletion carries no access material. It activates to the
                // terminal Deleted state with no local access; materialization
                // prunes the Circle's rows and caches from the winning chain.
                activations.push(VerifiedCircleReference {
                    reference: reference.clone(),
                    circle_id: reference.circle_id(),
                    control,
                    local_access: None,
                });
                continue;
            }
            let Some(identity) = identity else {
                activations.push(VerifiedCircleReference {
                    reference: reference.clone(),
                    circle_id: reference.circle_id(),
                    control,
                    local_access: None,
                });
                continue;
            };
            let Some(resolved) = resolve_identity_access_leaf(
                &verified_access,
                &checkpoint_members,
                reference,
                &control,
                commit,
                identity,
            )?
            else {
                activations.push(VerifiedCircleReference {
                    reference: reference.clone(),
                    circle_id: reference.circle_id(),
                    control,
                    local_access: None,
                });
                continue;
            };
            let ResolvedIdentityAccess {
                envelope,
                prepared_leaf,
            } = resolved;
            let leaf = &prepared_leaf.value;
            let active = match &leaf.disposition {
                CircleAccessDisposition::Active { keyring, .. } => {
                    let encryption = EncryptionService::from(
                        MasterKeyring::from_serialized(keyring).map_err(|error| {
                            CircleOperationError::InvalidState(format!(
                                "parse circle access keyring: {error}"
                            ))
                        })?,
                    );
                    let authority_roster = self
                        .load_circle_authority_roster(
                            verified_prefix,
                            commit,
                            reference.circle_id(),
                            &control,
                            encryption.clone(),
                            objects,
                            commit_ref,
                            &mut consumed_stream_activations,
                        )
                        .await?;
                    if !verify_merge_circle_owner_authority(
                        &control.value.author_pubkey,
                        &control.value.value.author_authority,
                        &authority_roster,
                    ) {
                        return Err(CircleOperationError::InvalidState(
                            "circle control author lacks its exact historical Owner grant"
                                .to_string(),
                        ));
                    }
                    let roster_chain = self
                        .load_circle_roster_chain(
                            verified_prefix,
                            commit_ref,
                            commit,
                            reference.circle_id(),
                            &control.value.roster_state_ref(),
                            encryption.clone(),
                            objects,
                            &mut consumed_stream_activations,
                        )
                        .await?;
                    let resolved = roster_chain
                        .try_resolved()
                        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
                    let close_outcome = self
                        .verify_epoch_close(
                            commit,
                            &control,
                            objects,
                            encryption.clone(),
                            &roster_chain,
                        )
                        .await?;
                    if let (Some(outcome), Some(local_device_id)) =
                        (&close_outcome, local_device_id.as_deref())
                    {
                        if let Some(excluded) = outcome.exclusions.iter().find(|registration| {
                            registration.device_id.to_string() == local_device_id
                        }) {
                            local_exclusions.push(LocalCircleExclusion {
                                circle_id: reference.circle_id(),
                                close_id: outcome.close_id,
                                excluded: excluded.clone(),
                                successor_control: control.coord.clone(),
                                activating_commit: commit_ref.clone(),
                            });
                        }
                    }
                    let resolved_members = resolved.members();
                    if !resolved_members.contains_key(&leaf.recipient_pubkey) {
                        return Err(CircleOperationError::InvalidState(
                            "circle Active access recipient is absent from its resolved roster"
                                .to_string(),
                        ));
                    }
                    let roster_owners = resolved_members
                        .iter()
                        .filter_map(|(pubkey, role)| {
                            (*role == coven_protocol::circle::CircleRole::Owner)
                                .then_some(pubkey.clone())
                        })
                        .collect::<Vec<_>>();
                    if roster_owners != control.value.owners() {
                        return Err(CircleOperationError::InvalidState(
                            "circle control Owners differ from its roster".to_string(),
                        ));
                    }
                    let metadata_state = control.value.metadata_state_ref();
                    let metadata = self
                        .load_circle_metadata_state(
                            verified_prefix,
                            commit,
                            reference.circle_id(),
                            &metadata_state,
                            encryption.clone(),
                            objects,
                            commit_ref,
                            &mut consumed_stream_activations,
                        )
                        .await?;
                    if let CircleAccessDisposition::Active {
                        bootstrap: Some(bootstrap),
                        ..
                    } = &leaf.disposition
                    {
                        // An excluded device that cannot yet read its successor bootstrap
                        // image defers the reset: flag the exclusion so the pull records
                        // it (detection is derived from the verified outcome above, not the
                        // bootstrap) and holds the successor. Its publication stays gated
                        // until a later pull reads the image and the reseed records
                        // coverage. The image read is the only source of a `CircleObject`
                        // error here — verification and blob checks fail as `InvalidState`.
                        match self
                            .build_verified_leaf_bootstrap_image(
                                leaf,
                                &control,
                                bootstrap,
                                encryption,
                                routing_key,
                            )
                            .await
                        {
                            Ok(image) => bootstraps.push(image),
                            Err(error @ CircleOperationError::Object(_)) => {
                                if let Some(exclusion) = local_exclusions
                                    .iter()
                                    .find(|exclusion| exclusion.circle_id == reference.circle_id())
                                {
                                    bootstrap_pending_exclusions.push(exclusion.clone());
                                    continue;
                                }
                                return Err(error);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Some(VerifiedCircleActive {
                        roster: resolved,
                        metadata,
                    })
                }
                CircleAccessDisposition::Inactive => None,
            };
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: Some(VerifiedCircleAccess {
                    envelope: envelope.clone(),
                    leaf: prepared_leaf,
                    active,
                }),
            });
        }
        let declared = commit
            .stream_activations()
            .iter()
            .map(StreamActivation::activation_id)
            .collect::<BTreeSet<_>>();
        if consumed_stream_activations != declared {
            return Err(CircleOperationError::InvalidState(
                "Store commit stream activations do not exactly introduce its first Circle heads"
                    .to_string(),
            ));
        }
        let stream_activations =
            VerifiedStreamActivations::from_verified_circle_commit(commit, commit_ref)
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        Ok(VerifiedCircleActivations::from_verified_parts(
            activations,
            stream_activations,
            bootstraps,
            local_exclusions,
            bootstrap_pending_exclusions,
        ))
    }
}
