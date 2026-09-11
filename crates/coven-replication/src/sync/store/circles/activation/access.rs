use super::heads::*;
use super::*;

/// Resolve an identity's own access leaf at an already-verified control, from the
/// control's own signed access map and membership checkpoint. Returns `None` when
/// the identity is not a current member (removed or never added). This is the
/// identity-specific decryption step of Circle activation, split out so a
/// snapshot-restore selection can resolve its own access without re-verifying the
/// control's lineage — the control is already verified in the retained
/// materialization, and the lineage walk touches covered controls a restore may
/// have reclaimed.
fn resolve_identity_access_leaf(
    checkpoint_members: &[(String, coven_protocol::membership::MemberRole)],
    reference: &coven_protocol::store_commit::CircleControlRef,
    control: &PreparedCircleControl,
    commit: &StoreBatchCommit,
    identity: &UserKeypair,
) -> Result<Option<PreparedAccessLeaf>, CircleOperationError> {
    let own_pubkey = keys::public_key_hex(identity);
    if !checkpoint_members
        .iter()
        .any(|(pubkey, _)| pubkey == &own_pubkey)
    {
        return Ok(None);
    }
    let owner_pubkey = &control.value.author_pubkey;
    let recipient_slot = recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id())?;
    let entry = control
        .value
        .value
        .access
        .entry(&recipient_slot)
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle activation lacks the recipient's exact access entry".to_string(),
            )
        })?;
    let prepared = PreparedAccessLeaf::open(entry, identity)?;
    let leaf = &prepared.value;
    if leaf.candidate_family != commit.candidate_family()
        || leaf.owner_pubkey != *owner_pubkey
        || leaf.recipient_pubkey != own_pubkey
        || leaf.recipient_slot != recipient_slot
        || leaf.store_membership != control.value.store_membership_state_ref()
        || leaf.epoch_id != control.value.epoch_id()
        || !prepared.verify(control, commit.candidate_family())
    {
        return Err(CircleOperationError::InvalidState(
            "circle access leaf failed context verification".to_string(),
        ));
    }
    if let CircleAccessDisposition::Active {
        bootstrap: Some(bootstrap),
        ..
    } = &leaf.disposition
    {
        if !reference.objects().names_bootstrap(leaf, bootstrap) {
            return Err(CircleOperationError::InvalidState(
                "Circle access bootstrap is absent from its signed object graph".to_string(),
            ));
        }
    }
    Ok(Some(prepared))
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
        predecessors: Vec<VerifiedCircleReference>,
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
        let image_bytes = self
            .database
            .verify_circle_bootstrap_image(
                image_bytes,
                bootstrap.clone(),
                leaf.circle_id,
                routing_key.cloned(),
            )
            .await
            .map_err(CircleOperationError::from)?;
        self.database
            .verify_circle_bootstrap_blob_authority(
                self.root().clone(),
                control.clone(),
                bootstrap.blobs.clone(),
                predecessors,
            )
            .await?;
        for binding in &bootstrap.blobs {
            let stored = binding.stored().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle bootstrap row blob has no exact locator".to_string(),
                )
            })?;
            self.storage.verify_blob_object(stored).await?;
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

    async fn verify_active_access(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        reference: &coven_protocol::store_commit::CircleControlRef,
        control: &PreparedCircleControl,
        leaf: &CircleAccessLeaf,
        encryption: EncryptionService,
        verified_prefix: &VerifiedStreamActivationPrefix,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
        prepared: &[&VerifiedCircleActivations],
    ) -> Result<(VerifiedCircleActive, Option<VerifiedCloseOutcome>), CircleOperationError> {
        let commit = verified.value();
        let commit_ref = verified.reference();
        let objects = reference.objects();
        let authority_roster = self
            .load_circle_authority_roster(
                verified_prefix,
                commit,
                reference.circle_id(),
                control,
                encryption.clone(),
                objects,
                commit_ref,
                consumed_stream_activations,
            )
            .await?;
        if !verify_merge_circle_owner_authority(
            &control.value.author_pubkey,
            &control.value.value.author_authority,
            &authority_roster,
        ) {
            return Err(CircleOperationError::InvalidState(
                "circle control author lacks its exact historical Owner grant".to_string(),
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
                consumed_stream_activations,
            )
            .await?;
        let resolved = roster_chain
            .try_resolved()
            .map_err(CircleOperationError::from)?;
        let close_outcome = self
            .verify_epoch_close(
                commit,
                control,
                objects,
                encryption.clone(),
                &roster_chain,
                prepared,
            )
            .await?;
        let resolved_members = resolved.members();
        if !resolved_members.contains_key(&leaf.recipient_pubkey) {
            return Err(CircleOperationError::InvalidState(
                "circle Active access recipient is absent from its resolved roster".to_string(),
            ));
        }
        let roster_owners = resolved_members
            .iter()
            .filter_map(|(pubkey, role)| {
                (*role == coven_protocol::circle::CircleRole::Owner).then_some(pubkey.clone())
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
                consumed_stream_activations,
            )
            .await?;
        Ok((
            VerifiedCircleActive {
                roster: resolved,
                metadata,
            },
            close_outcome,
        ))
    }

    pub(crate) async fn resolve_local_access(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        reference: &coven_protocol::store_commit::CircleControlRef,
        control: &PreparedCircleControl,
        identity: &UserKeypair,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<coven_database::StagedCircleAccess, CircleOperationError> {
        verify_control_context_for_verified_commit(reference, control, verified)?;
        let commit = verified.value();
        let checkpoint_members = self.verify_control_membership(control).await?;
        let resolved = if control.value.state().is_deleted() {
            None
        } else {
            resolve_identity_access_leaf(&checkpoint_members, reference, control, commit, identity)?
        };
        let local_device_id = self
            .database
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?;
        let mut local_exclusion = None;
        let mut leaf_bootstrap = None;
        let local_access = match resolved {
            None => None,
            Some(prepared_leaf) => {
                let leaf = &prepared_leaf.value;
                let active = match &leaf.disposition {
                    CircleAccessDisposition::Inactive => None,
                    CircleAccessDisposition::Active {
                        keyring, bootstrap, ..
                    } => {
                        let encryption =
                            EncryptionService::from(MasterKeyring::from_serialized(keyring)?);
                        let (active, close_outcome) = self
                            .verify_active_access(
                                verified,
                                reference,
                                control,
                                leaf,
                                encryption.clone(),
                                &VerifiedStreamActivationPrefix::empty(),
                                &mut BTreeSet::new(),
                                &[],
                            )
                            .await?;
                        if let (Some(outcome), Some(device_id)) =
                            (&close_outcome, local_device_id.as_deref())
                        {
                            local_exclusion =
                                outcome.local_exclusion(control, verified.reference(), device_id);
                        }
                        if let Some(bootstrap) = bootstrap {
                            leaf_bootstrap = Some(
                                self.build_verified_leaf_bootstrap_image(
                                    leaf,
                                    control,
                                    bootstrap,
                                    encryption,
                                    routing_key,
                                    Vec::new(),
                                )
                                .await?,
                            );
                        }
                        Some(active)
                    }
                };
                Some(VerifiedCircleAccess {
                    leaf: prepared_leaf,
                    active,
                })
            }
        };
        Ok(coven_database::StagedCircleAccess {
            activating_commit: verified.reference().clone(),
            activation: VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control: control.clone(),
                local_access,
            },
            leaf_bootstrap,
            local_exclusion,
        })
    }

    pub(crate) async fn load_payload(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        identity: Option<&UserKeypair>,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        verified_prefix: &VerifiedStreamActivationPrefix,
        verified_membership_prefix: &crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
        prepared: &[&VerifiedCircleActivations],
    ) -> Result<VerifiedCircleActivations, CircleOperationError> {
        let commit = verified.value();
        if commit.circle_controls().is_empty() && commit.stream_activations().is_empty() {
            return VerifiedCircleActivations::none(commit, verified.reference())
                .map_err(CircleOperationError::from);
        }
        self.load_with_prefix(
            verified,
            identity,
            routing_key,
            verified_prefix,
            verified_membership_prefix,
            prepared,
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
            .verify_refs(crate::sync::store::pull::commit_predecessor_references(
                commit,
            ))
            .await
            .map_err(CircleOperationError::from)?;
        let verified_membership_prefix = history_verifier
            .verified_membership_prefix(crate::sync::store::pull::commit_predecessor_references(
                commit,
            ))
            .map_err(CircleOperationError::from)?;
        let verified_prefix = VerifiedStreamActivationPrefix::empty();
        Box::pin(self.load_with_prefix(
            verified,
            Some(identity),
            routing_key,
            &verified_prefix,
            &verified_membership_prefix,
            &[],
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
        prepared: &[&VerifiedCircleActivations],
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
            let control_value: CircleControl = serde_json::from_slice(&control_bytes)?;
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
            let head: coven_protocol::circle::CircleControlHead = serde_json::from_slice(&bytes)?;
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
            let Some(prepared_leaf) = resolve_identity_access_leaf(
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
            let leaf = &prepared_leaf.value;
            let active = match &leaf.disposition {
                CircleAccessDisposition::Active { keyring, .. } => {
                    let encryption =
                        EncryptionService::from(MasterKeyring::from_serialized(keyring)?);
                    let (active, close_outcome) = self
                        .verify_active_access(
                            verified,
                            reference,
                            &control,
                            leaf,
                            encryption.clone(),
                            verified_prefix,
                            &mut consumed_stream_activations,
                            prepared,
                        )
                        .await?;
                    if let (Some(outcome), Some(local_device_id)) =
                        (&close_outcome, local_device_id.as_deref())
                    {
                        if let Some(exclusion) =
                            outcome.local_exclusion(&control, commit_ref, local_device_id)
                        {
                            local_exclusions.push(exclusion);
                        }
                    }
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
                                self.history.verified_circle_predecessors(
                                    commit,
                                    reference.circle_id(),
                                    prepared,
                                )?,
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
                    Some(active)
                }
                CircleAccessDisposition::Inactive => None,
            };
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: Some(VerifiedCircleAccess {
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
                .map_err(CircleOperationError::from)?;
        Ok(VerifiedCircleActivations::from_verified_parts(
            activations,
            stream_activations,
            bootstraps,
            local_exclusions,
            bootstrap_pending_exclusions,
        ))
    }
}
