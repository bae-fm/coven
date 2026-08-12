use super::*;

#[derive(Debug, Clone)]
pub struct StoreOwnerAnchor {
    root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
    founder: coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
    authority: RetainedReplayGenesisAuthority,
    genesis: ResolvedStoreDeviceState,
}

impl StoreOwnerAnchor {
    pub fn new(
        root_reference: coven_protocol::store_commit::StoreRootRef,
        root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
        founder_reference: StoreDeviceRegistrationRef,
        founder: coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
    ) -> Result<Self, DbError> {
        let parsed_root_reference = coven_protocol::store_commit::StoreRootRef {
            store_root_id: root.value.descriptor.store_root_id(),
            store_root_hash: root.value.object_hash(),
            object: root.object.clone(),
        };
        if root.bytes != root.value.to_bytes()
            || root.semantic_hash != root.value.object_hash()
            || parsed_root_reference != root_reference
        {
            return Err(DbError::Message(
                "Store owner root differs from its verified exact object".to_string(),
            ));
        }
        let parsed_founder_reference =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        if founder.bytes != founder.value.to_bytes()
            || founder.semantic_hash != founder.value.registration_hash()
            || parsed_founder_reference != founder_reference
            || founder.value.author_pubkey != root.value.descriptor.founder_pubkey
            || founder.object.slot() != &root.value.descriptor.founder_registration
            || founder.value.provider != root.value.descriptor.founder_provider_admin.provider
            || !matches!(
                founder.value.origin,
                coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            )
        {
            return Err(DbError::Message(
                "Store owner founder registration differs from its root or verified exact object"
                    .to_string(),
            ));
        }
        let genesis = ResolvedStoreDeviceState::founder(
            &root_reference,
            founder_reference.clone(),
            &root.value.descriptor.founder_pubkey,
            root.value.descriptor.founder_grant.clone(),
            &root.value.descriptor.founder_recovery,
        )
        .map_err(DbError::from)?;
        Ok(Self {
            root,
            founder,
            authority: RetainedReplayGenesisAuthority {
                store_root: root_reference,
                founder_registration: founder_reference,
            },
            genesis,
        })
    }

    pub(crate) fn authority(&self) -> &RetainedReplayGenesisAuthority {
        &self.authority
    }

    pub(crate) fn root(&self) -> &coven_protocol::objects::VerifiedObject<StoreProtocolRoot> {
        &self.root
    }

    pub(crate) fn founder(
        &self,
    ) -> &coven_protocol::objects::VerifiedObject<StoreDeviceRegistration> {
        &self.founder
    }

    pub(crate) fn genesis(&self) -> &ResolvedStoreDeviceState {
        &self.genesis
    }

    pub(crate) fn owner(&self) -> &str {
        &self.root.value.descriptor.founder_pubkey
    }
}

#[derive(Debug, Clone)]
pub struct DurableFounderGraph {
    pub root: ExactProtocolObject<StoreProtocolRoot>,
    pub registration: ExactProtocolObject<StoreDeviceRegistration>,
    pub initial_ack: ExactProtocolObject<StoreAck>,
    pub initial_ack_ref: StoreAckRef,
    pub membership: DurableFounderMembership,
    pub registration_state: LocalDeviceRegistrationState,
}

impl DurableFounderGraph {
    pub fn validate(&self) -> Result<(), DbError> {
        let root = StoreProtocolRoot::parse(&self.root.bytes)
            .map_err(|error| DbError::context("founder Store root", error))?;
        if root != self.root.value || root.object_hash() != self.root.value.object_hash() {
            return Err(DbError::Message(
                "founder Store root differs from its prepared exact object".to_string(),
            ));
        }
        let root_ref = coven_protocol::store_commit::StoreRootRef {
            store_root_id: root.descriptor.store_root_id(),
            store_root_hash: root.object_hash(),
            object: self.root.prepared.reference().clone(),
        };
        let registration = StoreDeviceRegistration::parse_at(
            &self.registration.bytes,
            &root_ref,
            self.registration.value.device_id,
        )
        .map_err(|error| DbError::context("founder Store registration", error))?;
        if registration != self.registration.value
            || registration.author_pubkey != root.descriptor.founder_pubkey
            || self.registration.prepared.reference().slot()
                != &root.descriptor.founder_registration
            || registration.provider != root.descriptor.founder_provider_admin.provider
            || !matches!(
                registration.origin,
                coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            )
        {
            return Err(DbError::Message(
                "founder registration differs from its root or prepared exact object".to_string(),
            ));
        }
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            self.registration.prepared.reference().clone(),
        );
        let initial_ack = StoreAck::parse_at(
            &self.initial_ack.bytes,
            &root_ref,
            &self.initial_ack_ref,
            &registration,
        )
        .map_err(|error| DbError::context("founder initial acknowledgement", error))?;
        if initial_ack != self.initial_ack.value
            || self.initial_ack_ref.registration != registration_ref
            || self.initial_ack_ref.sequence != 1
            || &self.initial_ack_ref.object != self.initial_ack.prepared.reference()
            || initial_ack.successor.predecessor.is_some()
            || initial_ack.registration != registration_ref
            || !initial_ack.store_cut.0.is_empty()
        {
            return Err(DbError::Message(
                "founder initial acknowledgement differs from its exact root graph".to_string(),
            ));
        }
        {
            let entry = &self.membership.entry;
            let entry_ref = &self.membership.entry_ref;
            let head = &self.membership.head;
            let head_ref = &self.membership.head_ref;
            let parsed_entry: MembershipEntry = serde_json::from_slice(&entry.bytes)
                .map_err(|error| DbError::context("founder membership entry", error))?;
            if parsed_entry != entry.value
                || root
                    .descriptor
                    .validate_merge_founder_entry(&parsed_entry)
                    .is_err()
                || entry_ref.coord != parsed_entry.coord()
                || &entry_ref.object != entry.prepared.reference()
            {
                return Err(DbError::Message(
                    "founder membership entry differs from its root or exact reference".to_string(),
                ));
            }
            let parsed_head: AuthorHead = serde_json::from_slice(&head.bytes)
                .map_err(|error| DbError::context("founder membership head", error))?;
            let anchor = parsed_entry.change.membership_anchor().ok_or_else(|| {
                DbError::Message("founder entry has no Store membership anchor".to_string())
            })?;
            let coven_protocol::store_commit::GrantStreamAnchor::StoreMembership { first_slot } =
                anchor
            else {
                return Err(DbError::Message(
                    "founder membership entry uses a recovery anchor".to_string(),
                ));
            };
            if parsed_head != head.value
                || !parsed_head.verify(&registration)
                || parsed_head.body.author_registration != registration_ref
                || parsed_head.body.entry != *entry_ref
                || parsed_head.body.predecessor.is_some()
                || parsed_head.entry_coord() != parsed_entry.coord()
                || head_ref.coord != parsed_entry.coord()
                || head_ref.head_hash != parsed_head.head_hash()
                || &head_ref.object != head.prepared.reference()
                || head.prepared.reference().slot() != &first_slot
                || parsed_head.body.successor.activation
                    != coven_protocol::store_commit::StreamActivation::grant_authorized(
                        root_ref.store_root_hash,
                        registration_ref.clone(),
                        parsed_entry.author_owner_grant.clone(),
                        coven_protocol::store_commit::GrantStreamAnchor::StoreMembership {
                            first_slot: first_slot.clone(),
                        },
                    )
                    .activation_id()
            {
                return Err(DbError::Message(
                    "founder membership head differs from its exact root graph".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DurableFounderMembership {
    pub entry: ExactProtocolObject<MembershipEntry>,
    pub entry_ref: MembershipEntryRef,
    pub head: ExactProtocolObject<AuthorHead>,
    pub head_ref: MembershipHeadRef,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFounderMembershipJournal {
    entry_ref: MembershipEntryRef,
    entry_bytes: Vec<u8>,
    entry_prepared: PreparedExactObject,
    head_ref: MembershipHeadRef,
    head_bytes: Vec<u8>,
    head_prepared: PreparedExactObject,
}

impl DurableFounderMembershipJournal {
    pub fn from_graph(graph: &DurableFounderMembership) -> Self {
        Self {
            entry_ref: graph.entry_ref.clone(),
            entry_bytes: graph.entry.bytes.clone(),
            entry_prepared: graph.entry.prepared.clone(),
            head_ref: graph.head_ref.clone(),
            head_bytes: graph.head.bytes.clone(),
            head_prepared: graph.head.prepared.clone(),
        }
    }

    pub fn into_graph(self) -> Result<DurableFounderMembership, DbError> {
        let entry_value: MembershipEntry = serde_json::from_slice(&self.entry_bytes)
            .map_err(|error| DbError::context("local founder membership entry", error))?;
        let head_value: AuthorHead = serde_json::from_slice(&self.head_bytes)
            .map_err(|error| DbError::context("local founder membership head", error))?;
        Ok(DurableFounderMembership {
            entry: ExactProtocolObject {
                value: entry_value,
                bytes: self.entry_bytes,
                prepared: self.entry_prepared,
            },
            entry_ref: self.entry_ref,
            head: ExactProtocolObject {
                value: head_value,
                bytes: self.head_bytes,
                prepared: self.head_prepared,
            },
            head_ref: self.head_ref,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FounderMembershipRefs {
    pub entry: MembershipEntryRef,
    pub head: MembershipHeadRef,
}

pub(crate) fn founder_graph_identity(graph: &DurableFounderGraph) -> ObjectHash {
    let membership = serde_json::to_vec(&(
        &graph.membership.entry_ref,
        &graph.membership.entry.bytes,
        &graph.membership.entry.prepared,
        &graph.membership.head_ref,
        &graph.membership.head.bytes,
        &graph.membership.head.prepared,
    ))
    .expect("founder membership graph serialization cannot fail");
    ObjectHash::digest(
        &serde_json::to_vec(&(
            &graph.root.bytes,
            &graph.root.prepared,
            &graph.registration.bytes,
            &graph.registration.prepared,
            &graph.initial_ack_ref,
            &graph.initial_ack.bytes,
            &graph.initial_ack.prepared,
            membership,
        ))
        .expect("founder graph serialization cannot fail"),
    )
}

pub(crate) fn load_store_root_authority_on(
    conn: &Connection,
) -> Result<
    Option<(
        coven_protocol::store_commit::StoreRootRef,
        StoreProtocolRoot,
    )>,
    DbError,
> {
    conn.query_row(
        "SELECT store_root_hash, store_protocol_root_bytes, store_root_object \
         FROM store_protocol_root_authority WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(hash, bytes, object)| {
        let value = StoreProtocolRoot::parse(&bytes)
            .map_err(|error| DbError::context("Store root authority bytes", error))?;
        let store_root_hash: ObjectHash = hash
            .parse()
            .map_err(|error| DbError::context("Store root authority semantic hash", error))?;
        let object: ExactObjectRef = serde_json::from_str(&object)
            .map_err(|error| DbError::context("Store root authority object", error))?;
        if value.object_hash() != store_root_hash {
            return Err(DbError::Message(
                "Store root authority hash differs from its signed bytes".to_string(),
            ));
        }
        Ok((
            coven_protocol::store_commit::StoreRootRef {
                store_root_id: value.descriptor.store_root_id(),
                store_root_hash,
                object,
            },
            value,
        ))
    })
    .transpose()
}

pub(crate) fn install_store_root_authority_on(
    conn: &Connection,
    reference: &coven_protocol::store_commit::StoreRootRef,
    bytes: &[u8],
) -> Result<StoreProtocolRoot, DbError> {
    let value = StoreProtocolRoot::parse(bytes)
        .map_err(|error| DbError::context("install Store root authority", error))?;
    if value.object_hash() != reference.store_root_hash {
        return Err(DbError::Message(
            "installed Store root reference differs from its signed bytes".to_string(),
        ));
    }
    let object = serde_json::to_string(&reference.object)
        .map_err(|error| DbError::context("serialize Store root authority", error))?;
    let existing = load_store_root_authority_on(conn)?;
    if let Some((existing_reference, existing_value)) = existing {
        if existing_reference == *reference && existing_value == value {
            return Ok(value);
        }
        return Err(DbError::Message(
            "database already trusts a different exact Store root".to_string(),
        ));
    }
    conn.execute(
        "INSERT INTO store_protocol_root_authority \
         (singleton, store_root_hash, store_protocol_root_bytes, store_root_object) \
         VALUES (1, ?1, ?2, ?3)",
        rusqlite::params![reference.store_root_hash.to_string(), bytes, object],
    )
    .map_err(DbError::from)?;
    Ok(value)
}

pub(crate) fn validate_replay_authority_on(
    conn: &Connection,
    baseline: &RetainedReplayBaseline,
) -> Result<(), DbError> {
    let (root_ref, root) = load_store_root_authority_on(conn)?.ok_or_else(|| {
        DbError::Message("retained replay image has no Store root authority".to_string())
    })?;
    let (authority_root, founder_registration) = match &baseline.authority {
        RetainedReplayAuthority::Genesis(authority) => {
            (&authority.store_root, &authority.founder_registration)
        }
        RetainedReplayAuthority::StableSnapshot(authority) => {
            authority.validate()?;
            (&authority.store_root, &authority.founder_registration)
        }
    };
    if &root_ref != authority_root || root.descriptor.sync_routing_hash != baseline.routing_hash {
        return Err(DbError::Message(
            "retained replay authority differs from its Store root".to_string(),
        ));
    }
    let founder = load_activated_registration_on(conn, &root_ref, founder_registration)?;
    let authority: String = conn
        .query_row(
            "SELECT activation_authority
             FROM store_device_registration_activations
             WHERE device_id = ?1 AND registration_hash = ?2",
            (
                founder_registration.device_id.to_string(),
                founder_registration.registration_hash.to_string(),
            ),
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation =
        serde_json::from_str(&authority).map_err(|error| {
            DbError::context("retained replay founder activation authority", error)
        })?;
    if founder.store_root != root_ref
        || authority
            != (coven_protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
                root: root_ref.clone(),
            })
    {
        return Err(DbError::Message(
            "retained replay founder differs from its exact activation".to_string(),
        ));
    }
    if let RetainedReplayAuthority::StableSnapshot(authority) = &baseline.authority {
        for registration in authority.active_registrations.values() {
            let installed =
                load_activated_registration_on(conn, &root_ref, registration.reference())?;
            if &installed != registration.value() {
                return Err(DbError::Message(
                    "retained snapshot active registration differs from its installed authority"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn install_store_founder_state_on(
    conn: &Connection,
    root: &coven_protocol::store_commit::StoreRootRef,
    founder_reference: &StoreDeviceRegistrationRef,
    founder: &StoreDeviceRegistration,
    founder_bytes: &[u8],
    genesis: &ResolvedStoreDeviceState,
) -> Result<(), DbError> {
    if founder.store_root != *root {
        return Err(DbError::Message(
            "Store founder registration belongs to another exact root".to_string(),
        ));
    }
    founder_reference
        .verify_registration(founder)
        .map_err(DbError::from)?;
    if founder.to_bytes() != founder_bytes {
        return Err(DbError::Message(
            "Store founder registration differs from its exact bytes".to_string(),
        ));
    }
    let founder_authority =
        coven_protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
            root: root.clone(),
        };
    let founder_values = (
        founder_reference.registration_hash.to_string(),
        founder.author_pubkey.clone(),
        founder.device_signing_pubkey.clone(),
        founder_bytes.to_vec(),
        serde_json::to_string(founder_reference)
            .map_err(|error| DbError::context("serialize Store founder registration ref", error))?,
        serde_json::to_string(&founder_authority)
            .map_err(|error| DbError::context("serialize Store founder activation", error))?,
    );
    conn.execute(
        "INSERT INTO store_device_registration_activations
         (device_id, registration_hash, author_pubkey, device_signing_pubkey,
          registration_bytes, registration_object, activation_authority)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(device_id) DO NOTHING",
        rusqlite::params![
            founder.device_id.to_string(),
            &founder_values.0,
            &founder_values.1,
            &founder_values.2,
            &founder_values.3,
            &founder_values.4,
            &founder_values.5,
        ],
    )
    .map_err(DbError::from)?;
    let stored_founder: (String, String, String, Vec<u8>, String, String) = conn
        .query_row(
            "SELECT registration_hash, author_pubkey, device_signing_pubkey,
                    registration_bytes, registration_object, activation_authority
             FROM store_device_registration_activations WHERE device_id = ?1",
            [founder.device_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(DbError::from)?;
    if stored_founder != founder_values {
        return Err(DbError::Message(
            "Store founder activation differs from installed exact authority".to_string(),
        ));
    }
    let genesis = serde_json::to_string(genesis)
        .map_err(|error| DbError::context("serialize Store device genesis", error))?;
    conn.execute(
        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
        (STORE_DEVICE_GENESIS_STATE_KEY, &genesis),
    )
    .map_err(DbError::from)?;
    let stored_genesis = required_protocol_state_on(conn, STORE_DEVICE_GENESIS_STATE_KEY)?;
    if stored_genesis != genesis {
        return Err(DbError::Message(
            "Store device genesis differs from installed exact authority".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn load_local_store_founder_graph_on(
    conn: &Connection,
) -> Result<Option<Box<DurableFounderGraph>>, DbError> {
    let owned_rows: i64 = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM local_store_protocol_root) \
                  + EXISTS(SELECT 1 FROM local_store_device_registration) \
                  + EXISTS(SELECT 1 FROM local_store_founder_graph)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if owned_rows == 0 {
        return Ok(None);
    }
    if owned_rows != 3 {
        return Err(DbError::Message(
            "local Store founder graph is only partially durable".to_string(),
        ));
    }
    let raw = conn
        .query_row(
            "SELECT r.store_root_hash, r.store_protocol_root_bytes, r.prepared_object, \
                    d.device_id, d.registration_hash, d.registration_bytes, d.prepared_object, \
                    d.initial_ack_ref, d.initial_ack_bytes, d.initial_ack_prepared, d.state, \
                    g.membership_graph \
             FROM local_store_protocol_root r \
             CROSS JOIN local_store_device_registration d \
             CROSS JOIN local_store_founder_graph g \
             WHERE r.singleton = 1 AND d.singleton = 1 AND g.singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .map_err(DbError::from)?;
    let (
        root_hash,
        root_bytes,
        root_prepared,
        device_id,
        registration_hash,
        registration_bytes,
        registration_prepared,
        initial_ack_ref,
        initial_ack_bytes,
        initial_ack_prepared,
        registration_state,
        membership_graph,
    ) = raw;
    let registration_state: LocalDeviceRegistrationState =
        serde_json::from_str(&registration_state)
            .map_err(|error| DbError::context("local registration journal state", error))?;
    let root_value = StoreProtocolRoot::parse(&root_bytes)
        .map_err(|error| DbError::context("local founder Store root", error))?;
    let root_prepared: PreparedExactObject = serde_json::from_str(&root_prepared)
        .map_err(|error| DbError::context("local founder Store root object", error))?;
    let store_root_hash: ObjectHash = root_hash
        .parse()
        .map_err(|error| DbError::context("local founder Store root hash", error))?;
    if store_root_hash != root_value.object_hash() {
        return Err(DbError::Message(
            "local founder Store root hash differs from its bytes".to_string(),
        ));
    }
    let root_ref = coven_protocol::store_commit::StoreRootRef {
        store_root_id: root_value.descriptor.store_root_id(),
        store_root_hash,
        object: root_prepared.reference().clone(),
    };
    let parsed_device_id = device_id
        .parse()
        .map_err(|error| DbError::context("local founder device id", error))?;
    let registration_value =
        StoreDeviceRegistration::parse_at(&registration_bytes, &root_ref, parsed_device_id)
            .map_err(|error| DbError::context("local founder Store registration", error))?;
    let parsed_registration_hash: ObjectHash = registration_hash
        .parse()
        .map_err(|error| DbError::context("local founder Store registration hash", error))?;
    if parsed_registration_hash != registration_value.registration_hash() {
        return Err(DbError::Message(
            "local founder registration hash differs from its bytes".to_string(),
        ));
    }
    let registration_prepared: PreparedExactObject =
        serde_json::from_str(&registration_prepared)
            .map_err(|error| DbError::context("local founder registration object", error))?;
    let initial_ack_ref: StoreAckRef = serde_json::from_str(&initial_ack_ref)
        .map_err(|error| DbError::context("local founder initial ack ref", error))?;
    let initial_ack_value = StoreAck::parse_at(
        &initial_ack_bytes,
        &root_ref,
        &initial_ack_ref,
        &registration_value,
    )
    .map_err(|error| DbError::context("local founder initial ack", error))?;
    let initial_ack_prepared: PreparedExactObject = serde_json::from_str(&initial_ack_prepared)
        .map_err(|error| DbError::context("local founder initial ack object", error))?;
    let membership = serde_json::from_str::<DurableFounderMembershipJournal>(&membership_graph)
        .map_err(|error| DbError::context("local founder membership graph", error))?
        .into_graph()?;
    let graph = DurableFounderGraph {
        root: ExactProtocolObject {
            value: root_value,
            bytes: root_bytes,
            prepared: root_prepared,
        },
        registration: ExactProtocolObject {
            value: registration_value,
            bytes: registration_bytes,
            prepared: registration_prepared,
        },
        initial_ack: ExactProtocolObject {
            value: initial_ack_value,
            bytes: initial_ack_bytes,
            prepared: initial_ack_prepared,
        },
        initial_ack_ref,
        membership,
        registration_state,
    };
    graph.validate()?;
    Ok(Some(Box::new(graph)))
}
