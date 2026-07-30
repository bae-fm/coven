use crate::database::database_open::load_coven_metadata;
use crate::database::database_open::validate_initialized_coven_schema;

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct DurableFounderGraph {
    pub root: ExactProtocolObject<StoreProtocolRoot>,
    pub registration: ExactProtocolObject<StoreDeviceRegistration>,
    pub initial_ack: ExactProtocolObject<StoreAck>,
    pub initial_ack_ref: StoreAckRef,
    pub membership: DurableFounderMembership,
    pub registration_state: LocalDeviceRegistrationState,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableFounderMembership {
    pub entry: ExactProtocolObject<MembershipEntry>,
    pub entry_ref: MembershipEntryRef,
    pub head: ExactProtocolObject<AuthorHead>,
    pub head_ref: MembershipHeadRef,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableFounderMembershipJournal {
    entry_ref: MembershipEntryRef,
    entry_bytes: Vec<u8>,
    entry_prepared: PreparedExactObject,
    head_ref: MembershipHeadRef,
    head_bytes: Vec<u8>,
    head_prepared: PreparedExactObject,
}

impl DurableFounderMembershipJournal {
    pub(crate) fn from_graph(graph: &DurableFounderMembership) -> Self {
        Self {
            entry_ref: graph.entry_ref.clone(),
            entry_bytes: graph.entry.bytes.clone(),
            entry_prepared: graph.entry.prepared.clone(),
            head_ref: graph.head_ref.clone(),
            head_bytes: graph.head.bytes.clone(),
            head_prepared: graph.head.prepared.clone(),
        }
    }

    pub(super) fn into_graph(self) -> Result<DurableFounderMembership, DbError> {
        let entry_value: MembershipEntry =
            serde_json::from_slice(&self.entry_bytes).map_err(|error| {
                DbError::Message(format!("local founder membership entry: {error}"))
            })?;
        let head_value: AuthorHead = serde_json::from_slice(&self.head_bytes)
            .map_err(|error| DbError::Message(format!("local founder membership head: {error}")))?;
        Ok(DurableFounderMembership {
            entry: ExactProtocolObject {
                value: entry_value,
                bytes: self.entry_bytes,
                object: self.entry_prepared.reference().clone(),
                prepared: self.entry_prepared,
            },
            entry_ref: self.entry_ref,
            head: ExactProtocolObject {
                value: head_value,
                bytes: self.head_bytes,
                object: self.head_prepared.reference().clone(),
                prepared: self.head_prepared,
            },
            head_ref: self.head_ref,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FounderMembershipRefs {
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
        crate::protocol::store_commit::StoreRootRef,
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
            .map_err(|error| DbError::Message(format!("Store root authority bytes: {error}")))?;
        let store_root_hash: ObjectHash = hash.parse().map_err(|error| {
            DbError::Message(format!("Store root authority semantic hash: {error}"))
        })?;
        let object: ExactObjectRef = serde_json::from_str(&object)
            .map_err(|error| DbError::Message(format!("Store root authority object: {error}")))?;
        if value.object_hash() != store_root_hash {
            return Err(DbError::Message(
                "Store root authority hash differs from its signed bytes".to_string(),
            ));
        }
        Ok((
            crate::protocol::store_commit::StoreRootRef {
                store_root_id: value.descriptor.store_root_id(),
                store_root_hash,
                object,
            },
            value,
        ))
    })
    .transpose()
}

pub(crate) fn required_store_root_authority_on(
    conn: &Connection,
) -> Result<crate::protocol::store_commit::StoreRootRef, DbError> {
    load_store_root_authority_on(conn)?
        .map(|(reference, _)| reference)
        .ok_or(DbError::StoreRootHashMissing)
}

pub(crate) fn install_store_root_authority_on(
    conn: &Connection,
    reference: &crate::protocol::store_commit::StoreRootRef,
    bytes: &[u8],
) -> Result<(), DbError> {
    let value = StoreProtocolRoot::parse(bytes)
        .map_err(|error| DbError::Message(format!("install Store root authority: {error}")))?;
    if value.object_hash() != reference.store_root_hash {
        return Err(DbError::Message(
            "installed Store root reference differs from its signed bytes".to_string(),
        ));
    }
    let object = serde_json::to_string(&reference.object)
        .map_err(|error| DbError::Message(format!("serialize Store root authority: {error}")))?;
    let existing = load_store_root_authority_on(conn)?;
    if let Some((existing_reference, existing_value)) = existing {
        if existing_reference == *reference && existing_value == value {
            return Ok(());
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
    .map(|_| ())
    .map_err(DbError::from)
}

pub(super) fn validate_replay_authority_on(
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
    let authority: crate::protocol::store_commit::StoreDeviceRegistrationActivation =
        serde_json::from_str(&authority).map_err(|error| {
            DbError::Message(format!(
                "retained replay founder activation authority: {error}"
            ))
        })?;
    if founder.store_root != root_ref
        || authority
            != (crate::protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
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
                load_activated_registration_on(conn, &root_ref, &registration.reference)?;
            if installed != registration.value {
                return Err(DbError::Message(
                    "retained snapshot active registration differs from its installed authority"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_generation_zero_replay_baseline_on(
    conn: &Connection,
    baseline: &RetainedReplayBaseline,
) -> Result<(), DbError> {
    baseline.validate_image()?;
    validate_replay_authority_on(conn, baseline)?;
    let image = baseline.open_image()?;
    let routing = load_coven_metadata(&image)?;
    if routing.hash() != baseline.routing_hash {
        return Err(DbError::Message(
            "retained replay image routing contract differs from its baseline".to_string(),
        ));
    }
    validate_initialized_coven_schema(&image, routing.has_scoped_graph())?;
    validate_replay_authority_on(&image, baseline)
}

struct StoredGenerationZeroReplayBaseline {
    generation: i64,
    exact_cut: String,
    schema_version: i64,
    routing_hash: String,
    image_hash: String,
    image_bytes: Vec<u8>,
    authority_bytes: Vec<u8>,
}

pub(crate) fn load_generation_zero_replay_baseline_on(
    conn: &Connection,
) -> Result<Option<RetainedReplayBaseline>, DbError> {
    let stored: Option<StoredGenerationZeroReplayBaseline> = conn
        .query_row(
            "SELECT generation, exact_cut, schema_version,
                    routing_hash, image_hash, image_bytes, authority_bytes
             FROM retained_replay_baselines WHERE singleton = 1",
            [],
            |row| {
                Ok(StoredGenerationZeroReplayBaseline {
                    generation: row.get(0)?,
                    exact_cut: row.get(1)?,
                    schema_version: row.get(2)?,
                    routing_hash: row.get(3)?,
                    image_hash: row.get(4)?,
                    image_bytes: row.get(5)?,
                    authority_bytes: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let generation = u64::try_from(stored.generation)
        .map_err(|_| DbError::Message("retained replay generation is negative".to_string()))?;
    let schema_version = u32::try_from(stored.schema_version)
        .map_err(|_| DbError::Message("retained replay schema version exceeds u32".to_string()))?;
    let parsed_exact_cut: CommitFrontier = serde_json::from_str(&stored.exact_cut)
        .map_err(|error| DbError::Message(format!("retained replay exact cut: {error}")))?;
    let authority: RetainedReplayAuthority = serde_json::from_slice(&stored.authority_bytes)
        .map_err(|error| DbError::Message(format!("retained replay authority: {error}")))?;
    if serde_json::to_string(&parsed_exact_cut).map_err(|error| {
        DbError::Message(format!("serialize retained replay exact cut: {error}"))
    })? != stored.exact_cut
        || serde_json::to_vec(&authority).map_err(|error| {
            DbError::Message(format!("serialize retained replay authority: {error}"))
        })? != stored.authority_bytes
    {
        return Err(DbError::Message(
            "retained replay baseline metadata is not canonical".to_string(),
        ));
    }
    let baseline =
        RetainedReplayBaseline {
            generation,
            exact_cut: parsed_exact_cut,
            schema_version,
            routing_hash: stored.routing_hash.parse().map_err(|error| {
                DbError::Message(format!("retained replay routing hash: {error}"))
            })?,
            image_hash: stored.image_hash.parse().map_err(|error| {
                DbError::Message(format!("retained replay image hash: {error}"))
            })?,
            image_bytes: stored.image_bytes,
            authority,
        };
    validate_generation_zero_replay_baseline_on(conn, &baseline)?;
    Ok(Some(baseline))
}

pub(crate) fn install_generation_zero_replay_baseline_on(
    conn: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
    authority: RetainedReplayGenesisAuthority,
) -> Result<(), DbError> {
    if load_generation_zero_replay_baseline_on(conn)?.is_some() {
        return Err(DbError::Message(
            "retained replay baseline already exists before founder activation".to_string(),
        ));
    }
    let baseline =
        RetainedReplayBaseline::generation_zero(conn, schema_version, routing_hash, authority)?;
    insert_retained_replay_baseline_on(conn, &baseline)
}

pub(crate) fn install_snapshot_replay_baseline_on(
    conn: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
    authority: RetainedReplaySnapshotAuthority,
) -> Result<(), DbError> {
    if load_generation_zero_replay_baseline_on(conn)?.is_some() {
        return Err(DbError::Message(
            "retained replay baseline already exists before snapshot bootstrap".to_string(),
        ));
    }
    let baseline =
        RetainedReplayBaseline::stable_snapshot(conn, schema_version, routing_hash, authority)?;
    insert_retained_replay_baseline_on(conn, &baseline)
}

pub(super) fn insert_retained_replay_baseline_on(
    conn: &Connection,
    baseline: &RetainedReplayBaseline,
) -> Result<(), DbError> {
    validate_replay_authority_on(conn, baseline)?;
    conn.execute(
        "INSERT INTO retained_replay_baselines
         (singleton, generation, exact_cut, schema_version,
          routing_hash, image_hash, image_bytes, authority_bytes)
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            i64::try_from(baseline.generation).map_err(|_| {
                DbError::Message("retained replay generation exceeds SQLite INTEGER".to_string())
            })?,
            serde_json::to_string(&baseline.exact_cut).map_err(|error| {
                DbError::Message(format!("serialize retained replay exact cut: {error}"))
            })?,
            i64::from(baseline.schema_version),
            baseline.routing_hash.to_string(),
            baseline.image_hash.to_string(),
            &baseline.image_bytes,
            baseline.canonical_authority_bytes()?,
        ],
    )
    .map_err(DbError::from)?;
    let installed = load_generation_zero_replay_baseline_on(conn)?.ok_or_else(|| {
        DbError::Message("installed retained replay baseline is absent".to_string())
    })?;
    if &installed != baseline {
        return Err(DbError::Message(
            "installed retained replay baseline differs from its verified image".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_founder_replay_baseline_on(
    conn: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
    authority: RetainedReplayGenesisAuthority,
) -> Result<(), DbError> {
    if let Some(existing) = load_generation_zero_replay_baseline_on(conn)? {
        let authority_matches = match &existing.authority {
            RetainedReplayAuthority::Genesis(existing) => existing == &authority,
            RetainedReplayAuthority::StableSnapshot(existing) => {
                existing.store_root == authority.store_root
                    && existing.founder_registration == authority.founder_registration
            }
        };
        if existing.schema_version != schema_version
            || existing.routing_hash != routing_hash
            || !authority_matches
        {
            return Err(DbError::Message(
                "retained replay baseline differs from the installed founder authority".to_string(),
            ));
        }
        return Ok(());
    }
    let accepted_history: i64 = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM materialized_commits)
                    + (SELECT COUNT(*) FROM snapshot_coverage)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if accepted_history != 0 {
        return Err(DbError::Message(
            "accepted Store history exists without a retained replay baseline".to_string(),
        ));
    }
    install_generation_zero_replay_baseline_on(conn, schema_version, routing_hash, authority)
}

pub(crate) fn install_store_founder_state_on(
    conn: &Connection,
    root: &crate::protocol::store_commit::StoreRootRef,
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
        .map_err(|error| DbError::Message(error.to_string()))?;
    if founder.to_bytes() != founder_bytes {
        return Err(DbError::Message(
            "Store founder registration differs from its exact bytes".to_string(),
        ));
    }
    let founder_authority =
        crate::protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
            root: root.clone(),
        };
    let founder_values = (
        founder_reference.registration_hash.to_string(),
        founder.author_pubkey.clone(),
        founder.device_signing_pubkey.clone(),
        founder_bytes.to_vec(),
        serde_json::to_string(founder_reference).map_err(|error| {
            DbError::Message(format!("serialize Store founder registration ref: {error}"))
        })?,
        serde_json::to_string(&founder_authority).map_err(|error| {
            DbError::Message(format!("serialize Store founder activation: {error}"))
        })?,
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
        .map_err(|error| DbError::Message(format!("serialize Store device genesis: {error}")))?;
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

pub(crate) fn validate_founder_graph(graph: &DurableFounderGraph) -> Result<(), DbError> {
    let root = StoreProtocolRoot::parse(&graph.root.bytes)
        .map_err(|error| DbError::Message(format!("founder Store root: {error}")))?;
    if root != graph.root.value
        || root.object_hash() != graph.root.value.object_hash()
        || graph.root.object != *graph.root.prepared.reference()
    {
        return Err(DbError::Message(
            "founder Store root differs from its prepared exact object".to_string(),
        ));
    }
    let root_ref = crate::protocol::store_commit::StoreRootRef {
        store_root_id: root.descriptor.store_root_id(),
        store_root_hash: root.object_hash(),
        object: graph.root.object.clone(),
    };
    let registration = StoreDeviceRegistration::parse_at(
        &graph.registration.bytes,
        &root_ref,
        graph.registration.value.device_id,
    )
    .map_err(|error| DbError::Message(format!("founder Store registration: {error}")))?;
    if registration != graph.registration.value
        || graph.registration.object != *graph.registration.prepared.reference()
        || registration.author_pubkey != root.descriptor.founder_pubkey
        || graph.registration.object.slot() != &root.descriptor.founder_registration
        || registration.provider != root.descriptor.founder_provider_admin.provider
        || !matches!(
            registration.origin,
            crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
        )
    {
        return Err(DbError::Message(
            "founder registration differs from its root or prepared exact object".to_string(),
        ));
    }
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &registration,
        graph.registration.object.clone(),
    );
    let initial_ack = StoreAck::parse_at(
        &graph.initial_ack.bytes,
        &root_ref,
        &graph.initial_ack_ref,
        &registration,
    )
    .map_err(|error| DbError::Message(format!("founder initial acknowledgement: {error}")))?;
    if initial_ack != graph.initial_ack.value
        || graph.initial_ack_ref.registration != registration_ref
        || graph.initial_ack_ref.sequence != 1
        || graph.initial_ack_ref.object != graph.initial_ack.object
        || graph.initial_ack.object != *graph.initial_ack.prepared.reference()
        || initial_ack.successor.predecessor.is_some()
        || initial_ack.registration != registration_ref
        || !initial_ack.store_cut.0.is_empty()
    {
        return Err(DbError::Message(
            "founder initial acknowledgement differs from its exact root graph".to_string(),
        ));
    }
    {
        let entry = &graph.membership.entry;
        let entry_ref = &graph.membership.entry_ref;
        let head = &graph.membership.head;
        let head_ref = &graph.membership.head_ref;
        let parsed_entry: MembershipEntry = serde_json::from_slice(&entry.bytes)
            .map_err(|error| DbError::Message(format!("founder membership entry: {error}")))?;
        if parsed_entry != entry.value
            || root
                .descriptor
                .validate_merge_founder_entry(&parsed_entry)
                .is_err()
            || entry_ref.coord != parsed_entry.coord()
            || entry_ref.object != entry.object
            || entry.object != *entry.prepared.reference()
        {
            return Err(DbError::Message(
                "founder membership entry differs from its root or exact reference".to_string(),
            ));
        }
        let parsed_head: AuthorHead = serde_json::from_slice(&head.bytes)
            .map_err(|error| DbError::Message(format!("founder membership head: {error}")))?;
        let anchor = parsed_entry.change.membership_anchor().ok_or_else(|| {
            DbError::Message("founder entry has no Store membership anchor".to_string())
        })?;
        let crate::protocol::store_commit::GrantStreamAnchor::StoreMembership { first_slot } =
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
            || head_ref.object != head.object
            || head.object != *head.prepared.reference()
            || head.object.slot() != &first_slot
            || parsed_head.body.successor.activation
                != crate::protocol::store_commit::StreamActivation::grant_authorized(
                    root_ref.store_root_hash,
                    registration_ref.clone(),
                    parsed_entry.author_owner_grant.clone(),
                    crate::protocol::store_commit::GrantStreamAnchor::StoreMembership {
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
        serde_json::from_str(&registration_state).map_err(|error| {
            DbError::Message(format!("local registration journal state: {error}"))
        })?;
    let root_value = StoreProtocolRoot::parse(&root_bytes)
        .map_err(|error| DbError::Message(format!("local founder Store root: {error}")))?;
    let root_prepared: PreparedExactObject = serde_json::from_str(&root_prepared)
        .map_err(|error| DbError::Message(format!("local founder Store root object: {error}")))?;
    let store_root_hash: ObjectHash = root_hash
        .parse()
        .map_err(|error| DbError::Message(format!("local founder Store root hash: {error}")))?;
    if store_root_hash != root_value.object_hash() {
        return Err(DbError::Message(
            "local founder Store root hash differs from its bytes".to_string(),
        ));
    }
    let root_ref = crate::protocol::store_commit::StoreRootRef {
        store_root_id: root_value.descriptor.store_root_id(),
        store_root_hash,
        object: root_prepared.reference().clone(),
    };
    let parsed_device_id = device_id
        .parse()
        .map_err(|error| DbError::Message(format!("local founder device id: {error}")))?;
    let registration_value =
        StoreDeviceRegistration::parse_at(&registration_bytes, &root_ref, parsed_device_id)
            .map_err(|error| {
                DbError::Message(format!("local founder Store registration: {error}"))
            })?;
    let parsed_registration_hash: ObjectHash = registration_hash.parse().map_err(|error| {
        DbError::Message(format!("local founder Store registration hash: {error}"))
    })?;
    if parsed_registration_hash != registration_value.registration_hash() {
        return Err(DbError::Message(
            "local founder registration hash differs from its bytes".to_string(),
        ));
    }
    let registration_prepared: PreparedExactObject = serde_json::from_str(&registration_prepared)
        .map_err(|error| {
        DbError::Message(format!("local founder registration object: {error}"))
    })?;
    let initial_ack_ref: StoreAckRef = serde_json::from_str(&initial_ack_ref)
        .map_err(|error| DbError::Message(format!("local founder initial ack ref: {error}")))?;
    let initial_ack_value = StoreAck::parse_at(
        &initial_ack_bytes,
        &root_ref,
        &initial_ack_ref,
        &registration_value,
    )
    .map_err(|error| DbError::Message(format!("local founder initial ack: {error}")))?;
    let initial_ack_prepared: PreparedExactObject = serde_json::from_str(&initial_ack_prepared)
        .map_err(|error| DbError::Message(format!("local founder initial ack object: {error}")))?;
    let membership = serde_json::from_str::<DurableFounderMembershipJournal>(&membership_graph)
        .map_err(|error| DbError::Message(format!("local founder membership graph: {error}")))?
        .into_graph()?;
    let graph = DurableFounderGraph {
        root: ExactProtocolObject {
            value: root_value,
            bytes: root_bytes,
            object: root_prepared.reference().clone(),
            prepared: root_prepared,
        },
        registration: ExactProtocolObject {
            value: registration_value,
            bytes: registration_bytes,
            object: registration_prepared.reference().clone(),
            prepared: registration_prepared,
        },
        initial_ack: ExactProtocolObject {
            value: initial_ack_value,
            bytes: initial_ack_bytes,
            object: initial_ack_prepared.reference().clone(),
            prepared: initial_ack_prepared,
        },
        initial_ack_ref,
        membership,
        registration_state,
    };
    validate_founder_graph(&graph)?;
    Ok(Some(Box::new(graph)))
}
