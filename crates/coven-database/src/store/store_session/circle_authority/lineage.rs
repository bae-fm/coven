//! How one Circle control covers another.
//!
//! A control names the predecessors it observed, so the retained controls of a
//! Circle form a lineage. These reads walk it: whether one control covers
//! another, which control no other covers, and what current state a batch of
//! prepared controls reduces the installed one to.

use super::activation::verified_circle_activation_with_prefix_on;
use super::circle_activation_commit_ref_on;
use crate::store::store_session::verified_store_authority::VerifiedStoreLookup;
use crate::store::store_session::{StoreRecords, StoreSession};
use crate::*;
use coven_protocol::store_commit::StoreBatchCommitRef;

impl StoreSession<'_> {
    fn circle_control_covers_strictly(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let Some(covering_reference) = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            covering,
        )?
        else {
            return Ok(false);
        };
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &covering_reference.control,
            covered,
        )
    }

    pub(super) fn circle_current_state_with_activations(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
        use coven_protocol::circle_activation::CircleCurrentState;

        let records = StoreRecords::new(self.conn, self.store_dir);
        let mut pending = std::collections::BTreeMap::new();
        for activation in activations
            .iter()
            .filter(|activation| activation.circle_id == circle_id)
        {
            if activation.control.value.store_root_hash != root.store_root_hash
                || activation.reference.circle_id() != circle_id
                || activation.reference.control() != &activation.control.coord
            {
                return Err(DbError::Message(
                    "prepared Circle activation differs from its Store or control reference"
                        .to_string(),
                ));
            }
            let next = CircleCurrentState::from_verified_reference(activation)?;
            let coordinate = activation.control.coord.clone();
            if let Some((prior, _)) = pending.insert(coordinate.clone(), (activation, next)) {
                if prior != activation {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} prepared history has conflicting copies of control {coordinate:?}"
                    )));
                }
            }
        }
        let dependencies = pending
            .iter()
            .map(|(coordinate, (activation, _))| {
                let dependencies = activation
                    .control
                    .value
                    .covered_controls()
                    .iter()
                    .map(|covered| &covered.coord)
                    .filter(|coordinate| pending.contains_key(*coordinate))
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                (coordinate.clone(), dependencies)
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut applied = std::collections::BTreeSet::new();
        let mut state = crate::store::store_session::circle_operations::circle_current_state_on(
            self.conn, circle_id,
        )?;
        while !pending.is_empty() {
            let coordinate = coven_protocol::causal_grants::canonical_ready_node(
                pending
                    .keys()
                    .map(|coordinate| (coordinate, &dependencies[coordinate])),
                &applied,
            )
            .ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} prepared controls contain a causal cycle"
                ))
            })?;
            let (activation, next) = pending.remove(&coordinate).ok_or_else(|| {
                DbError::Message("ready prepared Circle control is absent".to_string())
            })?;
            applied.insert(coordinate);
            let Some(current) = state.take() else {
                if !activation.control.value.is_founder() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} current state is absent for a prepared successor"
                    )));
                }
                state = Some(next);
                continue;
            };
            if current
                .resolved_control()
                .is_some_and(|head| head.coordinate() == &activation.control.coord)
            {
                // Snapshot recipient access can enrich the installed public
                // control without publishing that control a second time.
                state = Some(if activation.local_access.is_some() {
                    next
                } else {
                    current
                });
                continue;
            }
            let heads = match &current {
                CircleCurrentState::ControlConflict { branches } => branches
                    .iter()
                    .map(|branch| branch.coordinate())
                    .collect::<Vec<_>>(),
                _ => vec![current
                    .resolved_control()
                    .ok_or_else(|| {
                        DbError::Message("resolved Circle control is absent".to_string())
                    })?
                    .coordinate()],
            };
            let mut already_covered = false;
            for head in heads {
                let covering = verified_circle_activation_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    circle_id,
                    head,
                    activations,
                )?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} current control has no verified activation"
                    ))
                })?;
                if verified_circle_control_covers_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    circle_id,
                    &covering.control,
                    &activation.control.coord,
                    activations,
                )? {
                    already_covered = true;
                    break;
                }
            }
            state = Some(if already_covered {
                current
            } else {
                current.advance(next)?
            });
        }
        Ok(state)
    }

    fn circle_restore_head(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: &[coven_protocol::circle::CircleControlCoord],
    ) -> Result<
        Option<(
            coven_protocol::circle::CircleControlCoord,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        let Some(head) = StoreDatabase::head_circle_control_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            controls,
        )?
        else {
            return Ok(None);
        };
        let commit =
            circle_activation_commit_ref_on(self.conn, circle_id, &head)?.ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} head control has no activating commit"
                ))
            })?;
        Ok(Some((head, commit)))
    }

    fn verified_circle_control_coord_covers(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let Some(reference) = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            covering,
        )?
        else {
            return Ok(false);
        };
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &reference.control,
            covered,
        )
    }

    fn verified_circle_control_covers(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: &coven_protocol::circle::PreparedCircleControl,
        prior: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            current,
            prior,
        )
    }
}

impl StoreDatabase {
    /// Whether one activated Circle control strictly covers another in the retained
    /// control lineage — `covering` is a proper successor of `covered`. Bootstrap
    /// reclamation uses this to prove a removed recipient lost authority under a
    /// successor control that supersedes its seed's control. `false` when the
    /// controls are equal or `covering` is not retained.
    pub async fn circle_control_covers_strictly(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        if covering == covered {
            return Ok(false);
        }
        let covering = covering.clone();
        let covered = covered.clone();
        self.call_store(move |session| {
            session.circle_control_covers_strictly(&root, circle_id, &covering, &covered)
        })
        .await
    }

    pub async fn circle_restore_head(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: Vec<coven_protocol::circle::CircleControlCoord>,
    ) -> Result<
        Option<(
            coven_protocol::circle::CircleControlCoord,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.call_store(move |session| session.circle_restore_head(&root, circle_id, &controls))
            .await
    }

    pub async fn verified_circle_control_coord_covers(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: coven_protocol::circle::CircleControlCoord,
        covered: coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.verified_circle_control_coord_covers(&root, circle_id, &covering, &covered)
        })
        .await
    }

    pub async fn verified_circle_control_covers(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: coven_protocol::circle::PreparedCircleControl,
        prior: coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.verified_circle_control_covers(&root, circle_id, &current, &prior)
        })
        .await
    }

    /// The head control of a Circle: the retained control whose lineage no other
    /// retained control covers. Restore resolves the restoring identity's current
    /// access at the head control's activating commit, so a member removed by a
    /// later epoch close resolves against the successor control that excludes them
    /// — never against a stale predecessor that still lists them active. A Circle
    /// with two uncovered controls is a forked lineage and fails loud.
    pub(super) fn head_circle_control_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: &[coven_protocol::circle::CircleControlCoord],
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
        // A control whose activating commit was reclaimed is superseded by a later
        // epoch and cannot be head; keep only controls whose commit is retained.
        let mut retained: Vec<(
            coven_protocol::circle::CircleControlCoord,
            coven_protocol::circle::PreparedCircleControl,
        )> = Vec::new();
        for coord in controls {
            let Some(activation_commit) =
                records.retained_circle_activation_commit_ref(circle_id, coord)?
            else {
                continue;
            };
            let materialization =
                authority.retained_materialization_by_ref_on(records, &activation_commit)?;
            if materialization.root() != root {
                return Err(DbError::Message(
                    "Circle activation belongs to another Store root".to_string(),
                ));
            }
            let reference = materialization.circle_activation(circle_id, coord)?;
            retained.push((coord.clone(), reference.control));
        }
        let mut head: Option<coven_protocol::circle::CircleControlCoord> = None;
        for (index, (candidate, _)) in retained.iter().enumerate() {
            let mut covered = false;
            for (other_index, (_, other_control)) in retained.iter().enumerate() {
                if other_index == index {
                    continue;
                }
                if Self::verified_circle_control_covers_on(
                    records,
                    authority,
                    root,
                    circle_id,
                    other_control,
                    candidate,
                )? {
                    covered = true;
                    break;
                }
            }
            if !covered {
                if head.is_some() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} has multiple head controls"
                    )));
                }
                head = Some(candidate.clone());
            }
        }
        Ok(head)
    }
}

/// Whether `current` covers `prior` by walking the observed predecessor edges,
/// preferring activations the caller prepared but has not installed yet.
pub(super) fn verified_circle_control_covers_with_prefix_on(
    records: StoreRecords<'_>,
    authority: &mut dyn VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    current: &coven_protocol::circle::PreparedCircleControl,
    prior: &coven_protocol::circle::CircleControlCoord,
    activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
) -> Result<bool, DbError> {
    if current.value.circle_id != circle_id {
        return Err(DbError::Message(
            "Circle control lineage starts outside its Circle".to_string(),
        ));
    }
    if current.coord == *prior {
        return Ok(true);
    }
    let mut pending = current
        .value
        .covered_controls()
        .iter()
        .map(|covered| (current.clone(), covered.coord.clone()))
        .collect::<Vec<_>>();
    let mut visited = std::collections::BTreeSet::new();
    while let Some((successor, coordinate)) = pending.pop() {
        if !visited.insert(coordinate.clone()) {
            continue;
        }
        let predecessor = verified_circle_activation_with_prefix_on(
            records,
            authority,
            root,
            circle_id,
            &coordinate,
            activations,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} control lineage omits retained control {coordinate:?}"
            ))
        })?;
        if !successor.value.causally_covers(&predecessor.control.value) {
            return Err(DbError::Message(format!(
                "Circle {circle_id} control lineage contains a non-causal edge"
            )));
        }
        if predecessor.control.coord == *prior {
            return Ok(true);
        }
        pending.extend(
            predecessor
                .control
                .value
                .covered_controls()
                .iter()
                .map(|covered| (predecessor.control.clone(), covered.coord.clone())),
        );
    }
    Ok(false)
}
