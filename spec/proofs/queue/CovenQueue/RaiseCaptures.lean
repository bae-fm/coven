import CovenQueue.RaiseProgress

/-! §17.1 with successive local breaking migrations. Each migration appends
its empty marker and captures its raise through it, replacing a capture an
earlier, still unpublished migration left (Rust `raise_capture::capture`).
App writes made between two such migrations are newer than the first
marker's version, and no raise to that version will be kept; they sit in
the later captured prefix, so the gate lets them upload before that raise.

`Local` holds the app's version, the version of the latest local migration
(`publication`, zero for none) and its marker's number (`through`). -/

namespace CovenQueue.RaiseUpload

structure Local where
  world : World
  app : Nat
  publication : Nat
  through : Nat

/-- An app write, at the app's version. -/
def Local.appWrite (l : Local) (r : Record) : Local :=
  ⟨{ l.world with queue := l.world.queue.commit { r with version := l.app, disposition := .ordinary } },
    l.app, l.publication, l.through⟩

/-- A breaking migration: its marker, and a capture through it that replaces
any earlier one. -/
def Local.migrated (l : Local) (version : Nat) (id : Identity) : Local :=
  ⟨{ l.world with queue := RaiseUpload.migrate l.world.queue version id },
    version, version, l.world.queue.reserved + 1⟩

/-- App writes take the app's version. A migration to a newer version
appends its marker and captures through it. Every other queue event (key
choice, sends, landings, confirmations, crashes) reserves no number. -/
inductive LocalStep (encode : Seal) : Local → Local → Prop
  | write (l : Local) (r : Record) : LocalStep encode l (l.appWrite r)
  | migrate (l : Local) (version : Nat) (id : Identity) : l.app < version →
      LocalStep encode l (l.migrated version id)
  | queue (l : Local) (next : World) : CovenQueue.Step encode l.world next →
      next.queue.reserved = l.world.queue.reserved →
      LocalStep encode l { l with world := next }

/-- An installation with an empty queue and no local migration yet. -/
inductive LocalReach (encode : Seal) (device app : Nat) : Local → Prop
  | start : LocalReach encode device app ⟨.empty device, app, 0, 0⟩
  | next {l l'} : LocalReach encode device app l → LocalStep encode l l' →
      LocalReach encode device app l'

def records (q : Queue) : List Record := q.waiting.map Pending.record

/-- Every waiting record is no newer than the app; each one through the
latest marker is a marker or older than that marker's version. -/
structure LocalInv (l : Local) : Prop where
  valid : l.world.queue.Valid
  versions : ∀ r ∈ records l.world.queue, r.version ≤ l.app
  captured : ∀ r ∈ records l.world.queue, r.identity.id.number ≤ l.through →
    r.disposition = .migration ∨ r.version < l.publication
  within : l.through ≤ l.world.queue.reserved

theorem commit_records (q : Queue) (r : Record) :
    records (q.commit r) = records q ++
      [{ r with identity := { r.identity with id := ⟨q.device, q.reserved + 1⟩ } }] := by
  simp [records, Queue.commit, Pending.record]

theorem prepare_records (q : Queue) (f : Nat) (ks : List Nat) :
    records (q.prepare f ks) = records q := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => rfl
    | cons p rest => cases p <;> rfl

theorem finish_records (q : Queue) : records q.finish = (records q).drop
    (q.finish.settled - q.settled) := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => simp [records, Queue.finish]
    | cons p rest =>
      cases p with
      | untried _ => simp [records, Queue.finish]
      | tried _ => simp [records, Queue.finish]

/-- A queue event that reserves no number keeps the waiting records,
dropping only those it confirmed; it never changes or adds one. -/
theorem step_records {encode : Seal} {s t : World} (step : CovenQueue.Step encode s t)
    (same : t.queue.reserved = s.queue.reserved) :
    records t.queue = (records s.queue).drop (t.queue.settled - s.queue.settled) := by
  cases step with
  | beginWrite => simp [World.beginWrite]
  | crash => simp [World.crash]
  | receive | wait => simp
  | commit => simp [World.commitWrite, Queue.commit, Queue.reserved] at same
  | prepare =>
    rw [prepare_records, (prepare_counters _ _ _).1]; simp
  | send => unfold World.send; split <;> simp
  | land => simp only [World.land]; split <;> simp
  | acknowledge => exact finish_records _

theorem start_inv (device app : Nat) : LocalInv ⟨.empty device, app, 0, 0⟩ :=
  ⟨.nil _, by simp [records, World.empty], by simp [records, World.empty],
    Nat.zero_le _⟩

theorem local_step_inv {encode : Seal} {l l' : Local} (h : LocalInv l)
    (step : LocalStep encode l l') : LocalInv l' := by
  cases step with
  | write r =>
    refine ⟨commit_valid _ h.valid _, ?_, ?_, ?_⟩
    · intro x member
      change x ∈ records (l.world.queue.commit _) at member
      rw [commit_records] at member
      rcases List.mem_append.mp member with old | new
      · exact h.versions x old
      · simp at new; subst new; exact Nat.le_refl _
    · intro x member number
      change x ∈ records (l.world.queue.commit _) at member
      rw [commit_records] at member
      rcases List.mem_append.mp member with old | new
      · exact h.captured x old number
      · simp at new; subst new
        have := h.within
        simp [Local.appWrite] at number; omega
    · have := h.within
      simp [Local.appWrite, Queue.commit, Queue.reserved] at this ⊢; omega
  | migrate version id newer =>
    refine ⟨commit_valid _ h.valid _, ?_, ?_, ?_⟩
    · intro x member
      change x ∈ records (l.world.queue.commit _) at member
      rw [commit_records] at member
      rcases List.mem_append.mp member with old | new
      · exact Nat.le_trans (h.versions x old) (Nat.le_of_lt newer)
      · simp at new; subst new; exact Nat.le_refl _
    · intro x member _
      change x ∈ records (l.world.queue.commit _) at member
      rw [commit_records] at member
      rcases List.mem_append.mp member with old | new
      · exact .inr (Nat.lt_of_le_of_lt (h.versions x old) newer)
      · simp at new; subst new; exact .inl rfl
    · simp [Local.migrated, RaiseUpload.migrate, Queue.commit, Queue.reserved]; omega
  | queue next step same =>
    have kept := step_records step same
    refine ⟨step_queue_valid step h.valid, ?_, ?_, ?_⟩
    · intro x member
      rw [kept] at member
      exact h.versions x (List.mem_of_mem_drop member)
    · intro x member number
      rw [kept] at member
      exact h.captured x (List.mem_of_mem_drop member) number
    · rw [same]; exact h.within

theorem reach_inv {encode : Seal} {device app : Nat} {l : Local}
    (reach : LocalReach encode device app l) : LocalInv l := by
  induction reach with
  | start => exact start_inv device app
  | next _ step ih => exact local_step_inv ih step

/-- Every local event keeps the queued records byte for byte: writes and
migrations append, other events drop only the confirmed front. -/
theorem local_step_keeps_records {encode : Seal} {l l' : Local}
    (step : LocalStep encode l l') :
    ∃ k, l'.world.queue.settled = l.world.queue.settled + k ∧
      (records l.world.queue).drop k <+: records l'.world.queue := by
  cases step with
  | write r =>
    refine ⟨0, rfl, ?_⟩
    change _ <+: records (l.world.queue.commit _)
    rw [commit_records, List.drop_zero]; exact List.prefix_append _ _
  | migrate version id _ =>
    refine ⟨0, rfl, ?_⟩
    change _ <+: records (l.world.queue.commit _)
    rw [commit_records, List.drop_zero]; exact List.prefix_append _ _
  | queue next step same =>
    refine ⟨next.queue.settled - l.world.queue.settled, by
      have := (step_monotone step).1; show next.queue.settled = _; omega, ?_⟩
    rw [step_records step same]; exact List.prefix_refl _

theorem numbered_take_lt {d n : Nat} {ps : List Pending} (h : Numbered d n ps) (k : Nat) :
    ∀ p ∈ ps.take k, p.record.identity.id.number < n + k := by
  induction h generalizing k with
  | nil => simp
  | @cons next p rest eq _ ih =>
    intro x member
    cases k with
    | zero => simp at member
    | succ k =>
      rcases List.mem_cons.mp (by simpa using member) with rfl | tail
      · simp only [eq]; omega
      · have := ih k x tail; omega

/-- In every history of writes, successive local migrations and queue
events, the latest capture publishes: its whole own prefix, including writes
made between earlier unpublished migrations, passes the gate with the store
still older, and the raise follows its confirmation. Keys, identity and
provider success are supplied, as in `no_raise_deadlock`. -/
theorem later_capture_never_deadlocks (encode : Seal) (published : WriteId → Prop)
    {device app0 : Nat} {l : Local} (reach : LocalReach encode device app0 l)
    (plan : Plan) (store format : Nat) (requests : List KeyRequest) (keys : List Key)
    (updated : store ≤ l.app) (chosen : chooseKeys requests = some keys)
    (header : ∃ h ps, requests = h :: ps ∧ h.audience = 0 ∧ h.currentMember = true)
    (identity : IdentityAvailable)
    (slots : Compatible encode format (keys.map Key.id) l.world)
    (cut : plan.through = l.through)
    (coverage : ∀ id ∈ plan.covers, if id.device = l.world.queue.device then
      0 < id.number ∧ id.number ≤ plan.through else published id) :
    ∃ t, Run encode published plan l.world ⟨t, .entryStored⟩ ∧
      plan.through ≤ t.queue.settled := by
  have inv := reach_inv reach
  by_cases behind : l.world.queue.settled ≤ l.through
  · let k := l.through - l.world.queue.settled
    let front := l.world.queue.waiting.take k
    have length : front.length = k := by
      have := inv.within
      simp only [front, List.length_take, Queue.reserved] at this ⊢
      omega
    have enabled : ∀ p ∈ front, Uploadable store l.publication p.record := by
      intro p member
      have number := numbered_take_lt inv.valid k p member
      have mem : p.record ∈ records l.world.queue :=
        List.mem_map_of_mem (List.mem_of_mem_take member)
      rcases inv.captured p.record mem (by omega) with marker | older
      · exact .inl marker
      · exact .inr (.inr (.inl older))
    obtain ⟨t, run, settled, _⟩ := no_raise_deadlock encode published plan l.world l.app store
      l.publication format requests keys front (l.world.queue.waiting.drop k) inv.valid
      (List.take_append_drop _ _).symm updated enabled chosen header identity slots
      (by rw [cut, length]; omega) coverage
    exact ⟨t, run, by omega⟩
  · have confirmed : Confirmed published plan l.world :=
      ⟨by omega, coverage⟩
    exact ⟨l.world, .step (.step (.step (.step .initial
      (.requestSnapshot _ rfl confirmed)) (.landSnapshot _ rfl)) (.requestEntry _ rfl))
        (.landEntry _ rfl), by omega⟩

/-- An app write at the latest migration's version still waits for a kept
raise to it: the later capture admits only what it covers. -/
theorem after_latest_marker_waits (l : Local) (r : Record) (kept : Nat)
    (ordinary : r.disposition = .ordinary) (current : r.version = l.publication)
    (migrated : 0 < l.publication) (enabled : Uploadable kept l.publication r) :
    l.publication ≤ kept := current ▸ new_schema_requires_kept r kept l.publication
      ordinary current migrated enabled

end CovenQueue.RaiseUpload
