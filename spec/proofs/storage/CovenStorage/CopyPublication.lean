import CovenStorage.Objects

/-! §4's copy publication: a multipart upload assembles its bytes at a
staging temporary named by the session, outside D10's layout; one copy,
conditioned on an absent destination, publishes it; the temporary is then
deleted. The copy is atomic and conditional: that is the provider property
certification's races test, assumed here. Each published object keeps the
session marker the copy carried from its temporary. -/
namespace CovenStorage.CopyPublication

structure Published where
  object : Object
  session : Nat
  deriving DecidableEq, Repr

/-- Staging temporaries are keyed by session, not by `Path`: no D10 path
names one, so no published read, status or listing reaches it. -/
structure Bucket where
  published : Path → Option Published
  staging : Nat → Option Bytes

def empty : Bucket := ⟨fun _ => none, fun _ => none⟩

/-- What a published read sees. -/
def Bucket.objects (b : Bucket) : Storage := fun p => (b.published p).map (·.object)

inductive Outcome where
  | copied
  /-- The destination was occupied: it refuses the condition, unchanged. -/
  | refused
  /-- No temporary for this session. -/
  | missing
  deriving DecidableEq, Repr

/-- Assemble a session's temporary: unconditional, at its own name. -/
def stage (b : Bucket) (session : Nat) (bytes : Bytes) : Bucket :=
  { b with staging := fun s => if s = session then some bytes else b.staging s }

def discard (b : Bucket) (session : Nat) : Bucket :=
  { b with staging := fun s => if s = session then none else b.staging s }

/-- One atomic conditional copy. Its publication time is the copy's own. -/
def copy (b : Bucket) (session : Nat) (dest : Path) (time : Nat) : Bucket × Outcome :=
  match b.staging session with
  | none => (b, .missing)
  | some bytes =>
      match b.published dest with
      | some _ => (b, .refused)
      | none =>
          ({ b with published := fun p =>
              if p = dest then some ⟨⟨bytes, time⟩, session⟩ else b.published p }, .copied)

/-- The adapter's publication: copy, then delete the temporary after a
success or a definitive refusal. -/
def publish (b : Bucket) (session : Nat) (dest : Path) (time : Nat) : Bucket × Outcome :=
  let (after, outcome) := copy b session dest time
  (discard after session, outcome)

/-- Settle a copy whose reply was lost or refused by reading the destination:
this session's marker means its copy landed (§4). -/
def confirm (b : Bucket) (session : Nat) (dest : Path) : Settlement :=
  match b.published dest with
  | none => .absent
  | some p => if p.session = session then .stored else .reset

theorem stage_hidden (b : Bucket) (session : Nat) (bytes : Bytes) :
    (stage b session bytes).published = b.published := rfl

theorem discard_hidden (b : Bucket) (session : Nat) :
    (discard b session).published = b.published := rfl

/-- A copy publishes exactly as create-once does, so every create-once
result (`create_preserves`, `stored_history_preserved`) applies to it. -/
theorem copy_is_create (b : Bucket) (session : Nat) (dest : Path) (time : Nat) (bytes : Bytes)
    (staged : b.staging session = some bytes) :
    (copy b session dest time).1.objects = create b.objects dest bytes time := by
  funext p
  cases hd : b.published dest with
  | some o =>
      have occupied : b.objects dest = some o.object := by simp [Bucket.objects, hd]
      simp only [copy, staged, hd]
      by_cases same : p = dest
      · subst p; simp [create, occupied]
      · simp [create, same]
  | none =>
      have free : b.objects dest = none := by simp [Bucket.objects, hd]
      by_cases same : p = dest
      · subst p; simp [copy, staged, hd, create, Bucket.objects]
      · simp [copy, staged, hd, create, same, Bucket.objects]

theorem copy_outcome (b : Bucket) (session : Nat) (dest : Path) (time : Nat) (bytes : Bytes)
    (staged : b.staging session = some bytes) :
    (copy b session dest time).2 = if (b.published dest).isSome then .refused else .copied := by
  cases hd : b.published dest <;> simp [copy, staged, hd]

/-- A published object never changes: staging, deleting temporaries and
further copies leave it, marker included. -/
theorem publish_preserves (b : Bucket) (session : Nat) (dest p : Path) (time : Nat)
    (x : Published) (held : b.published p = some x) :
    (publish b session dest time).1.published p = some x := by
  simp only [publish, discard_hidden, copy]
  cases hs : b.staging session with
  | none => simpa using held
  | some bytes =>
      cases hd : b.published dest with
      | some _ => simpa using held
      | none =>
          have : p ≠ dest := by intro eq; subst eq; simp [hd] at held
          simp [this, held]

inductive Op where
  | stage (session : Nat) (bytes : Bytes)
  | publish (session : Nat) (dest : Path) (time : Nat)
  deriving DecidableEq, Repr

def Op.apply (b : Bucket) : Op → Bucket × Option Outcome
  | .stage s bytes => (CopyPublication.stage b s bytes, none)
  | .publish s dest t =>
      let (after, outcome) := CopyPublication.publish b s dest t
      (after, some outcome)

/-- Run requests in their arrival order at the provider. -/
def run (b : Bucket) : List Op → Bucket × List Outcome
  | [] => (b, [])
  | op :: ops =>
      let (next, outcome) := op.apply b
      let (last, outcomes) := run next ops
      (last, outcome.toList ++ outcomes)

theorem run_preserves (b : Bucket) (ops : List Op) (p : Path) (x : Published)
    (held : b.published p = some x) : (run b ops).1.published p = some x := by
  induction ops generalizing b with
  | nil => exact held
  | cons op ops ih =>
      cases op with
      | stage s bytes => exact ih _ (by simpa [Op.apply, stage_hidden] using held)
      | publish s dest t => exact ih _ (publish_preserves b s dest p t x held)

/-- Racing sessions, each already assembled, publish one name. -/
def race (b : Bucket) (dest : Path) (time : Nat) : List (Nat × Bytes) → Bucket × List Outcome
  | [] => (b, [])
  | (s, _) :: rest =>
      let (next, outcome) := publish b s dest time
      let (last, outcomes) := race next dest time rest
      (last, outcome :: outcomes)

theorem publish_staging (b : Bucket) (s t : Nat) (dest : Path) (time : Nat) :
    (publish b s dest time).1.staging t = if t = s then none else b.staging t := by
  simp only [publish, discard, copy]
  by_cases same : t = s
  · simp [same]
  · simp only [same, ite_false]
    cases b.staging s with
    | none => rfl
    | some _ => cases b.published dest <;> rfl

theorem race_cons (b : Bucket) (dest : Path) (time : Nat) (y : Nat × Bytes)
    (ys : List (Nat × Bytes)) :
    race b dest time (y :: ys) =
      ((race (publish b y.1 dest time).1 dest time ys).1,
        (publish b y.1 dest time).2 :: (race (publish b y.1 dest time).1 dest time ys).2) := rfl

/-- A deleted temporary stays deleted while no later racer has its session. -/
theorem race_keeps_gone (c : Bucket) (dest : Path) (time s : Nat) (zs : List (Nat × Bytes))
    (absent : s ∉ zs.map (·.1)) (gone : c.staging s = none) :
    (race c dest time zs).1.staging s = none := by
  induction zs generalizing c with
  | nil => exact gone
  | cons w ws ih =>
      simp only [List.map_cons, List.mem_cons, not_or] at absent
      rw [race_cons]
      exact ih _ absent.2 (by rw [publish_staging]; simp [absent.1, gone])

/-- With the destination occupied, every racing copy refuses it, leaves it
unchanged and deletes its own temporary. -/
theorem race_occupied (b : Bucket) (dest : Path) (time : Nat) (ss : List (Nat × Bytes))
    (x : Published) (held : b.published dest = some x)
    (staged : ∀ y ∈ ss, b.staging y.1 = some y.2) (distinct : (ss.map (·.1)).Nodup) :
    (race b dest time ss).2 = ss.map (fun _ => .refused) ∧
      (race b dest time ss).1.published dest = some x ∧
      ∀ y ∈ ss, (race b dest time ss).1.staging y.1 = none := by
  induction ss generalizing b with
  | nil => exact ⟨rfl, held, by simp⟩
  | cons y ys ih =>
      simp only [List.map_cons, List.nodup_cons, List.mem_map, not_exists, not_and] at distinct
      have outcome : (publish b y.1 dest time).2 = .refused := by
        simp [publish, copy, staged y (by simp), held]
      have rest := ih (publish b y.1 dest time).1
        (publish_preserves b y.1 dest dest time x held)
        (fun z hz => by
          rw [publish_staging]
          simp only [fun eq => distinct.1 z hz eq, ite_false]
          exact staged z (by simp [hz]))
        distinct.2
      rw [race_cons]
      refine ⟨by simp [outcome, rest.1], rest.2.1, ?_⟩
      intro z hz
      rcases List.mem_cons.mp hz with rfl | hz
      · exact race_keeps_gone _ dest time _ ys
          (fun hm => by obtain ⟨w, hw, eq⟩ := List.mem_map.mp hm; exact distinct.1 w hw eq)
          (by rw [publish_staging]; simp)
      · exact rest.2.2 z hz

/-- Exactly one winner: from a free destination, the first copy to arrive
publishes its own temporary's bytes, dated by that copy, with its session's
marker; every other racing copy refuses the condition. No temporary is left. -/
theorem race_one_winner (b : Bucket) (dest : Path) (time : Nat) (s : Nat) (bytes : Bytes)
    (rest : List (Nat × Bytes)) (free : b.published dest = none)
    (staged : ∀ y ∈ (s, bytes) :: rest, b.staging y.1 = some y.2)
    (distinct : (((s, bytes) :: rest).map (·.1)).Nodup) :
    (race b dest time ((s, bytes) :: rest)).2 = .copied :: rest.map (fun _ => .refused) ∧
      (race b dest time ((s, bytes) :: rest)).1.published dest = some ⟨⟨bytes, time⟩, s⟩ ∧
      ∀ y ∈ (s, bytes) :: rest, (race b dest time ((s, bytes) :: rest)).1.staging y.1 = none := by
  have mine : b.staging s = some bytes := staged (s, bytes) (by simp)
  simp only [List.map_cons, List.nodup_cons, List.mem_map, not_exists, not_and] at distinct
  have won : (publish b s dest time).1.published dest = some ⟨⟨bytes, time⟩, s⟩ := by
    simp [publish, discard_hidden, copy, mine, free]
  have outcome : (publish b s dest time).2 = .copied := by simp [publish, copy, mine, free]
  have others := race_occupied (publish b s dest time).1 dest time rest _ won
    (fun z hz => by
      rw [publish_staging]
      simp only [fun eq => distinct.1 z hz eq, ite_false]
      exact staged z (by simp [hz]))
    distinct.2
  rw [race_cons]
  refine ⟨by simp [outcome, others.1], others.2.1, ?_⟩
  intro y hy
  rcases List.mem_cons.mp hy with rfl | hy
  · exact race_keeps_gone _ dest time _ rest
      (fun hm => by obtain ⟨w, hw, eq⟩ := List.mem_map.mp hm; exact distinct.1 w hw eq)
      (by rw [publish_staging]; simp)
  · exact others.2.2 y hy

/-- A lost copy reply: the retry's copy refuses the occupied name, and
reading the destination finds this session's marker, so it settles as
stored, whatever was published or staged in between. -/
theorem lost_reply_settles (b : Bucket) (s : Nat) (dest : Path) (time : Nat) (ops : List Op)
    (landed : (copy b s dest time).2 = .copied) :
    confirm (run (copy b s dest time).1 ops).1 s dest = .stored := by
  have held : ∃ x, (copy b s dest time).1.published dest = some x ∧ x.session = s := by
    cases hs : b.staging s with
    | none => simp [copy, hs] at landed
    | some bytes =>
        cases hd : b.published dest with
        | some _ => simp [copy, hs, hd] at landed
        | none => exact ⟨⟨⟨bytes, time⟩, s⟩, by simp [copy, hs, hd], rfl⟩
  obtain ⟨x, hx, hs⟩ := held
  simp [confirm, run_preserves _ ops dest x hx, hs]

/-- Another session's object settles as `reset`: the occupied name is not
this session's publication (§10). -/
theorem other_marker_refuses (b : Bucket) (s : Nat) (dest : Path) (x : Published)
    (held : b.published dest = some x) (other : x.session ≠ s) : confirm b s dest = .reset := by
  simp [confirm, held, other]

/-- The marker settles bytes: a session's temporary is fixed when it is
assembled (its attempt's bytes are fixed, §6), so a destination holding this
session's marker holds this session's bytes. -/
theorem marker_settles_bytes (b : Bucket) (s : Nat) (dest : Path) (bytesOf : Nat → Bytes)
    (fixed : ∀ p x, b.published p = some x → x.object.bytes = bytesOf x.session)
    (stored : confirm b s dest = .stored) :
    ∃ x, b.published dest = some x ∧ x.object.bytes = bytesOf s := by
  cases hd : b.published dest with
  | none => simp [confirm, hd] at stored
  | some x =>
      by_cases same : x.session = s
      · exact ⟨x, rfl, same ▸ fixed dest x hd⟩
      · simp [confirm, hd, same] at stored

/-- Why atomicity is the premise: a completion that checks the name and then
writes it lets two racers both check a free name. Both report success and the
later write replaces the first winner's object. SeaweedFS 4.48's conditional
multipart completion behaved this way in certification suite 4. -/
def checkThenWrite (b : Bucket) (racers : List (Nat × Bytes)) (dest : Path) (time : Nat) :
    Bucket × List Outcome :=
  let free := (b.published dest).isNone
  let outcomes := racers.map fun _ => if free then Outcome.copied else .refused
  let last := racers.foldl (fun c (s, bytes) =>
    if free then { c with published := fun p =>
      if p = dest then some ⟨⟨bytes, time⟩, s⟩ else c.published p } else c) b
  (last, outcomes)

theorem check_then_write_two_winners :
    let after := checkThenWrite empty [(1, [1]), (2, [2])] (.write 31 1) 0
    after.2 = [.copied, .copied] ∧
      after.1.objects (.write 31 1) = some ⟨[2], 0⟩ ∧
      confirm after.1 1 (.write 31 1) = .reset := by
  decide

end CovenStorage.CopyPublication
