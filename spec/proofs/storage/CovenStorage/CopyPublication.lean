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

/-! ## Every object by copy

A profile that publishes every object by copy (`S3CopyPublication::Every`)
stages each create, whatever its size, as a session temporary and publishes
it with one conditional copy, so every publication of a name is a copy: a
run of `Op`s. The provider assumption is copy against copy only: the
provider applies conditional copies of one name one at a time, each checking
the destination and writing it in one step (`copy`). Nothing is assumed of
a conditional PUT racing a copy (`put_racing_copy_two_winners`). -/

/-- 1 when this step was a copy that published `dest`. -/
def Op.copiedTo (dest : Path) : Op → Option Outcome → Nat
  | .publish _ d _, some .copied => if d = dest then 1 else 0
  | _, _ => 0

/-- How many of a run's copies published `dest`. -/
def copiedAt (dest : Path) : Bucket → List Op → Nat
  | _, [] => 0
  | b, op :: ops => op.copiedTo dest (op.apply b).2 + copiedAt dest (op.apply b).1 ops

theorem publish_elsewhere (b : Bucket) (s : Nat) (d dest : Path) (time : Nat)
    (other : d ≠ dest) : (publish b s d time).1.published dest = b.published dest := by
  have : dest ≠ d := fun eq => other eq.symm
  simp only [publish, discard_hidden, copy]
  cases b.staging s with
  | none => rfl
  | some _ => cases b.published d <;> simp [this]

/-- A publication that did not copy left the published objects as they were. -/
theorem publish_unchanged (b : Bucket) (s : Nat) (d : Path) (time : Nat)
    (refused : (publish b s d time).2 ≠ .copied) :
    (publish b s d time).1.published = b.published := by
  simp only [publish, discard_hidden, copy] at refused ⊢
  cases hs : b.staging s with
  | none => rfl
  | some _ =>
      cases hd : b.published d with
      | some _ => rfl
      | none => simp [hs, hd] at refused

/-- A copy that published `d` leaves it occupied. -/
theorem publish_occupies (b : Bucket) (s : Nat) (d : Path) (time : Nat)
    (copied : (publish b s d time).2 = .copied) :
    ((publish b s d time).1.published d).isSome := by
  simp only [publish, discard_hidden, copy] at copied ⊢
  cases hs : b.staging s with
  | none => simp [hs] at copied
  | some _ =>
      cases hd : b.published d with
      | some _ => simp [hs, hd] at copied
      | none => simp

/-- Copies into an occupied name never publish. -/
theorem publish_occupied (b : Bucket) (s : Nat) (d : Path) (time : Nat)
    (occupied : (b.published d).isSome) : (publish b s d time).2 ≠ .copied := by
  simp only [publish, copy]
  cases hs : b.staging s with
  | none => simp
  | some _ =>
      cases hd : b.published d with
      | none => simp [hd] at occupied
      | some _ => simp

theorem copiedAt_stage (dest : Path) (b : Bucket) (s : Nat) (bytes : Bytes) (ops : List Op) :
    copiedAt dest b (.stage s bytes :: ops) = copiedAt dest (stage b s bytes) ops := by
  simp [copiedAt, Op.apply, Op.copiedTo]

theorem copiedAt_publish (dest : Path) (b : Bucket) (s : Nat) (d : Path) (time : Nat)
    (ops : List Op) :
    copiedAt dest b (.publish s d time :: ops) =
      (if d = dest ∧ (publish b s d time).2 = .copied then 1 else 0) +
        copiedAt dest (publish b s d time).1 ops := by
  simp only [copiedAt, Op.apply]
  cases (publish b s d time).2 <;> simp [Op.copiedTo]

/-- Of a run's copies to one name, in any arrival order and with any
temporaries staged, at most one publishes, and none when the name was
already occupied. -/
theorem copied_at_most (dest : Path) (b : Bucket) (ops : List Op) :
    copiedAt dest b ops ≤ 1 ∧ ((b.published dest).isSome → copiedAt dest b ops = 0) := by
  induction ops generalizing b with
  | nil => simp [copiedAt]
  | cons op ops ih =>
      cases op with
      | stage s bytes =>
          rw [copiedAt_stage]
          exact ih (stage b s bytes)
      | publish s d time =>
          rw [copiedAt_publish]
          have rest := ih (publish b s d time).1
          by_cases same : d = dest
          · subst same
            by_cases copied : (publish b s d time).2 = .copied
            · have zero := rest.2 (publish_occupies b s d time copied)
              have free : (b.published d).isSome = false := by
                cases h : (b.published d).isSome
                · rfl
                · exact absurd copied (publish_occupied b s d time h)
              simp [copied, zero, free]
            · have kept := publish_unchanged b s d time copied
              rw [kept] at rest
              simpa [copied] using rest
          · have kept := publish_elsewhere b s d dest time same
            rw [kept] at rest
            simpa [same] using rest

/-- When every publication of a name is a conditional copy, at most one is
accepted, given the provider applies one name's copies one at a time. -/
theorem every_copy_at_most_one (dest : Path) (b : Bucket) (ops : List Op) :
    copiedAt dest b ops ≤ 1 :=
  (copied_at_most dest b ops).1

/-- A conditional PUT the provider checks and writes in two steps, between
which a copy of the same name can land. -/
def putCheck (b : Bucket) (dest : Path) : Bool := (b.published dest).isNone

def putWrite (b : Bucket) (checked : Bool) (s : Nat) (dest : Path) (bytes : Bytes)
    (time : Nat) : Bucket × Bool :=
  if checked then
    ({ b with published := fun p => if p = dest then some ⟨⟨bytes, time⟩, s⟩ else b.published p },
      true)
  else (b, false)

/-- Why the assumption covers copies only: a conditional PUT checks a free
name, a copy of that name lands, then the PUT writes. Both are accepted and
the PUT replaces the copy's object, so the copy's session no longer finds its
marker. SeaweedFS 4.48's unversioned bucket accepted both writers of this
race in 14 of 200 suite 5 trials. -/
theorem put_racing_copy_two_winners :
    let dest := Path.write 31 1
    let staged := stage empty 2 [2]
    let copied := publish staged 2 dest 0
    let put := putWrite copied.1 (putCheck staged dest) 1 dest [1] 0
    copied.2 = .copied ∧ put.2 = true ∧ put.1.objects dest = some ⟨[1], 0⟩ ∧
      confirm put.1 2 dest = .reset := by
  decide

/-! ## Every staged temporary is recorded

A temporary, or a native multipart upload that will assemble one, is the
publisher's to remove. Under T208 a single-request create staged under a
fresh attempt id and a multipart session was recorded only after its upload
was initiated, so a crash in between left one that no record named and no
later step removes. The rule (T218): stage only under a name the caller has
already recorded, and drop the record only once nothing is staged under it.
A single-request create's temporary is named by the create itself (its
destination and bytes), which the caller keeps until the create settles; a
multipart session's name is recorded before its upload is initiated. -/

namespace Recorded

/-- A create the caller keeps until it settles: its destination and fixed bytes. -/
structure Create where
  dest : Path
  bytes : Bytes
  deriving DecidableEq, Repr

/-- A temporary's name: a create's own, or an id drawn for one attempt or
session. -/
inductive Name where
  | create (c : Create)
  | attempt (id : Nat)
  deriving DecidableEq, Repr

/-- Published objects; staged temporaries by name, with bytes and the
attempt's marker; the names the caller's durable records hold. -/
structure World where
  published : Path → Option Published
  staging : Name → Option (Bytes × Nat)
  recorded : List Name

inductive Step where
  /-- The caller commits a record naming `n`: a queued create, or a session. -/
  | record (n : Name)
  /-- Stage bytes under `n` with an attempt's marker (a PUT, or an initiated
  and assembled multipart upload). -/
  | stage (n : Name) (bytes : Bytes) (marker : Nat)
  /-- One conditional copy of `n` to `dest`. -/
  | copy (n : Name) (dest : Path) (time : Nat)
  /-- Delete the temporary; deleting an absent one succeeds. -/
  | delete (n : Name)
  /-- The caller drops its record once the call settled. -/
  | settle (n : Name)
  deriving DecidableEq, Repr

/-- `strict` is the rule: staging needs a recorded name, and a record is
dropped only with nothing staged under it. Otherwise (T208) staging and
dropping are unguarded. A crash is a step that never comes: every step
leaves durable state as it stands. -/
def step (strict : Bool) (w : World) : Step → World
  | .record n => { w with recorded := n :: w.recorded }
  | .stage n bytes marker =>
      if !strict || n ∈ w.recorded then
        { w with staging := fun m => if m = n then some (bytes, marker) else w.staging m }
      else w
  | .copy n dest time =>
      match w.staging n, w.published dest with
      | some (bytes, marker), none =>
          { w with published := fun p =>
              if p = dest then some ⟨⟨bytes, time⟩, marker⟩ else w.published p }
      | _, _ => w
  | .delete n => { w with staging := fun m => if m = n then none else w.staging m }
  | .settle n =>
      if !strict || w.staging n = none then { w with recorded := w.recorded.erase n } else w

def run (strict : Bool) (w : World) (steps : List Step) : World := steps.foldl (step strict) w

def start : World := ⟨fun _ => none, fun _ => none, []⟩

/-- Every staged temporary's name is recorded. -/
def Covered (w : World) : Prop := ∀ n, (w.staging n).isSome → n ∈ w.recorded

theorem step_covered {w : World} (h : Covered w) (s : Step) : Covered (step true w s) := by
  intro n hn
  cases s with
  | record m =>
      simp only [step] at hn ⊢
      exact List.mem_cons_of_mem _ (h n hn)
  | stage m bytes marker =>
      by_cases hm : m ∈ w.recorded
      · simp only [step, hm] at hn ⊢
        simp at hn ⊢
        by_cases e : n = m
        · subst e; exact hm
        · simp only [e, ↓reduceIte] at hn; exact h n hn
      · simp [step, hm] at hn ⊢
        exact h n hn
  | copy m dest time =>
      simp only [step] at hn ⊢
      split at hn <;> exact h n hn
  | delete m =>
      simp only [step] at hn ⊢
      by_cases e : n = m
      · subst e; simp at hn
      · simp only [e, ↓reduceIte] at hn; exact h n hn
  | settle m =>
      by_cases hs : w.staging m = none
      · simp only [step, hs] at hn ⊢
        simp at hn ⊢
        by_cases e : n = m
        · subst e; rw [hs] at hn; cases hn
        · exact (List.mem_erase_of_ne e).mpr (h n hn)
      · simp [step, hs] at hn ⊢
        exact h n hn

/-- Under the rule, in every history and whatever crashes interrupt, every
staged temporary is named by a record the caller still holds. -/
theorem staged_recorded (steps : List Step) : Covered (run true start steps) := by
  suffices ∀ w, Covered w → Covered (run true w steps) from this start (by intro n hn; simp [start] at hn)
  intro w hw
  induction steps generalizing w with
  | nil => exact hw
  | cons s ss ih => exact ih _ (step_covered hw s)

/-- A record's retry removes its temporary: staging again under the same
name, copying and deleting leave nothing staged there, whatever an earlier,
crashed attempt left; the record can then be dropped. -/
theorem retry_clears (w : World) (n : Name) (bytes : Bytes) (marker : Nat) (dest : Path)
    (time : Nat) :
    (run true w [.stage n bytes marker, .copy n dest time, .delete n]).staging n = none := by
  simp [run, step]

/-- A create whose copy landed but whose deletion failed: its retry, with a
fresh marker under the same name, is refused the occupied destination and
leaves it holding the create's own bytes, so comparing bytes accepts it as
the same create; its deletion then clears the name. -/
theorem failed_deletion_retry_accepted (c : Create) (time time' marker marker' : Nat) :
    let n := Name.create c
    let first := run true start [.record n, .stage n c.bytes marker, .copy n c.dest time]
    let retry := run true first [.stage n c.bytes marker', .copy n c.dest time', .delete n]
    (first.staging n).isSome ∧
      (retry.published c.dest).map (·.object.bytes) = some c.bytes ∧
      retry.published c.dest = first.published c.dest ∧
      retry.staging n = none := by
  simp [run, step, start]

/-- Why names must be recorded first: under T208's naming a single-request
create stages under a fresh attempt id, and a crash before its deletion
strands it. The retry stages under another id, publishes and deletes its
own, and the record is dropped: the first temporary stays, and no record
names it. A multipart session initiated before it was recorded is the same
history with the native upload for the temporary. -/
theorem attempt_naming_strands :
    let c : Create := ⟨.write 31 1, [7]⟩
    let after := run false start
      [.record (.create c), .stage (.attempt 0) c.bytes 0,
       .stage (.attempt 1) c.bytes 1, .copy (.attempt 1) c.dest 0, .delete (.attempt 1),
       .settle (.create c)]
    (after.staging (.attempt 0)).isSome ∧ after.recorded = [] := by
  decide

/-! ### Attempts of one create share its temporary (T232)

Two creators of one path with identical bytes are attempts of one create:
they stage, copy and delete one name. One can delete the temporary between
the other's staging and its copy, which then finds no source. That answer
says nothing about the destination, so the adapter settles it by reading the
destination, as it settles a refused copy or a lost reply: this attempt's
marker is stored, another object is an occupied path whose bytes the caller
compares (§10), and an empty destination is a retryable failure that keeps
the record. -/

/-- What one attempt's create returns to its caller. -/
inductive CreateAnswer where
  /-- The destination carries this attempt's marker. -/
  | stored
  /-- Another publication occupies the destination; the caller compares
  these bytes with its own (§10). -/
  | occupied (bytes : Bytes)
  /-- Nothing is published; the caller keeps its record and retries. -/
  | retry
  /-- A hard error that neither settles nor retries the create. -/
  | failed
  deriving DecidableEq, Repr

/-- Reading the destination after a copy whose answer does not settle it. -/
def confirmDest (w : World) (dest : Path) (marker : Nat) : CreateAnswer :=
  match w.published dest with
  | some p => if p.session = marker then .stored else .occupied p.object.bytes
  | none => .retry

/-- One attempt's conditional copy and its answer. `settles` is T232's rule:
a copy whose temporary is gone is settled by reading the destination; without
it, the provider's not-found answer is returned as a hard error. -/
def attemptCopy (settles : Bool) (w : World) (n : Name) (dest : Path) (time marker : Nat) :
    World × CreateAnswer :=
  let after := step true w (.copy n dest time)
  match w.staging n, w.published dest with
  | some _, none => (after, .stored)
  | some _, some _ => (after, confirmDest after dest marker)
  | none, _ => (after, if settles then confirmDest after dest marker else .failed)

/-- One attempt's request to create `c`, in its arrival order at the provider. -/
inductive Act where
  | stage (marker : Nat)
  | copy (time marker : Nat)
  | delete
  deriving DecidableEq, Repr

/-- Attempts of create `c`, interleaved in any arrival order; each copy's answer. -/
def runActs (settles : Bool) (c : Create) : World → List Act → World × List CreateAnswer
  | w, [] => (w, [])
  | w, .stage m :: acts => runActs settles c (step true w (.stage (.create c) c.bytes m)) acts
  | w, .copy t m :: acts =>
      let (next, answer) := attemptCopy settles w (.create c) c.dest t m
      let (last, answers) := runActs settles c next acts
      (last, answer :: answers)
  | w, .delete :: acts => runActs settles c (step true w (.delete (.create c))) acts

theorem confirmDest_not_failed (w : World) (dest : Path) (marker : Nat) :
    confirmDest w dest marker ≠ .failed := by
  unfold confirmDest
  split
  · split <;> simp
  · simp

theorem attemptCopy_not_failed (w : World) (n : Name) (dest : Path) (time marker : Nat) :
    (attemptCopy true w n dest time marker).2 ≠ .failed := by
  unfold attemptCopy
  split
  · simp
  · exact confirmDest_not_failed _ _ _
  · exact confirmDest_not_failed _ _ _

/-- Under the rule no attempt of a create ends in a hard error, in any
interleaving of any number of attempts. -/
theorem shared_never_fails (c : Create) (w : World) (acts : List Act) :
    .failed ∉ (runActs true c w acts).2 := by
  induction acts generalizing w with
  | nil => simp [runActs]
  | cons a acts ih =>
      cases a with
      | stage m => exact ih _
      | delete => exact ih _
      | copy t m =>
          simp only [runActs, List.mem_cons, not_or]
          exact ⟨fun h => attemptCopy_not_failed w _ _ t m h.symm, ih _⟩

theorem attemptCopy_fst (settles : Bool) (w : World) (n : Name) (dest : Path)
    (time marker : Nat) :
    (attemptCopy settles w n dest time marker).1 = step true w (.copy n dest time) := by
  unfold attemptCopy
  split <;> rfl

theorem confirmDest_retry (w : World) (dest : Path) (marker : Nat)
    (h : confirmDest w dest marker = .retry) : w.published dest = none := by
  unfold confirmDest at h
  split at h
  · split at h <;> cases h
  · assumption

/-- A retry is answered only while the destination is empty: nothing is
published, so the create is not lost by retrying it. -/
theorem attemptCopy_retry_empty (settles : Bool) (w : World) (n : Name) (dest : Path)
    (time marker : Nat) (retry : (attemptCopy settles w n dest time marker).2 = .retry) :
    (attemptCopy settles w n dest time marker).1.published dest = none := by
  rw [attemptCopy_fst]
  apply confirmDest_retry (marker := marker)
  revert retry
  unfold attemptCopy
  split
  · simp
  · intro h; exact h
  · cases settles <;> simp

/-- The destination holds only the create's bytes, and its name stages only them. -/
def Same (c : Create) (w : World) : Prop :=
  (∀ p, w.published c.dest = some p → p.object.bytes = c.bytes) ∧
    (∀ b m, w.staging (.create c) = some (b, m) → b = c.bytes)

theorem same_stage (c : Create) (w : World) (m : Nat) (h : Same c w) :
    Same c (step true w (.stage (.create c) c.bytes m)) := by
  refine ⟨fun p hp => h.1 p (by simp only [step] at hp; split at hp <;> exact hp), ?_⟩
  intro b m' hs
  simp only [step] at hs
  split at hs
  · simp at hs; exact hs.1.symm
  · exact h.2 b m' hs

theorem same_delete (c : Create) (w : World) (h : Same c w) :
    Same c (step true w (.delete (.create c))) :=
  ⟨h.1, fun b m hs => by simp [step] at hs⟩

theorem same_copy (c : Create) (w : World) (t : Nat) (h : Same c w) :
    Same c (step true w (.copy (.create c) c.dest t)) := by
  constructor
  · intro p hp
    simp only [step] at hp
    split at hp
    · next bytes marker hs _ =>
        simp at hp
        rw [← hp]
        exact h.2 bytes marker hs
    · exact h.1 p hp
  · intro b m hs
    simp only [step] at hs
    split at hs <;> exact h.2 b m hs

theorem confirmDest_same (c : Create) (w : World) (marker : Nat) (b : Bytes) (h : Same c w)
    (occ : confirmDest w c.dest marker = .occupied b) : b = c.bytes := by
  unfold confirmDest at occ
  split at occ
  · next p hp =>
      split at occ
      · cases occ
      · cases occ; exact h.1 p hp
  · cases occ

/-- From a destination that is empty or already holds the create's bytes,
every attempt's occupied answer shows the create's own bytes, so the
caller's byte comparison accepts it as the same create (§10). -/
theorem shared_occupied_identical (c : Create) (w : World) (acts : List Act) (h : Same c w)
    (b : Bytes) (occ : .occupied b ∈ (runActs true c w acts).2) : b = c.bytes := by
  induction acts generalizing w with
  | nil => simp [runActs] at occ
  | cons a acts ih =>
      cases a with
      | stage m => exact ih _ (same_stage c w m h) occ
      | delete => exact ih _ (same_delete c w h) occ
      | copy t m =>
          simp only [runActs, List.mem_cons] at occ
          have next := same_copy c w t h
          rcases occ with eq | rest
          · revert eq
            unfold attemptCopy
            split
            · simp
            · exact fun eq => confirmDest_same c _ m b next eq.symm
            · simp only [ite_true]
              exact fun eq => confirmDest_same c _ m b next eq.symm
          · exact ih _ next (by rwa [attemptCopy_fst] at rest)

/-- A retry that finds the destination empty publishes: the retryable answer
loses nothing. -/
theorem retry_publishes (c : Create) (w : World) (m t : Nat) (recorded : .create c ∈ w.recorded)
    (empty : w.published c.dest = none) :
    (runActs true c w [.stage m, .copy t m]).2 = [.stored] := by
  simp [runActs, attemptCopy, step, recorded, empty]

/-- The interleaving T232 found: attempt 2 stages, attempt 1 stages over it,
copies and deletes the shared temporary, and attempt 2's copy finds no source.
Without the rule it is a hard error though the destination holds the create's
bytes; with it, an occupied path holding those bytes. -/
theorem shared_temporary_missing :
    let c : Create := ⟨.write 31 1, [7]⟩
    let w : World := { start with recorded := [.create c] }
    let acts := [Act.stage 2, .stage 1, .copy 0 1, .delete, .copy 0 2]
    (runActs false c w acts).2 = [.stored, .failed] ∧
      (runActs true c w acts).2 = [.stored, .occupied [7]] := by
  decide

/-- The marker names the staging, not the copy: when attempt 2 stages over
attempt 1's bytes before attempt 1 copies, attempt 1's copy publishes attempt
2's marker, and attempt 2's copy, finding no source, reads its own marker.
Both answer stored, which is what equal bytes settle to anyway (§10). -/
theorem shared_temporary_both_stored :
    let c : Create := ⟨.write 31 1, [7]⟩
    let w : World := { start with recorded := [.create c] }
    (runActs true c w [.stage 1, .stage 2, .copy 0 1, .delete, .copy 0 2]).2 =
      [.stored, .stored] := by
  decide

end Recorded

end CovenStorage.CopyPublication
