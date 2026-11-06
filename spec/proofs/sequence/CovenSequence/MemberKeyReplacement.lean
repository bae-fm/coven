/-! A rejected self-replacement cannot release writes under an unheld binding
(§12.3). Keys are equality-preserving public-pair tokens. The child claim has
already been undone. Installing a held candidate still owes the restore-code
commit, so that branch retains the pause too. -/
namespace CovenSequence.MemberKeyReplacement

inductive Resolution where
  | blocked | activate | undo
  deriving DecidableEq

def resolve (active candidate : Nat) (current : Option Nat) : Resolution :=
  if current = some candidate then .activate
  else if current = some active then .undo else .blocked

theorem unheld_keeps_pause (active candidate : Nat) (current : Option Nat)
    (old : current ≠ some active) (new : current ≠ some candidate) :
    resolve active candidate current = .blocked := by
  simp [resolve, old, new]

theorem candidate_installs (active candidate : Nat) :
    resolve active candidate (some candidate) = .activate := by
  simp [resolve]

theorem undo_requires_active (active candidate : Nat) (current : Option Nat)
    (ended : resolve active candidate current = .undo) :
    current = some active ∧ current ≠ some candidate := by
  unfold resolve at ended
  split at ended
  · contradiction
  · split at ended
    · exact ⟨‹current = some active›, ‹current ≠ some candidate›⟩
    · contradiction

/-! ## Racing replacements (§10.4, §11.2)

A replacement names the member, the pair it expects current, and its new pair.
It applies only while that pair is current and the new pair was never bound, as
every device judges it. Pairs are equality-preserving tokens. -/

structure Replacement (Mem : Type) where
  member : Mem
  expected : Nat
  new : Nat

/-- Each member's current pair, and every pair ever bound. -/
structure Pairs (Mem : Type) where
  current : Mem → Nat
  bound : List Nat

variable {Mem : Type} [DecidableEq Mem]

def replace (p : Pairs Mem) (r : Replacement Mem) : Option (Pairs Mem) :=
  if p.current r.member = r.expected ∧ r.new ∉ p.bound then
    some { current := fun m => if m = r.member then r.new else p.current m,
           bound := r.new :: p.bound }
  else none

/-- Replacements in slot order; a refused one changes nothing. -/
def run (p : Pairs Mem) : List (Replacement Mem) → Pairs Mem
  | [] => p
  | r :: rs => run ((replace p r).getD p) rs

/-- Every current pair is bound. -/
def Bound (p : Pairs Mem) : Prop := ∀ m, p.current m ∈ p.bound

theorem replace_bound {p q : Pairs Mem} {r : Replacement Mem} (hb : Bound p)
    (applied : replace p r = some q) : Bound q := by
  unfold replace at applied
  split at applied
  · cases applied
    intro m
    by_cases same : m = r.member
    · simp [same]
    · simp [same, hb m]
  · cases applied

/-- **A replaced pair never comes back.** Once a replacement applies, the pair it
replaced is never its member's current pair again, whatever replacements follow. -/
theorem replaced_never_current {p q : Pairs Mem} {r : Replacement Mem} (hb : Bound p)
    (applied : replace p r = some q) (rs : List (Replacement Mem)) :
    (run q rs).current r.member ≠ r.expected := by
  have holds : ∀ (rs : List (Replacement Mem)) (s : Pairs Mem), r.expected ∈ s.bound →
      s.current r.member ≠ r.expected → (run s rs).current r.member ≠ r.expected := by
    intro rs
    induction rs with
    | nil => intro s _ ne; exact ne
    | cons x xs ih =>
      intro s kept ne
      simp only [run]
      cases h : replace s x with
      | none => exact ih s kept ne
      | some t =>
        unfold replace at h
        split at h
        · rename_i ok
          cases h
          simp only [Option.getD_some]
          apply ih
          · exact List.mem_cons_of_mem _ kept
          · by_cases same : r.member = x.member
            · simp only [same, ite_true]
              intro eq
              exact ok.2 (eq ▸ kept)
            · simp [same, ne]
        · cases h
  unfold replace at applied
  split at applied
  · rename_i ok
    cases applied
    apply holds
    · exact List.mem_cons_of_mem _ (ok.1 ▸ hb r.member)
    · simp only [ite_true]
      intro eq
      exact ok.2 (eq ▸ ok.1 ▸ hb r.member)
  · cases applied

/-- **A retry never overwrites a competing replacement.** Of two replacements
expecting the same pair, once one applies, the other, and every retry of it,
which keeps the pair it expects (§11.2), is refused at every later slot. -/
theorem competing_replacement_refused {p q : Pairs Mem} {r : Replacement Mem} (hb : Bound p)
    (applied : replace p r = some q) (rs : List (Replacement Mem)) (r' : Replacement Mem)
    (same : r'.member = r.member) (expects : r'.expected = r.expected) :
    replace (run q rs) r' = none := by
  have := replaced_never_current hb applied rs
  simp [replace, same, expects, this]

/-! ## Replacing one's own keys (§12.3)

The device pauses the app's writes, sends the writes already queued, keeps the
new pair in custody, claims the replacement, installs the pair, refreshes the
restore code, and only then lifts the pause. A write while paused is refused
with `MemberKeysChanging`. The claim of the replacement's copies, and the
undo after a refusal (`resolve`), are outside this part. Slots are numbered by
`head`, the next free one; other devices fill slots at any time. -/

/-- A write: its id and whether it was made under the new pair. -/
abbrev Made := Nat × Bool

structure Device where
  paused : Bool
  /-- App writes waiting to send. -/
  queue : List Made
  /-- Writes landed, with their slots. -/
  sent : List (Made × Nat)
  /-- The new pair is kept in custody. -/
  candidate : Bool
  /-- The slot the replacement landed in. -/
  replacement : Option Nat
  installed : Bool
  code : Bool
  head : Nat

def Device.start (head : Nat) : Device :=
  ⟨false, [], [], false, none, false, false, head⟩

inductive Step : Device → Device → Prop
  /-- The app writes; refused with `MemberKeysChanging` while paused. -/
  | write (d : Device) (w : Nat) (open_ : d.paused = false) :
      Step d { d with queue := d.queue ++ [(w, d.installed)] }
  | send (d : Device) (x : Made) (rest : List Made) (first : d.queue = x :: rest) :
      Step d { d with queue := rest, sent := (x, d.head) :: d.sent, head := d.head + 1 }
  | other (d : Device) : Step d { d with head := d.head + 1 }
  | pause (d : Device) (unclaimed : d.replacement = none) : Step d { d with paused := true }
  | keep (d : Device) (paused : d.paused = true) : Step d { d with candidate := true }
  /-- Claim the replacement once the queue is sent and the pair is in custody. -/
  | claim (d : Device) (paused : d.paused = true) (drained : d.queue = [])
      (kept : d.candidate = true) (unclaimed : d.replacement = none) :
      Step d { d with replacement := some d.head, head := d.head + 1 }
  | install (d : Device) (landed : d.replacement.isSome = true) : Step d { d with installed := true }
  | refresh (d : Device) (installed : d.installed = true) : Step d { d with code := true }
  | lift (d : Device) (installed : d.installed = true) (code : d.code = true) :
      Step d { d with paused := false }
  /-- Give up before claiming: the old pair is still current. -/
  | abandon (d : Device) (unclaimed : d.replacement = none) : Step d { d with paused := false }

inductive Steps : Device → Device → Prop
  | refl (d : Device) : Steps d d
  | step {a b c : Device} : Step a b → Steps b c → Steps a c

/-- What every reachable device satisfies. -/
structure Inv (d : Device) : Prop where
  queued_new : ∀ x ∈ d.queue, x.2 = true → d.installed = true
  sent_new : ∀ e ∈ d.sent, e.1.2 = true → d.installed = true
  sent_below : ∀ e ∈ d.sent, e.2 < d.head
  installed_claimed : d.installed = true → d.replacement.isSome = true
  claimed : ∀ r, d.replacement = some r →
    r < d.head ∧ d.candidate = true ∧
    (∀ x ∈ d.queue, x.2 = true) ∧
    (∀ e ∈ d.sent, e.1.2 = false → e.2 < r) ∧
    (∀ e ∈ d.sent, e.1.2 = true → r < e.2) ∧
    (d.paused = false → d.installed = true ∧ d.code = true)

theorem inv_start (head : Nat) : Inv (Device.start head) :=
  ⟨by simp [Device.start], by simp [Device.start], by simp [Device.start],
    by simp [Device.start], by simp [Device.start]⟩

theorem device_step_inv {d d' : Device} (st : Step d d') (hi : Inv d) : Inv d' := by
  cases st with
  | write w open_ =>
    refine ⟨fun x hx new => ?_, hi.sent_new, hi.sent_below, hi.installed_claimed, fun r hr => ?_⟩
    · simp only [List.mem_append, List.mem_singleton] at hx
      rcases hx with old | rfl
      · exact hi.queued_new x old new
      · exact new
    · obtain ⟨below, kept, queued, old, new, lifted⟩ := hi.claimed r hr
      refine ⟨below, kept, fun x hx => ?_, old, new, lifted⟩
      simp only [List.mem_append, List.mem_singleton] at hx
      rcases hx with old' | rfl
      · exact queued x old'
      · exact (lifted open_).1
  | send x rest first =>
    have inQueue : x ∈ d.queue := by simp [first]
    refine ⟨fun y hy new => hi.queued_new y (by simp [first, hy]) new, fun e he new => ?_,
      fun e he => ?_, hi.installed_claimed, fun r hr => ?_⟩
    · simp only [List.mem_cons] at he
      rcases he with rfl | old
      · exact hi.queued_new x inQueue new
      · exact hi.sent_new e old new
    · simp only [List.mem_cons] at he
      rcases he with rfl | old
      · exact Nat.lt_succ_self _
      · exact Nat.lt_succ_of_lt (hi.sent_below e old)
    · obtain ⟨below, kept, queued, old, new, lifted⟩ := hi.claimed r hr
      refine ⟨Nat.lt_succ_of_lt below, kept, fun y hy => queued y (by simp [first, hy]),
        fun e he oldPair => ?_, fun e he newPair => ?_, lifted⟩
      · simp only [List.mem_cons] at he
        rcases he with rfl | prior
        · have := queued _ inQueue
          simp_all
        · exact old e prior oldPair
      · simp only [List.mem_cons] at he
        rcases he with rfl | prior
        · exact below
        · exact new e prior newPair
  | other =>
    refine ⟨hi.queued_new, hi.sent_new, fun e he => Nat.lt_succ_of_lt (hi.sent_below e he),
      hi.installed_claimed, fun r hr => ?_⟩
    obtain ⟨below, rest⟩ := hi.claimed r hr
    exact ⟨Nat.lt_succ_of_lt below, rest⟩
  | pause unclaimed =>
    exact ⟨hi.queued_new, hi.sent_new, hi.sent_below, hi.installed_claimed,
      fun r hr => by simp [unclaimed] at hr⟩
  | keep paused =>
    refine ⟨hi.queued_new, hi.sent_new, hi.sent_below, hi.installed_claimed, fun r hr => ?_⟩
    obtain ⟨below, _, rest⟩ := hi.claimed r hr
    exact ⟨below, rfl, rest⟩
  | claim paused drained kept unclaimed =>
    have notInstalled : d.installed = false := by
      cases h : d.installed
      · rfl
      · have := hi.installed_claimed h; simp [unclaimed] at this
    refine ⟨by simp [drained], hi.sent_new, fun e he => Nat.lt_succ_of_lt (hi.sent_below e he),
      fun h => by simp, fun r hr => ?_⟩
    simp only [Option.some.injEq] at hr
    subst hr
    refine ⟨Nat.lt_succ_self _, kept, by simp [drained], fun e he _ => hi.sent_below e he,
      fun e he new => ?_, fun open_ => ?_⟩
    · have := hi.sent_new e he new; simp [notInstalled] at this
    · simp [paused] at open_
  | install landed =>
    refine ⟨fun x _ _ => rfl, fun e _ _ => rfl, hi.sent_below, fun _ => landed, fun r hr => ?_⟩
    obtain ⟨below, kept, queued, old, new, lifted⟩ := hi.claimed r hr
    exact ⟨below, kept, queued, old, new, fun open_ => ⟨rfl, (lifted open_).2⟩⟩
  | refresh installed =>
    refine ⟨hi.queued_new, hi.sent_new, hi.sent_below, hi.installed_claimed, fun r hr => ?_⟩
    obtain ⟨below, kept, queued, old, new, _⟩ := hi.claimed r hr
    exact ⟨below, kept, queued, old, new, fun _ => ⟨installed, rfl⟩⟩
  | lift installed code =>
    refine ⟨hi.queued_new, hi.sent_new, hi.sent_below, hi.installed_claimed, fun r hr => ?_⟩
    obtain ⟨below, kept, queued, old, new, _⟩ := hi.claimed r hr
    exact ⟨below, kept, queued, old, new, fun _ => ⟨installed, code⟩⟩
  | abandon unclaimed =>
    exact ⟨hi.queued_new, hi.sent_new, hi.sent_below, hi.installed_claimed,
      fun r hr => by simp [unclaimed] at hr⟩

theorem device_steps_inv {d d' : Device} (run : Steps d d') (hi : Inv d) : Inv d' := by
  induction run with
  | refl => exact hi
  | step st _ ih => exact ih (device_step_inv st hi)

/-- **Every write under the old pair lands before the replacement, and every
write under the new pair after it.** Before the replacement's slot the old
pair still counts; from it on, only the new one does (§14.1), so no write the
device made is lost to the cut-off. No write under the old pair still waits
once the replacement has landed. -/
theorem writes_straddle_replacement {d : Device} (head : Nat) (run : Steps (Device.start head) d)
    {r : Nat} (landed : d.replacement = some r) :
    (∀ e ∈ d.sent, e.1.2 = false → e.2 < r) ∧ (∀ e ∈ d.sent, e.1.2 = true → r < e.2) ∧
      ∀ x ∈ d.queue, x.2 = true := by
  obtain ⟨_, _, queued, old, new, _⟩ := (device_steps_inv run (inv_start head)).claimed r landed
  exact ⟨old, new, queued⟩

/-- **The pause lifts only after custody and the restore code.** Once the
replacement has landed, the device writes again only after the new pair is in
custody, installed, and its restore code refreshed; a write made meanwhile is
refused with `MemberKeysChanging`. -/
theorem pause_lifts_after_install {d : Device} (head : Nat) (run : Steps (Device.start head) d)
    {r : Nat} (landed : d.replacement = some r) (writing : d.paused = false) :
    d.candidate = true ∧ d.installed = true ∧ d.code = true := by
  obtain ⟨_, kept, _, _, _, lifted⟩ := (device_steps_inv run (inv_start head)).claimed r landed
  exact ⟨kept, lifted writing⟩

end CovenSequence.MemberKeyReplacement
