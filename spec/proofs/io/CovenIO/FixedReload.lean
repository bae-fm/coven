import CovenIO.Reload

namespace CovenIO.FixedReload

/-- The fixed list enumerates published numbers in the required writer prefixes.
The reader uses their paths; audience metadata is used only in the proofs.
A successful absent GET has a fresh, complete deletion certificate. -/
structure Source (order : SnapshotOrder) (target : List Write) where
  world : World
  history : Nat → SnapshotHistory.State
  published : Write → Prop
  required : ∀ w ∈ target, published w
  unique : ∀ u v, published u → published v → u.writer = v.writer → u.number = v.number → u = v
  runs : ∀ t, SnapshotHistory.Run published order (history t)
  deletedMono : ∀ s t, s ≤ t → (history s).deleted ⊆ (history t).deleted
  availableFrom : Nat := 0
  absent : ∀ t w, availableFrom ≤ t → w ∈ target → world t (.log .write w.writer w.number) = none →
    w ∈ (history (t + 1)).deleted

def position (snap : Option Snapshot) (writer : Nat) : Nat :=
  match snap with | none => 0 | some s => s.position writer

def selected (source : Source order target) (audience time : Nat) : Option Snapshot :=
  catalogCurrent order (source.history time).landed audience

/-- A deletion certificate identifies the numbered path, without knowing
which encrypted audiences its missing object contained. -/
def deletedNumber (deleted : List Write) (w : Write) : Bool :=
  deleted.any (fun v => v.writer == w.writer && v.number == w.number)

theorem deleted_number (source : Source order target) (time : Nat) (w : Write) (required : w ∈ target) :
    deletedNumber (source.history time).deleted w = true ↔ w ∈ (source.history time).deleted := by
  constructor
  · intro known
    obtain ⟨v, member, path⟩ := List.any_eq_true.mp known
    have keys : v.writer = w.writer ∧ v.number = w.number := by simpa using path
    have pub := ((SnapshotHistory.run_valid (source.runs time)).2 v member).1
    have eq := source.unique v w pub (source.required w required) keys.1 keys.2
    exact eq ▸ member
  · intro member
    exact List.any_eq_true.mpr ⟨w, member, by simp⟩

inductive Result where
  | complete (received : List Write)
  | retry (time : Nat) (missing : Write)
  | uncertified (missing : Write)
  deriving DecidableEq, Repr

/-- A round stops at the frozen target. The selection's fresh catalog already
certifies its deleted holes: covered ones are in the snapshot, uncovered ones
have no part in this audience. Later misses refresh that evidence after GET. -/
def scan (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (audience : Nat) (loaded : Option Snapshot) (knownDeleted : List Write) : List Write → Nat → Result
  | [], _ => .complete []
  | w :: rest, time =>
      if w.number ≤ position loaded w.writer ∨ deletedNumber knownDeleted w = true then
        scan world history order audience loaded knownDeleted rest time
      else match world time (.log .write w.writer w.number) with
      | some _ => match scan world history order audience loaded knownDeleted rest (time + 1) with
          | .complete received => .complete (w :: received)
          | other => other
      | none =>
          if deletedNumber (history (time + 1)).deleted w then
            if w.number ≤ position (catalogCurrent order (history (time + 1)).landed audience) w.writer
            then .retry (time + 1) w
            else scan world history order audience loaded knownDeleted rest (time + 1)
          else .uncertified w

/-- A replacement catalog is observed after the round's missing GET. -/
theorem scan_retry_time (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (audience : Nat) (loaded : Option Snapshot) (knownDeleted todo : List Write)
    (time next : Nat) (missing : Write)
    (result : scan world history order audience loaded knownDeleted todo time = .retry next missing) :
    time < next := by
  induction todo generalizing time with
  | nil => cases result
  | cons w rest ih =>
      simp only [scan] at result
      split at result
      · exact ih time result
      · split at result
        · cases h : scan world history order audience loaded knownDeleted rest (time + 1) with
          | complete received => simp [h] at result
          | retry t v =>
              simp only [h] at result
              cases result
              exact Nat.lt_of_succ_lt (ih (time + 1) h)
          | uncertified v => simp [h] at result
        · split at result
          · split at result
            · cases result; omega
            · exact Nat.lt_of_succ_lt (ih (time + 1) result)
          · contradiction

/-- A finite scan retains no write beyond the enumerated cut. -/
theorem scan_received_subset (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (audience : Nat) (loaded : Option Snapshot) (knownDeleted todo : List Write)
    (time : Nat) (received : List Write)
    (result : scan world history order audience loaded knownDeleted todo time = .complete received) :
    received ⊆ todo := by
  induction todo generalizing time received with
  | nil => cases result; exact List.Subset.refl []
  | cons w rest ih =>
      simp only [scan] at result
      split at result
      · exact List.Subset.trans (ih time received result) (by intro v hv; exact List.mem_cons_of_mem _ hv)
      · split at result
        · cases h : scan world history order audience loaded knownDeleted rest (time + 1) with
          | complete got =>
              simp only [h] at result
              cases result
              exact List.cons_subset_cons _ (ih (time + 1) got h)
          | retry t v => simp [h] at result
          | uncertified v => simp [h] at result
        · split at result
          · split at result
            · contradiction
            · exact List.Subset.trans (ih (time + 1) received result) (by intro v hv; exact List.mem_cons_of_mem _ hv)
          · contradiction

theorem scan_covered (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (audience : Nat) (loaded : Option Snapshot) (knownDeleted target : List Write) (time : Nat)
    (covered : ∀ w ∈ target, w.number ≤ position loaded w.writer) :
    scan world history order audience loaded knownDeleted target time = .complete [] := by
  induction target with
  | nil => rfl
  | cons w rest ih =>
      have head := covered w (by simp)
      simpa [scan, head] using ih (fun v hv => covered v (by simp [hv]))

def round (source : Source order target) (audience start : Nat) : Result :=
  scan source.world source.history order audience (selected source audience start)
    (source.history start).deleted target start

def Realized (audience : Nat) (loaded : Option Snapshot) (received : List Write) (w : Write) : Prop :=
  audience ∈ w.audiences → w.number ≤ position loaded w.writer ∨ w ∈ received

theorem deleted_selected (source : Source order target) (audience time : Nat) (w : Write)
    (deleted : w ∈ (source.history time).deleted) (relevant : audience ∈ w.audiences) :
    w.number ≤ position (selected source audience time) w.writer := by
  have covered := SnapshotHistory.deleted_stays_covered (source.runs time) w deleted audience relevant
  cases h : selected source audience time with
  | none => simp [selected] at h; simp [h] at covered
  | some snap =>
      have hc : snap.covers w audience := by
        change (match selected source audience time with
          | none => False | some s => s.covers w audience) at covered
        simpa [h] using covered
      simpa [position, h] using hc.2

theorem skipped_has_no_part (source : Source order target) (audience time : Nat) (w : Write)
    (deleted : w ∈ (source.history time).deleted)
    (uncovered : ¬ w.number ≤ position (selected source audience time) w.writer) :
    audience ∉ w.audiences := fun relevant => uncovered (deleted_selected source audience time w deleted relevant)

/-- The result carries actual log receipts or snapshot coverage. Every retry
spends a previously undeleted target number; cross-audience skips are safe. -/
theorem scan_spec (source : Source order target) (audience start time : Nat)
    (afterStart : start ≤ time) (available : source.availableFrom ≤ time) (todo : List Write) (subset : todo ⊆ target) :
    match scan source.world source.history order audience (selected source audience start)
        (source.history start).deleted todo time with
    | .complete received =>
        (∀ w ∈ todo, Realized audience (selected source audience start) received w) ∧
        (∀ w ∈ received, ∃ t o, start ≤ t ∧ source.world t (.log .write w.writer w.number) = some o)
    | .retry next w => start < next ∧ w ∈ todo ∧
        w ∈ (source.history next).deleted ∧ w ∉ (source.history start).deleted
    | .uncertified _ => False := by
  induction todo generalizing time with
  | nil => simp [scan]
  | cons w rest ih =>
      have tailSubset : rest ⊆ target := fun v hv => subset (List.mem_cons_of_mem _ hv)
      simp only [scan, deleted_number source start w (subset (by simp))]
      by_cases already : w.number ≤ position (selected source audience start) w.writer ∨ w ∈ (source.history start).deleted
      · simp only [ite_eq_left already]
        have result := ih time afterStart available tailSubset
        cases h : scan source.world source.history order audience (selected source audience start)
            (source.history start).deleted rest time with
        | complete received =>
            simp only [h] at result ⊢
            refine ⟨?_, result.2⟩
            intro v hv relevant
            rcases List.mem_cons.mp hv with rfl | tail
            · exact Or.inl (already.elim id (fun hd => deleted_selected source audience start v hd relevant))
            · exact result.1 v tail relevant
        | retry next missing =>
            simp only [h] at result ⊢
            exact ⟨result.1, List.mem_cons_of_mem _ result.2.1, result.2.2⟩
        | uncertified missing => simp [h] at result
      · simp only [ite_eq_right already]
        have notDeleted : w ∉ (source.history start).deleted := fun h => already (Or.inr h)
        cases found : source.world time (.log .write w.writer w.number) with
        | some object =>
            have result := ih (time + 1) (by omega) (by omega) tailSubset
            cases h : scan source.world source.history order audience (selected source audience start)
                (source.history start).deleted rest (time + 1) with
            | complete received =>
                simp only [h] at result ⊢
                constructor
                · intro v hv relevant
                  rcases List.mem_cons.mp hv with rfl | tail
                  · exact Or.inr (by simp)
                  · rcases result.1 v tail relevant with covered | read
                    · exact Or.inl covered
                    · exact Or.inr (List.mem_cons_of_mem _ read)
                · intro v hv
                  rcases List.mem_cons.mp hv with rfl | tail
                  · exact ⟨time, object, afterStart, found⟩
                  · exact result.2 v tail
            | retry next missing =>
                simp only [h] at result ⊢
                exact ⟨result.1, List.mem_cons_of_mem _ result.2.1, result.2.2⟩
            | uncertified missing => simp [h] at result
        | none =>
            have deleted := source.absent time w available (subset (by simp)) found
            simp only [deleted_number source (time + 1) w (subset (by simp)), deleted, ↓reduceIte]
            by_cases covered : w.number ≤ position (selected source audience (time + 1)) w.writer
            · simp only [selected] at covered
              simp only [ite_eq_left covered]
              exact ⟨by omega, by simp, deleted, notDeleted⟩
            · have irrelevant := skipped_has_no_part source audience (time + 1) w deleted covered
              simp only [selected] at covered
              simp only [ite_eq_right covered]
              have result := ih (time + 1) (by omega) (by omega) tailSubset
              cases h : scan source.world source.history order audience (selected source audience start)
                  (source.history start).deleted rest (time + 1) with
              | complete received =>
                  simp only [h] at result ⊢
                  refine ⟨?_, result.2⟩
                  intro v hv relevant
                  rcases List.mem_cons.mp hv with rfl | tail
                  · exact False.elim (irrelevant relevant)
                  · exact result.1 v tail relevant
              | retry next missing =>
                  simp only [h] at result ⊢
                  exact ⟨result.1, List.mem_cons_of_mem _ result.2.1, result.2.2⟩
              | uncertified missing => simp [h] at result

def remaining (source : Source order target) (time : Nat) : List Write :=
  target.filter (fun w => !decide (w ∈ (source.history time).deleted))

theorem deletion_decreases (source : Source order target) (start next : Nat) (w : Write)
    (later : start ≤ next) (required : w ∈ target)
    (deleted : w ∈ (source.history next).deleted) (new : w ∉ (source.history start).deleted) :
    (remaining source next).length < (remaining source start).length := by
  have sub : (remaining source next).Sublist (remaining source start) := by
    have filterMono : ∀ xs : List Write,
        (xs.filter (fun w => !decide (w ∈ (source.history next).deleted))).Sublist
          (xs.filter (fun w => !decide (w ∈ (source.history start).deleted))) := by
      intro xs
      induction xs with
      | nil => simp
      | cons v vs ih =>
          by_cases hn : v ∈ (source.history next).deleted
          · by_cases hs : v ∈ (source.history start).deleted
            · simpa [hn, hs] using ih
            · simpa [hn, hs] using List.Sublist.cons v ih
          · have hs : v ∉ (source.history start).deleted :=
              fun h => hn (source.deletedMono start next later h)
            simpa [hn, hs] using List.Sublist.cons_cons v ih
    exact filterMono target
  have le := sub.length_le
  apply Nat.lt_of_not_ge
  intro no
  have eq := sub.eq_of_length (by omega)
  have old : w ∈ remaining source start := by simp [remaining, required, new]
  rw [← eq] at old
  simp [remaining, deleted] at old

inductive Loads (source : Source order target) (audience : Nat) : Nat → Nat → Nat → List Write → Prop
  | commit (start : Nat) (received : List Write) (completed : round source audience start = .complete received) :
      Loads source audience start 0 start received
  | retry (start next selectedAt retries : Nat) (w : Write) (received : List Write)
      (again : round source audience start = .retry next w)
      (rest : Loads source audience next retries selectedAt received) :
      Loads source audience start (retries + 1) selectedAt received

/-- Combined termination and committed-prefix theorem. Successful GETs, fresh
catalogs and snapshot bodies are finite operations; no duration bound is used.
The finite target is unchanged by every retry and by concurrent publication. -/
theorem terminates (source : Source order target) (audience start : Nat)
    (available : source.availableFrom ≤ start) :
    ∃ retries selectedAt received, Loads source audience start retries selectedAt received ∧
      retries ≤ (remaining source start).length ∧
      (∀ w ∈ target, Realized audience (selected source audience selectedAt) received w) ∧
      (∀ w ∈ received, ∃ t o, selectedAt ≤ t ∧ source.world t (.log .write w.writer w.number) = some o) := by
  generalize measure : (remaining source start).length = n
  induction n using Nat.strongRecOn generalizing start with
  | ind n ih =>
      have spec := scan_spec source audience start start (Nat.le_refl _) available target (fun _ h => h)
      change (match round source audience start with | .complete r => _ | .retry t w => _ | .uncertified _ => _) at spec
      cases h : round source audience start with
      | complete received =>
          simp only [h] at spec
          exact ⟨0, start, received, .commit start received h, by omega, spec⟩
      | uncertified w => simp [h] at spec
      | retry next w =>
          simp only [h] at spec
          have smaller := deletion_decreases source start next w (by omega) spec.2.1 spec.2.2.1 spec.2.2.2
          obtain ⟨retries, selectedAt, received, run, bound, complete⟩ :=
            ih (remaining source next).length (by omega) next (by omega) rfl
          exact ⟨retries + 1, selectedAt, received, .retry start next selectedAt retries w received h run,
            by omega, complete⟩

/-- Turn the coverage-or-receipt result into an actual snapshot or GET witness.
Number zero is not a published log object. -/
theorem committed_contains (source : Source order target) (audience start : Nat)
    (available : source.availableFrom ≤ start) :
    ∃ retries selectedAt received, Loads source audience start retries selectedAt received ∧
      retries ≤ target.length ∧ ∀ w ∈ target, 0 < w.number → audience ∈ w.audiences →
        (∃ snap, selected source audience selectedAt = some snap ∧ snap.covers w audience) ∨
        (w ∈ received ∧ ∃ t o, selectedAt ≤ t ∧ source.world t (.log .write w.writer w.number) = some o) := by
  obtain ⟨retries, selectedAt, received, run, bound, complete, receipts⟩ := terminates source audience start available
  refine ⟨retries, selectedAt, received, run,
    Nat.le_trans bound (List.length_filter_le _ _), ?_⟩
  intro w hw positive relevant
  rcases complete w hw relevant with covered | hit
  · left
    cases hs : selected source audience selectedAt with
    | none => simp [hs, position] at covered; omega
    | some snap =>
        have facts := current_member order (source.history selectedAt).landed audience snap hs
        exact ⟨snap, rfl, facts.2.1, by simpa [position, hs] using covered⟩
  · exact Or.inr ⟨hit, receipts w hit⟩

/-- Enumerating every published number through the frozen writer positions
turns the finite-list result into the per-writer prefix guarantee. Writer
registration discovery and unpublished local reservations are separate inputs. -/
theorem prefix_committed (source : Source order target) (audience start : Nat)
    (writers : List Nat) (limit : Nat → Nat) (available : source.availableFrom ≤ start)
    (enumerated : ∀ w, source.published w → w.writer ∈ writers → 0 < w.number →
      w.number ≤ limit w.writer → w ∈ target) :
    ∃ retries selectedAt received, Loads source audience start retries selectedAt received ∧
      retries ≤ target.length ∧ ∀ w, source.published w → w.writer ∈ writers →
        0 < w.number → w.number ≤ limit w.writer → audience ∈ w.audiences →
        (∃ snap, selected source audience selectedAt = some snap ∧ snap.covers w audience) ∨
        (w ∈ received ∧ ∃ t o, selectedAt ≤ t ∧ source.world t (.log .write w.writer w.number) = some o) := by
  obtain ⟨retries, selectedAt, received, run, bound, complete⟩ := committed_contains source audience start available
  exact ⟨retries, selectedAt, received, run, bound, fun w pub writer positive within relevant =>
    complete w (enumerated w pub writer positive within) positive relevant⟩

end CovenIO.FixedReload
