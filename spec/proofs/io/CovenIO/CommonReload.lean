import CovenIO.AtomicReload
import CovenIO.SnapshotWrites

namespace CovenIO.CommonReload
open FixedReload

/-- Frontiers certify published numbers, never peer posted positions. The
finite prefix inventory and successful body validation are source obligations. -/
structure Source (order : SnapshotOrder) (writers : List Nat) where
  world : World
  history : Nat → SnapshotHistory.State
  published : Write → Prop
  inventory : (Nat → Nat) → List Write
  required : ∀ cut w, w ∈ inventory cut → published w
  within : ∀ cut w, w ∈ inventory cut → w.writer ∈ writers ∧ 0 < w.number ∧ w.number ≤ cut w.writer
  enumerated : ∀ cut w, published w → w.writer ∈ writers → 0 < w.number →
    w.number ≤ cut w.writer → w ∈ inventory cut
  unique : ∀ u v, published u → published v → u.writer = v.writer → u.number = v.number → u = v
  runs : ∀ t, SnapshotHistory.Run published order (history t)
  deletedMono : ∀ s t, s ≤ t → (history s).deleted ⊆ (history t).deleted
  publication : Nat → SnapshotWrites.State
  publicationRuns : ∀ t, SnapshotWrites.Run (publication t)
  catalogPublished : ∀ t snap, snap ∈ (history t).landed → snap ∈ (publication t).landed
  frontierMono : ∀ s t, s ≤ t → ∀ w ∈ writers,
    ((publication s).logs w).published.length ≤ ((publication t).logs w).published.length
  /-- Decode and validate the bytes supplied by the publication history. -/
  decoded : ∀ t writer, writer ∈ writers → ∀ number bytes, 0 < number →
    ((publication t).logs writer).published[number - 1]? = some bytes →
      ∃ w, published w ∧ w.writer = writer ∧ w.number = number
  absent : ∀ start cut, (∀ w ∈ writers, cut w ≤ ((publication start).logs w).published.length) →
    ∀ t w, start ≤ t → w ∈ inventory cut → world t (.log .write w.writer w.number) = none →
      w ∈ (history (t + 1)).deleted

def Source.frontier (source : Source order writers) (time writer : Nat) : Nat :=
  ((source.publication time).logs writer).published.length

/-- Derived from the publication history: a selected snapshot cannot carry
unconfirmed own positions, including when it is a breaking raise snapshot. -/
theorem Source.selectedBound (source : Source order writers) (t a w : Nat) :
    position (catalogCurrent order (source.history t).landed a) w ≤ source.frontier t w := by
  cases selected : catalogCurrent order (source.history t).landed a with
  | none => simp [position]
  | some snap =>
      have member := (current_member order (source.history t).landed a snap selected).1
      exact (SnapshotWrites.run_valid (source.publicationRuns t)).2.2.2 snap
        (source.catalogPublished t snap member) w

theorem Source.numbered (source : Source order writers) (t writer : Nat)
    (known : writer ∈ writers) (number : Nat) (positive : 0 < number)
    (within : number ≤ source.frontier t writer) :
    ∃ w, source.published w ∧ w.writer = writer ∧ w.number = number := by
  have index : number - 1 < ((source.publication t).logs writer).published.length := by
    change number ≤ ((source.publication t).logs writer).published.length at within
    omega
  exact source.decoded t writer known number
    ((source.publication t).logs writer).published[number - 1] positive (by simp [index])

def Source.fixed (source : Source order writers) (cut : Nat → Nat) (start : Nat)
    (ready : ∀ w ∈ writers, cut w ≤ source.frontier start w) : FixedReload.Source order (source.inventory cut) where
  world := source.world
  history := source.history
  published := source.published
  required := source.required cut
  unique := source.unique
  runs := source.runs
  deletedMono := source.deletedMono
  availableFrom := start
  absent := source.absent start cut ready

/-- Selections survive rounds. Only a deleted hole replaces its audience. -/
structure State where
  time : Nat
  selectedAt : Nat → Nat

def selected (source : Source order writers) (s : State) (a : Nat) : Option Snapshot :=
  catalogCurrent order (source.history (s.selectedAt a)).landed a

def maximum (audiences : List Nat) (values : Nat → Nat) : Nat :=
  (audiences.map values).foldr max 0

def target (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (writer : Nat) : Nat :=
  max (required writer) (maximum audiences (fun a => position (selected source s a) writer))

theorem maximum_member (audiences : List Nat) (values : Nat → Nat) (a : Nat) (member : a ∈ audiences) :
    values a ≤ maximum audiences values := by
  induction audiences with
  | nil => simp at member
  | cons b rest ih =>
      rcases List.mem_cons.mp member with rfl | tail
      · exact Nat.le_max_left _ _
      · exact Nat.le_trans (ih tail) (Nat.le_max_right _ _)

theorem maximum_le (audiences : List Nat) (values : Nat → Nat) (bound : Nat)
    (bounded : ∀ a ∈ audiences, values a ≤ bound) : maximum audiences values ≤ bound := by
  induction audiences with
  | nil => exact Nat.zero_le _
  | cons a rest ih =>
      exact Nat.max_le.mpr ⟨bounded a (by simp), ih (fun b hb => bounded b (by simp [hb]))⟩

def Valid (source : Source order writers) (audiences : List Nat) (required : Nat → Nat) (s : State) : Prop :=
  (∀ a ∈ audiences, s.selectedAt a ≤ s.time) ∧
    ∀ w ∈ writers, required w ≤ source.frontier s.time w

theorem target_ready (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (valid : Valid source audiences required s) :
    ∀ w ∈ writers, target source audiences required s w ≤ source.frontier s.time w := by
  intro w hw
  apply Nat.max_le.mpr
  refine ⟨valid.2 w hw, maximum_le _ _ _ ?_⟩
  intro a ha
  exact Nat.le_trans (source.selectedBound _ a w)
    (source.frontierMono _ _ (valid.1 a ha) w hw)

inductive Result where
  | complete (receipts : List (Nat × List Write))
  | retry (audience time : Nat) (missing : Write)
  | uncertified (audience : Nat) (missing : Write)
  deriving DecidableEq, Repr

/-- Finite audience scans may run concurrently at the same cut. Abandoned
receipts are never installed, and no scan follows a growing tail. -/
def scan (source : Source order writers) (s : State) (cut : Nat → Nat) : List Nat → Result
  | [] => .complete []
  | a :: rest =>
      match FixedReload.scan source.world source.history order a (selected source s a)
        (source.history (s.selectedAt a)).deleted (source.inventory cut) s.time with
      | .complete received => match scan source s cut rest with
          | .complete receipts => .complete ((a, received) :: receipts)
          | other => other
      | .retry next missing => .retry a next missing
      | .uncertified missing => .uncertified a missing

def replace (s : State) (a next : Nat) : State :=
  ⟨next, fun b => if b = a then next else s.selectedAt b⟩

def round (source : Source order writers) (audiences : List Nat) (required : Nat → Nat) (s : State) : Result :=
  scan source s (target source audiences required s) audiences

/-- Actual coverage or retained successful GETs, not only position labels. -/
def Complete (source : Source order writers) (audiences : List Nat) (s : State)
    (cut : Nat → Nat) (receipts : List (Nat × List Write)) : Prop :=
  ∀ a ∈ audiences, ∃ received, (a, received) ∈ receipts ∧ received ⊆ source.inventory cut ∧
    (∀ w ∈ source.inventory cut, Realized a (selected source s a) received w) ∧
    ∀ w ∈ received, ∃ t o, s.selectedAt a ≤ t ∧ source.world t (.log .write w.writer w.number) = some o

theorem scan_spec (source : Source order writers) (audiences : List Nat) (s : State) (cut : Nat → Nat)
    (times : ∀ a ∈ audiences, s.selectedAt a ≤ s.time)
    (ready : ∀ w ∈ writers, cut w ≤ source.frontier s.time w) :
    match scan source s cut audiences with
    | .complete receipts => Complete source audiences s cut receipts
    | .retry a next w => a ∈ audiences ∧ s.time < next ∧
        w ∈ (source.history next).deleted ∧ w ∉ (source.history (s.selectedAt a)).deleted
    | .uncertified _ _ => False := by
  induction audiences with
  | nil => simp [scan, Complete]
  | cons a rest ih =>
      let fixed := source.fixed cut s.time ready
      have spec := FixedReload.scan_spec fixed a (s.selectedAt a) s.time (times a (by simp))
        (Nat.le_refl _) (source.inventory cut) (fun _ h => h)
      change (match FixedReload.scan source.world source.history order a (selected source s a)
        (source.history (s.selectedAt a)).deleted (source.inventory cut) s.time with
        | .complete r => _ | .retry t w => _ | .uncertified _ => _) at spec
      have tail := ih (fun b hb => times b (by simp [hb]))
      simp only [scan]
      cases head : FixedReload.scan source.world source.history order a (selected source s a)
          (source.history (s.selectedAt a)).deleted (source.inventory cut) s.time with
      | uncertified w => simp [head] at spec
      | retry next w =>
          simp only [head] at spec ⊢
          have clock := FixedReload.scan_retry_time _ _ _ _ _ _ _ _ _ _ head
          exact ⟨by simp, clock, spec.2.2⟩
      | complete received =>
          simp only [head] at spec ⊢
          cases restResult : scan source s cut rest with
          | uncertified b w => simp [restResult] at tail
          | retry b next w =>
              simp only [restResult] at tail ⊢
              exact ⟨by simp [tail.1], tail.2⟩
          | complete receipts =>
              simp only [restResult] at tail ⊢
              intro b hb
              rcases List.mem_cons.mp hb with rfl | member
              · exact ⟨received, by simp, FixedReload.scan_received_subset _ _ _ _ _ _ _ _ _ head, spec⟩
              · obtain ⟨got, member, subset, realized, read⟩ := tail b member
                exact ⟨got, by simp [member], subset, realized, read⟩

/-- Deletion cessation constrains neither writes nor snapshot publication. -/
def Quiescent (source : Source order writers) (stop : Nat) : Prop :=
  ∀ t, stop ≤ t → (source.history t).deleted = (source.history stop).deleted

def stale (audiences : List Nat) (s : State) (stop : Nat) : List Nat :=
  audiences.filter (fun a => decide (s.selectedAt a < stop))

theorem retry_stale (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (stop : Nat) (quiet : Quiescent source stop) (afterStop : stop ≤ s.time)
    (valid : Valid source audiences required s) (a next : Nat) (w : Write)
    (again : round source audiences required s = .retry a next w) :
    a ∈ stale audiences s stop ∧ s.time < next := by
  have spec := scan_spec source audiences s _ valid.1 (target_ready source audiences required s valid)
  change (match round source audiences required s with | .complete r => _ | .retry a t w => _ | .uncertified a w => _) at spec
  rw [again] at spec
  have old : s.selectedAt a < stop := by
    apply Nat.lt_of_not_ge
    intro h
    have eq := quiet (s.selectedAt a) (by omega)
    have eqNext := quiet next (by omega)
    exact spec.2.2.2 (by rw [eq, ← eqNext]; exact spec.2.2.1)
  exact ⟨by simp [stale, spec.1, old], spec.2.1⟩

theorem replace_valid (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (valid : Valid source audiences required s) (a next : Nat) (later : s.time ≤ next) :
    Valid source audiences required (replace s a next) := by
  constructor
  · intro b hb
    simp only [replace]
    split
    · exact Nat.le_refl _
    · exact Nat.le_trans (valid.1 b hb) later
  · intro w hw
    exact Nat.le_trans (valid.2 w hw) (source.frontierMono _ _ later w hw)

theorem stale_decreases (audiences : List Nat) (s : State) (stop a next : Nat)
    (member : a ∈ stale audiences s stop) (later : stop ≤ next) :
    (stale audiences (replace s a next) stop).length < (stale audiences s stop).length := by
  have eq : stale audiences (replace s a next) stop =
      (stale audiences s stop).filter (fun b => b != a) := by
    simp only [stale, List.filter_filter]
    apply List.filter_congr
    intro b _
    by_cases h : b = a <;> simp [replace, h, show ¬ next < stop by omega]
  rw [eq]
  have le := List.length_filter_le (fun b => b != a) (stale audiences s stop)
  have ne : ((stale audiences s stop).filter (fun b => b != a)).length ≠
      (stale audiences s stop).length := by
    intro equal
    have h := (List.length_filter_eq_length_iff.mp equal) a member
    simp at h
  omega

inductive Loads (source : Source order writers) (audiences : List Nat) (required : Nat → Nat) :
    State → Nat → State → List (Nat × List Write) → Prop
  | commit (s : State) (receipts : List (Nat × List Write))
      (completed : round source audiences required s = .complete receipts) :
      Loads source audiences required s 0 s receipts
  | retry (s last : State) (a next retries : Nat) (w : Write) (receipts : List (Nat × List Write))
      (again : round source audiences required s = .retry a next w)
      (rest : Loads source audiences required (replace s a next) retries last receipts) :
      Loads source audiences required s (retries + 1) last receipts

/-- Safety holds for every completed execution, including while deletion
continues. Deletion cessation is needed only to establish termination. -/
theorem loads_complete (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s last : State) (retries : Nat) (receipts : List (Nat × List Write))
    (run : Loads source audiences required s retries last receipts)
    (valid : Valid source audiences required s) :
    Valid source audiences required last ∧
      Complete source audiences last (target source audiences required last) receipts := by
  induction run with
  | commit s receipts completed =>
      have spec := scan_spec source audiences s _ valid.1 (target_ready source audiences required s valid)
      change (match round source audiences required s with | .complete r => _ | .retry a t w => _ | .uncertified a w => _) at spec
      rw [completed] at spec
      exact ⟨valid, spec⟩
  | retry s last a next retries w receipts again rest ih =>
      have spec := scan_spec source audiences s _ valid.1 (target_ready source audiences required s valid)
      change (match round source audiences required s with | .complete r => _ | .retry a t w => _ | .uncertified a w => _) at spec
      rw [again] at spec
      exact ih (replace_valid source audiences required s valid a next (by have := spec.2.1; omega))

/-- Install exactly the cut justified by a completed execution. All waiting
remote pasts are satisfied, independently of any termination assumption. -/
theorem committed_safe (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s last : State) (retries : Nat) (receipts : List (Nat × List Write))
    (run : Loads source audiences required s retries last receipts)
    (valid : Valid source audiences required s) (old : AtomicReload.View)
    (waiting : List (Nat → Nat)) (pastBound : ∀ past ∈ waiting, ∀ w ∈ writers, past w ≤ required w) :
    Complete source audiences last (target source audiences required last) receipts ∧
      (∀ a ∈ audiences, ∀ w ∈ writers,
        position (selected source last a) w ≤ target source audiences required last w) ∧
      let view : AtomicReload.View := fun _ => target source audiences required last
      AtomicReload.commit ⟨old, some view⟩ = some ⟨view, none⟩ ∧
      AtomicReload.CommonPoint audiences writers view ∧
      ∀ past ∈ waiting, AtomicReload.WaitingReady audiences writers view past := by
  refine ⟨(loads_complete source audiences required s last retries receipts run valid).2, ?_, rfl, ?_, ?_⟩
  · intro a ha w _
    exact Nat.le_trans (maximum_member audiences (fun a => position (selected source last a) w) a ha) (Nat.le_max_right _ _)
  · intro a _ b _ w _; rfl
  · intro past member a _ w hw
    exact Nat.le_trans (pastBound past member w hw) (Nat.le_max_left _ _)

/-- At most one replacement per stale audience after the last deletion,
even with unbounded new writes and snapshots. Each round is a finite scan. -/
theorem terminates (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (stop : Nat) (quiet : Quiescent source stop) (afterStop : stop ≤ s.time)
    (valid : Valid source audiences required s) :
    ∃ retries last receipts, Loads source audiences required s retries last receipts ∧
      retries ≤ (stale audiences s stop).length ∧
      Complete source audiences last (target source audiences required last) receipts := by
  generalize measure : (stale audiences s stop).length = n
  induction n using Nat.strongRecOn generalizing s with
  | ind n ih =>
      have spec := scan_spec source audiences s _ valid.1 (target_ready source audiences required s valid)
      change (match round source audiences required s with | .complete r => _ | .retry a t w => _ | .uncertified a w => _) at spec
      cases result : round source audiences required s with
      | uncertified a w => simp [result] at spec
      | complete receipts =>
          rw [result] at spec
          exact ⟨0, s, receipts, .commit s receipts result, by omega, spec⟩
      | retry a next w =>
          obtain ⟨member, later⟩ := retry_stale source audiences required s stop quiet afterStop valid a next w result
          have decreases := stale_decreases audiences s stop a next member (by omega)
          obtain ⟨retries, last, receipts, run, bound, complete⟩ :=
            ih _ (by omega) (replace s a next) (by dsimp [replace]; omega)
              (replace_valid source audiences required s valid a next (by omega)) rfl
          exact ⟨retries + 1, last, receipts, .retry s last a next retries w receipts result run, by omega, complete⟩

/-- Starting before the last deletion also terminates. The first term counts
remaining observation events before cessation; thereafter only stale audiences
can force replacements. Storage timestamps need not advance during a round. -/
theorem eventually_terminates (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (stop : Nat) (quiet : Quiescent source stop)
    (valid : Valid source audiences required s) :
    ∃ retries last receipts, Loads source audiences required s retries last receipts ∧
      retries ≤ (stop - s.time) + audiences.length ∧
      Complete source audiences last (target source audiences required last) receipts := by
  generalize measure : stop - s.time = n
  induction n using Nat.strongRecOn generalizing s with
  | ind n ih =>
      by_cases afterStop : stop ≤ s.time
      · obtain ⟨retries, last, receipts, run, bound, complete⟩ :=
          terminates source audiences required s stop quiet afterStop valid
        have le := List.length_filter_le (fun a => decide (s.selectedAt a < stop)) audiences
        exact ⟨retries, last, receipts, run, by dsimp [stale] at bound; omega, complete⟩
      · have spec := scan_spec source audiences s _ valid.1 (target_ready source audiences required s valid)
        change (match round source audiences required s with | .complete r => _ | .retry a t w => _ | .uncertified a w => _) at spec
        cases result : round source audiences required s with
        | uncertified a w => simp [result] at spec
        | complete receipts =>
            rw [result] at spec
            exact ⟨0, s, receipts, .commit s receipts result, Nat.zero_le _, spec⟩
        | retry a next w =>
            rw [result] at spec
            have later := spec.2.1
            obtain ⟨retries, last, receipts, run, bound, complete⟩ :=
              ih (stop - next) (by omega) (replace s a next)
                (replace_valid source audiences required s valid a next (by omega)) rfl
            exact ⟨retries + 1, last, receipts, .retry s last a next retries w receipts result run,
              by omega, complete⟩

/-- Assemble the cut using snapshots and log receipts, then expose it in one
transaction. Every waiting write's recorded remote past is bounded at entry. -/
theorem common_cut_commits (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s : State) (stop : Nat) (quiet : Quiescent source stop) (afterStop : stop ≤ s.time)
    (valid : Valid source audiences required s) (old : AtomicReload.View)
    (waiting : List (Nat → Nat)) (pastBound : ∀ past ∈ waiting, ∀ w ∈ writers, past w ≤ required w) :
    ∃ retries last receipts, Loads source audiences required s retries last receipts ∧
      retries ≤ audiences.length ∧
      Complete source audiences last (target source audiences required last) receipts ∧
      (∀ a ∈ audiences, ∀ w ∈ writers,
        position (selected source last a) w ≤ target source audiences required last w) ∧
      let view : AtomicReload.View := fun _ => target source audiences required last
      AtomicReload.commit ⟨old, some view⟩ = some ⟨view, none⟩ ∧
      AtomicReload.CommonPoint audiences writers view ∧
      ∀ past ∈ waiting, AtomicReload.WaitingReady audiences writers view past := by
  obtain ⟨retries, last, receipts, run, bound, _⟩ :=
    terminates source audiences required s stop quiet afterStop valid
  exact ⟨retries, last, receipts, run, Nat.le_trans bound (List.length_filter_le _ _),
    committed_safe source audiences required s last retries receipts run valid old waiting pastBound⟩

/-- The completeness certificate also covers every published prefix write,
not merely the inventory's own members. -/
theorem committed_prefix (source : Source order writers) (audiences : List Nat) (s : State)
    (cut : Nat → Nat) (receipts : List (Nat × List Write))
    (complete : Complete source audiences s cut receipts)
    (a : Nat) (ha : a ∈ audiences) (w : Write) (pub : source.published w)
    (writer : w.writer ∈ writers) (positive : 0 < w.number) (within : w.number ≤ cut w.writer)
    (relevant : a ∈ w.audiences) :
    w.number ≤ position (selected source s a) w.writer ∨
      ∃ received t o, (a, received) ∈ receipts ∧ w ∈ received ∧
        source.world t (.log .write w.writer w.number) = some o := by
  obtain ⟨received, member, _, realized, reads⟩ := complete a ha
  rcases realized w (source.enumerated cut w pub writer positive within) relevant with covered | read
  · exact Or.inl covered
  · obtain ⟨t, o, _, hit⟩ := reads w read
    exact Or.inr ⟨received, t, o, member, read, hit⟩

/-- Every positive number at the committed cut names a real published write;
there can be no unenumerated hole hidden behind the common position labels. -/
theorem committed_number (source : Source order writers) (audiences : List Nat) (required : Nat → Nat)
    (s last : State) (retries : Nat) (receipts : List (Nat × List Write))
    (run : Loads source audiences required s retries last receipts)
    (valid : Valid source audiences required s) (writer : Nat) (hw : writer ∈ writers)
    (number : Nat) (positive : 0 < number) (within : number ≤ target source audiences required last writer) :
    ∃ w, source.published w ∧ w.writer = writer ∧ w.number = number ∧
      ∀ a ∈ audiences, a ∈ w.audiences →
        number ≤ position (selected source last a) writer ∨
        ∃ received t o, (a, received) ∈ receipts ∧ w ∈ received ∧
          source.world t (.log .write writer number) = some o := by
  obtain ⟨lastValid, complete⟩ := loads_complete source audiences required s last retries receipts run valid
  have bound := target_ready source audiences required last lastValid writer hw
  obtain ⟨w, pub, writerEq, numberEq⟩ := source.numbered last.time writer hw number positive (by omega)
  refine ⟨w, pub, writerEq, numberEq, ?_⟩
  intro a ha relevant
  have result := committed_prefix source audiences last _ receipts complete a ha w pub
    (writerEq ▸ hw) (numberEq ▸ positive) (by simpa [writerEq, numberEq] using within) relevant
  simpa [writerEq, numberEq] using result

/-- A bundle already at the desired cut requires no log GETs. If it is
behind the waiting past, the ordinary common-cut scan still applies. -/
theorem bundle_complete (source : Source order writers) (audiences : List Nat) (s : State)
    (cut : Nat → Nat)
    (bundle : ∀ a ∈ audiences, ∀ w ∈ writers, position (selected source s a) w = cut w) :
    scan source s cut audiences = .complete (audiences.map (fun a => (a, []))) := by
  induction audiences with
  | nil => rfl
  | cons a rest ih =>
      have covered := FixedReload.scan_covered source.world source.history order a (selected source s a)
        (source.history (s.selectedAt a)).deleted (source.inventory cut) s.time (by
          intro w hw
          have facts := source.within cut w hw
          rw [bundle a (by simp) w.writer facts.1]
          exact facts.2.2)
      simp only [scan, covered]
      rw [ih (fun b hb => bundle b (by simp [hb]))]
      rfl

end CovenIO.CommonReload
