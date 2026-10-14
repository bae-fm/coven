import CovenStorage.ReplacementRead

/-! §10 as coven-sync judges it: each old-id write or entry is judged by the
replay of its own recorded past, not by one kept set shared by all objects.

Replay itself is a parameter. The store-log model proves that replaying a
closed set of entries gives one result whatever the arrival order
(`storelog_converges`); `SetDetermined` is that fact as this model uses it. -/
namespace CovenStorage.ReplacementRead

section Views

variable {Entry : Type} (path : Entry → Path) (replay : List Entry → List Registration)

/-- Replaying a closed set of entries depends on the set alone. -/
def SetDetermined : Prop :=
  ∀ a b : List Entry, (∀ e, e ∈ a ↔ e ∈ b) → ∀ r, r ∈ replay a ↔ r ∈ replay b

/-- Every entry the object recorded reading has arrived. Coven judges an
object only then; until then it waits for its causes. -/
def Ready (received : List Entry) (o : LogObject) : Prop :=
  ∀ p ∈ o.past, ∃ e ∈ received, path e = p

/-- Storage holds one object per path, so a path names one entry. -/
def OnePerPath (entries : List Entry) : Prop :=
  ∀ a ∈ entries, ∀ b ∈ entries, path a = path b → a = b

/-- The received entries the object recorded reading: its own view. -/
def view (received : List Entry) (o : LogObject) : List Entry :=
  received.filter fun e => decide (path e ∈ o.past)

/-- §10: an old-id object counts unless the replay of its own recorded past
keeps a replacement of its device. -/
def acceptsView (received : List Entry) (o : LogObject) : Bool :=
  (replay (view path received o)).all fun r => r.old != o.device

theorem acceptsView_iff (received : List Entry) (o : LogObject) :
    acceptsView path replay received o = true ↔
      ∀ r ∈ replay (view path received o), r.old ≠ o.device := by
  simp [acceptsView, List.all_eq_true]

/-- Rebuild the applied identities from retained received objects, each
judged in its own view. -/
def loadViews (received : List Entry) (objects : List LogObject) : Applied :=
  receiveAll [] ((objects.filter (acceptsView path replay received)).map LogObject.path)

theorem loadViews_iff (received : List Entry) (objects : List LogObject) (p : Path) :
    p ∈ loadViews path replay received objects ↔
      ∃ o ∈ objects, acceptsView path replay received o = true ∧ o.path = p := by
  simp [loadViews, receipt_members, List.mem_map, List.mem_filter, and_assoc]

theorem loadViews_nodup (received : List Entry) (objects : List LogObject) :
    (loadViews path replay received objects).Nodup := receipt_no_duplicates _ _ (by simp)

/-- Two devices holding the same entries, in any arrival order, give every
object the same verdict. -/
theorem view_order_irrelevant (hset : SetDetermined replay) (a b : List Entry)
    (same : ∀ e, e ∈ a ↔ e ∈ b) (o : LogObject) :
    acceptsView path replay a o = acceptsView path replay b o := by
  have hview : ∀ e, e ∈ view path a o ↔ e ∈ view path b o := by
    intro e; simp [view, same]
  apply Bool.eq_iff_iff.mpr
  simp only [acceptsView_iff]
  constructor
  · intro h r hr; exact h r ((hset _ _ hview r).mpr hr)
  · intro h r hr; exact h r ((hset _ _ hview r).mp hr)

/-- Once an object is ready, later arrivals add nothing to its view. -/
theorem view_stable (received later : List Entry) (o : LogObject)
    (ready : Ready path received o) (unique : OnePerPath path (received ++ later)) :
    ∀ e, e ∈ view path (received ++ later) o ↔ e ∈ view path received o := by
  intro e
  simp only [view, List.mem_filter, List.mem_append, decide_eq_true_eq]
  constructor
  · rintro ⟨he | he, hp⟩
    · exact ⟨he, hp⟩
    · obtain ⟨e', he', hpath⟩ := ready (path e) hp
      have : e' = e := unique e' (List.mem_append_left _ he') e
        (List.mem_append_right _ he) hpath
      exact ⟨this ▸ he', hp⟩
  · rintro ⟨he, hp⟩; exact ⟨Or.inl he, hp⟩

/-- No rejudging: a ready object's verdict never changes as more entries
arrive, even when the full replay then drops or keeps different replacements. -/
theorem verdict_never_rejudged (hset : SetDetermined replay) (received later : List Entry)
    (o : LogObject) (ready : Ready path received o)
    (unique : OnePerPath path (received ++ later)) :
    acceptsView path replay (received ++ later) o = acceptsView path replay received o := by
  have hview := view_stable path received later o ready unique
  apply Bool.eq_iff_iff.mpr
  simp only [acceptsView_iff]
  constructor
  · intro h r hr; exact h r ((hset _ _ hview r).mpr hr)
  · intro h r hr; exact h r ((hset _ _ hview r).mp hr)

/-- Devices with the same entries and objects apply the same identities. -/
theorem views_converge (hset : SetDetermined replay) (a b : List Entry)
    (xs ys : List LogObject) (entries : ∀ e, e ∈ a ↔ e ∈ b)
    (objects : ∀ o, o ∈ xs ↔ o ∈ ys) :
    (fun p => decide (p ∈ loadViews path replay a xs)) =
      (fun p => decide (p ∈ loadViews path replay b ys)) := by
  funext p
  simp only [loadViews_iff, objects, view_order_irrelevant path replay hset a b entries]

/-- An applied identity stays applied as entries and objects keep arriving:
each device's applied set only grows toward the common result. -/
theorem applied_never_retracted (hset : SetDetermined replay)
    (received later : List Entry) (objects more : List LogObject)
    (ready : ∀ o ∈ objects, Ready path received o)
    (unique : OnePerPath path (received ++ later)) (p : Path)
    (applied : p ∈ loadViews path replay received objects) :
    p ∈ loadViews path replay (received ++ later) (objects ++ more) := by
  obtain ⟨o, ho, ha, hp⟩ := (loadViews_iff path replay received objects p).mp applied
  refine (loadViews_iff path replay _ _ p).mpr ⟨o, List.mem_append_left _ ho, ?_, hp⟩
  rw [verdict_never_rejudged path replay hset received later o (ready o ho) unique]
  exact ha

/-- The copy that made an object had not stopped: its own replay kept no
replacement of its id. Every such object counts. A copy stops exactly when
its replay keeps one (§10), so admission and stopping use one judgement. -/
theorem unstopped_copy_counts (received : List Entry) (o : LogObject)
    (unstopped : ∀ r ∈ replay (view path received o), r.old ≠ o.device) :
    acceptsView path replay received o = true :=
  (acceptsView_iff path replay received o).mpr unstopped

/-- When every view keeps exactly the replacements of one kept set that it
read, the per-object rule is `accepts` with that set. -/
theorem agrees_with_kept_set (kept : List Registration) (received : List Entry)
    (o : LogObject)
    (consistent : ∀ r, r ∈ replay (view path received o) ↔ r ∈ kept ∧ r.entry ∈ o.past) :
    acceptsView path replay received o = accepts kept o := by
  apply Bool.eq_iff_iff.mpr
  rw [acceptsView_iff, accepts_iff]
  constructor
  · intro h r hr old read; exact h r ((consistent r).mpr ⟨hr, read⟩) old
  · intro h r hr old; exact h r ((consistent r).mp hr).1 old ((consistent r).mp hr).2

end Views

end CovenStorage.ReplacementRead
