namespace CovenIO.Budget

/-! §17.8: each namespace keeps its cached files and chunks within the budget
the app declares for it. Pinned copies don't count toward it: only unpinned
entries' bytes are measured. Over budget, it evicts the least recently read entry
it may, then the next, until it is within budget or nothing more may go. A
pinned entry never goes, and a file's header stays while any chunk of the file
is cached, since chunks need it for offline opens. Eviction in one namespace
never touches another. It is a function of the entries, so it never fails a
read: the read that went over budget commits, and the eviction follows.

An unpin removes the entries of the unpinned files' bytes, and no other. Clearing
a namespace's cache removes every unpinned entry of the namespace, and no other.

This mirrors `file_cache::trim_entries`, which orders by the last read and
then the cache file's name. -/

/-- One cache entry: a cached range of chunks, or a file's header. `space` is
its namespace, `object` the stored file it is part of, and `name` its cache
file's name, unique among entries. `pinned` marks a pinned whole copy: the
cache stores no flag, since a whole copy is the pinned one. -/
structure Entry where
  space : Nat
  name : Nat
  object : Nat
  header : Bool
  size : Nat
  lastRead : Nat
  pinned : Bool
  deriving DecidableEq, Repr

/-- The bytes a namespace's unpinned entries hold: what counts toward its
budget. A pinned entry's bytes don't. -/
def used (ns : Nat) (es : List Entry) : Nat :=
  ((es.filter fun e => e.space == ns && !e.pinned).map (·.size)).sum

/-- An entry the budget may evict: in the namespace, unpinned, and not a
header whose file still has a chunk cached. A pinned entry is a whole copy,
which is neither a header nor a chunk. -/
def evictable (es : List Entry) (ns : Nat) (e : Entry) : Bool :=
  e.space == ns && !e.pinned &&
    (!e.header || !es.any fun c =>
      c.space == ns && c.object == e.object && !c.header && !c.pinned)

/-- Read earlier, or read at once with the smaller name. -/
def before (a b : Entry) : Prop :=
  a.lastRead < b.lastRead ∨ (a.lastRead = b.lastRead ∧ a.name ≤ b.name)

instance (a b : Entry) : Decidable (before a b) := inferInstanceAs (Decidable (_ ∨ _))

/-- The least recently read entry of a list. -/
def oldest : List Entry → Option Entry
  | [] => none
  | e :: es => match oldest es with
    | none => some e
    | some o => if before e o then some e else some o

theorem oldest_none' (es : List Entry) (none_ : oldest es = none) : es = [] := by
  cases es with
  | nil => rfl
  | cons x xs =>
    cases tail : oldest xs with
    | none => simp [oldest, tail] at none_
    | some y => by_cases xy : before x y <;> simp [oldest, tail, xy] at none_

theorem oldest_min (es : List Entry) (o : Entry) (chosen : oldest es = some o) :
    o ∈ es ∧ ∀ e ∈ es, before o e := by
  induction es generalizing o with
  | nil => cases chosen
  | cons x xs ih =>
    cases tail : oldest xs with
    | none =>
      simp only [oldest, tail, Option.some.injEq] at chosen
      subst chosen
      have empty : xs = [] := oldest_none' xs tail
      subst empty
      refine ⟨by simp, fun e he => ?_⟩
      simp only [List.mem_singleton] at he
      rw [he]
      exact Or.inr ⟨rfl, Nat.le_refl _⟩
    | some y =>
      have rest := ih y tail
      by_cases xy : before x y
      · simp only [oldest, tail, xy, ite_true, Option.some.injEq] at chosen
        subst chosen
        refine ⟨by simp, fun e he => ?_⟩
        rcases List.mem_cons.mp he with rfl | he
        · exact Or.inr ⟨rfl, Nat.le_refl _⟩
        · have := rest.2 e he
          unfold before at xy this ⊢
          omega
      · simp only [oldest, tail, xy, ite_false, Option.some.injEq] at chosen
        subst chosen
        refine ⟨List.mem_cons_of_mem _ rest.1, fun e he => ?_⟩
        rcases List.mem_cons.mp he with rfl | he
        · unfold before at xy ⊢
          omega
        · exact rest.2 e he

/-- Evict from namespace `ns` down to `budget`, with `fuel` steps at most:
the entries kept, and those evicted in order. -/
def trim (budget ns : Nat) : Nat → List Entry → List Entry × List Entry
  | 0, es => (es, [])
  | fuel + 1, es =>
    if used ns es ≤ budget then (es, []) else
      match oldest (es.filter (evictable es ns)) with
      | none => (es, [])
      | some e => let rest := trim budget ns fuel (es.erase e); (rest.1, e :: rest.2)

/-- Each step evicts one entry, so the entries' count is enough steps. -/
def evict (budget ns : Nat) (es : List Entry) : List Entry × List Entry :=
  trim budget ns es.length es

theorem trim_step (budget ns fuel : Nat) (es : List Entry) :
    trim budget ns (fuel + 1) es =
      if used ns es ≤ budget then (es, []) else
        match oldest (es.filter (evictable es ns)) with
        | none => (es, [])
        | some e =>
          let rest := trim budget ns fuel (es.erase e); (rest.1, e :: rest.2) := by
  rw [trim]

theorem trim_kept_sub (budget ns : Nat) :
    ∀ fuel es, ∀ x ∈ (trim budget ns fuel es).1, x ∈ es
  | 0, _, _, h => h
  | fuel + 1, es, x, h => by
    simp only [trim] at h
    split at h
    · exact h
    · split at h
      · exact h
      · exact List.mem_of_mem_erase (trim_kept_sub budget ns fuel _ x h)

/-- **Within budget, or nothing more may go.** -/
theorem within_budget (budget ns : Nat) :
    ∀ fuel es, es.length ≤ fuel →
      used ns (trim budget ns fuel es).1 ≤ budget ∨
        ((trim budget ns fuel es).1.filter (evictable (trim budget ns fuel es).1 ns)) = []
  | 0, es, short => by
    have : es = [] := List.eq_nil_of_length_eq_zero (by omega)
    subst this
    left; simp [trim, used]
  | fuel + 1, es, short => by
    simp only [trim]
    split
    · left; assumption
    · split
      · rename_i chosen
        right
        exact oldest_none' _ chosen
      · rename_i e chosen
        have member := (List.mem_filter.mp (oldest_min _ e chosen).1).1
        apply within_budget budget ns fuel
        have := List.length_erase_of_mem member
        omega

/-- **A pinned entry is never evicted.** -/
theorem pinned_kept (budget ns : Nat) :
    ∀ fuel es, ∀ x ∈ es, x.pinned = true → x ∈ (trim budget ns fuel es).1
  | 0, _, _, h, _ => h
  | fuel + 1, es, x, h, pinned => by
    simp only [trim]
    split
    · exact h
    · split
      · exact h
      · rename_i e chosen
        have ev := (List.mem_filter.mp (oldest_min _ e chosen).1).2
        have ne : x ≠ e := by
          intro eq; subst eq
          simp [evictable, pinned] at ev
        exact pinned_kept budget ns fuel _ x ((List.mem_erase_of_ne ne).mpr h) pinned

/-- A pinned entry is not a chunk, so it changes which entries are evictable
by none. -/
theorem evictable_pinned_cons (ns : Nat) (p : Entry) (es : List Entry)
    (pinned : p.pinned = true) (e : Entry) :
    evictable (p :: es) ns e = evictable es ns e := by
  simp [evictable, List.any_cons, pinned]

/-- Pinned entries don't count toward the budget. -/
theorem used_pinned_cons (ns : Nat) (p : Entry) (es : List Entry)
    (pinned : p.pinned = true) : used ns (p :: es) = used ns es := by
  simp [used, pinned]

/-- **A pinned entry crowds nothing out.** With room for the steps, evicting
from a namespace that holds a pinned entry evicts exactly what evicting
without it does, and keeps the pinned entry. -/
theorem trim_pinned_crowds_nothing (budget ns : Nat) (p : Entry) (pinned : p.pinned = true) :
    ∀ fuel es, es.length ≤ fuel →
      trim budget ns (fuel + 1) (p :: es) =
        (p :: (trim budget ns fuel es).1, (trim budget ns fuel es).2)
  | fuel, es, short => by
    have same : evictable (p :: es) ns = evictable es ns :=
      funext (evictable_pinned_cons ns p es pinned)
    have candidates :
        (p :: es).filter (evictable (p :: es) ns) = es.filter (evictable es ns) := by
      rw [List.filter_cons, same]
      simp [evictable, pinned]
    have lift : ∀ e, e ∈ es → e.pinned = false → (p :: es).erase e = p :: es.erase e := by
      intro e _ unpinned
      have ne : (p == e) = false := by
        cases h : (p == e)
        · rfl
        · have : p = e := beq_iff_eq.mp h
          subst this; simp [pinned] at unpinned
      exact List.erase_cons_tail (by simp [ne])
    by_cases under : used ns es ≤ budget
    · cases fuel <;> simp [trim, used_pinned_cons ns p es pinned, under]
    · have over : ¬ used ns (p :: es) ≤ budget := by rwa [used_pinned_cons ns p es pinned]
      cases fuel with
      | zero =>
        have empty : es = [] := List.eq_nil_of_length_eq_zero (by omega)
        subst empty
        simp [trim, over, candidates, oldest]
      | succ fuel =>
        rw [trim_step budget ns (fuel + 1) (p :: es), trim_step budget ns fuel es]
        simp only [over, under, ite_false, candidates]
        cases chosen : oldest (es.filter (evictable es ns)) with
        | none => rfl
        | some e =>
          have member := (List.mem_filter.mp (oldest_min _ e chosen).1)
          have unpinned : e.pinned = false := by
            have := member.2
            simp [evictable] at this
            exact this.1.2
          dsimp only
          rw [lift e member.1 unpinned]
          have shorter : (es.erase e).length ≤ fuel := by
            have := List.length_erase_of_mem member.1
            omega
          have ih := trim_pinned_crowds_nothing budget ns p pinned fuel (es.erase e) shorter
          simp only [ih]

/-- **A pinned entry crowds nothing out.** Evicting from `p :: es` with `p`
pinned evicts exactly what evicting from `es` does, and keeps `p`. -/
theorem pinned_crowds_nothing (budget ns : Nat) (p : Entry) (pinned : p.pinned = true)
    (es : List Entry) :
    evict budget ns (p :: es) =
      (p :: (evict budget ns es).1, (evict budget ns es).2) :=
  trim_pinned_crowds_nothing budget ns p pinned es.length es (Nat.le_refl _)

/-- **Eviction in one namespace never touches another.** -/
theorem other_namespace_kept (budget ns : Nat) :
    ∀ fuel es, ∀ x ∈ es, x.space ≠ ns → x ∈ (trim budget ns fuel es).1
  | 0, _, _, h, _ => h
  | fuel + 1, es, x, h, other => by
    simp only [trim]
    split
    · exact h
    · split
      · exact h
      · rename_i e chosen
        have ev := (List.mem_filter.mp (oldest_min _ e chosen).1).2
        have ne : x ≠ e := by
          intro eq; subst eq
          simp [evictable, other] at ev
        exact other_namespace_kept budget ns fuel _ x ((List.mem_erase_of_ne ne).mpr h) other

/-- Within budget, nothing goes. -/
theorem within_budget_keeps_all (budget ns fuel : Nat) (es : List Entry)
    (under : used ns es ≤ budget) : trim budget ns fuel es = (es, []) := by
  cases fuel with
  | zero => rfl
  | succ fuel => simp [trim, under]

/-- **The least recently read go first.** Every evicted entry was read no later
than every chunk of the namespace that stays unpinned. -/
theorem least_recent_first (budget ns : Nat) :
    ∀ fuel es, ∀ x ∈ (trim budget ns fuel es).2, ∀ y ∈ (trim budget ns fuel es).1,
      y.space = ns → y.pinned = false → y.header = false → before x y
  | 0, _, x, h, _, _, _, _, _ => by simp [trim] at h
  | fuel + 1, es, x, hx, y, hy, ns_, unpinned, chunk => by
    by_cases under : used ns es ≤ budget
    · simp [trim, under] at hx
    · cases chosen : oldest (es.filter (evictable es ns)) with
      | none => simp [trim, under, chosen] at hx
      | some e =>
        simp only [trim, under, chosen, ite_false] at hx hy
        rcases List.mem_cons.mp hx with rfl | later
        · have inEs := List.mem_of_mem_erase (trim_kept_sub budget ns fuel _ y hy)
          have ev : evictable es ns y = true := by simp [evictable, ns_, unpinned, chunk]
          exact (oldest_min _ x chosen).2 y (List.mem_filter.mpr ⟨inEs, ev⟩)
        · exact least_recent_first budget ns fuel _ x later y hy ns_ unpinned chunk

/-- §17.8: the app's unpin of stored files, named by their namespace and
object. It removes every entry of their bytes, pinned whole copy and cached
chunks and headers alike, and nothing else: no trim follows, since removing
bytes cannot put a namespace over its budget. -/
def unpin (targets : List (Nat × Nat)) (es : List Entry) : List Entry :=
  es.filter fun e => !targets.contains (e.space, e.object)

/-- **Unpinning removes the files' entries.** -/
theorem unpin_removes (targets : List (Nat × Nat)) (es : List Entry) :
    ∀ e ∈ unpin targets es, (e.space, e.object) ∉ targets := by
  intro e he hin
  simp [unpin] at he
  exact he.2 hin

/-- **Unpinning evicts nothing else, in any namespace.** -/
theorem unpin_keeps_others (targets : List (Nat × Nat)) (es : List Entry) :
    ∀ x ∈ es, (x.space, x.object) ∉ targets → x ∈ unpin targets es := by
  intro x hx hout
  simp [unpin]
  exact ⟨hx, hout⟩

/-- §17.8: the app clears a namespace's cache. Every unpinned entry of the
namespace goes, headers and chunks alike. -/
def clear (ns : Nat) (es : List Entry) : List Entry :=
  es.filter fun e => !(e.space == ns && !e.pinned)

/-- **Clearing leaves no unpinned entry in the namespace.** -/
theorem clear_empties (ns : Nat) (es : List Entry) :
    ∀ e ∈ clear ns es, e.space = ns → e.pinned = true := by
  intro e he hs
  simp [clear] at he
  have := he.2
  cases h : e.pinned
  · exact absurd hs (by simpa [h] using this)
  · rfl

/-- **Clearing keeps every pinned entry.** -/
theorem clear_keeps_pinned (ns : Nat) (es : List Entry) :
    ∀ x ∈ es, x.pinned = true → x ∈ clear ns es := by
  intro x hx hp
  simp [clear, hx, hp]

/-- **Clearing one namespace never touches another.** -/
theorem clear_other_namespace_kept (ns : Nat) (es : List Entry) :
    ∀ x ∈ es, x.space ≠ ns → x ∈ clear ns es := by
  intro x hx hs
  simp [clear, hx, hs]

end CovenIO.Budget
