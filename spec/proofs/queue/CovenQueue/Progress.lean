import CovenQueue.Upload

namespace CovenQueue

theorem prepare_counters (q : Queue) (f : Nat) (ks : List Nat) :
    (q.prepare f ks).settled = q.settled ∧ (q.prepare f ks).reserved = q.reserved := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => exact ⟨rfl, rfl⟩
    | cons p rest => cases p <;> exact ⟨rfl, rfl⟩

theorem finish_counters (q : Queue) :
    q.settled ≤ q.finish.settled ∧ q.finish.reserved = q.reserved := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => exact ⟨Nat.le_refl _, rfl⟩
    | cons p rest =>
      cases p with
      | untried _ => exact ⟨Nat.le_refl _, rfl⟩
      | tried _ =>
        simp only [Queue.finish, Queue.reserved, List.length_cons]
        omega

theorem finish_advances (q : Queue) (a : Attempt) (h : q.head = some a) :
    q.finish.settled = q.settled + 1 := by
  cases q with | mk d n ps =>
    cases ps with
    | nil => simp [Queue.head] at h
    | cons p rest =>
      cases p with
      | untried _ => simp [Queue.head] at h
      | tried _ => rfl

theorem step_monotone {encode : Seal} {s t : World} (h : Step encode s t) :
    s.queue.settled ≤ t.queue.settled ∧ s.queue.reserved ≤ t.queue.reserved := by
  cases h with
  | commit => simp [Queue.commit, Queue.reserved]
  | prepare f requests keys =>
    obtain ⟨hs, hr⟩ := prepare_counters s.queue f (keys.map Key.id)
    simp only [hs, hr, Nat.le_refl, and_self]
  | convert => simp [Queue.convert, Queue.reserved]
  | send => unfold World.send; split <;> exact ⟨Nat.le_refl _, Nat.le_refl _⟩
  | land => simp only [World.land]; split <;> exact ⟨Nat.le_refl _, Nat.le_refl _⟩
  | acknowledge =>
    obtain ⟨hs, hr⟩ := finish_counters s.queue
    exact ⟨hs, hr ▸ Nat.le_refl _⟩
  | wait => exact ⟨Nat.le_refl _, Nat.le_refl _⟩

def StoredPrefix (s : World) : Prop :=
  ∀ n, 0 < n → n ≤ s.queue.settled → ∃ bytes, s.stored n = some bytes

theorem landing_retains (encode : Seal) (s : World) (a : Attempt) (n : Nat) (bytes : Bytes)
    (h : s.stored n = some bytes) : (s.land encode a).stored n = some bytes := by
  simp only [World.land]
  split
  · rename_i absent
    by_cases hn : n = a.record.identity.id.number
    · subst n; simp [h] at absent
    · simp [hn, h]
  · exact h

theorem step_stored_prefix {encode : Seal} {s t : World}
    (h : Step encode s t) (hv : s.queue.Valid) (hs : StoredPrefix s) : StoredPrefix t := by
  cases h with
  | commit => exact hs
  | prepare =>
    intro n hn hp
    rw [(prepare_counters _ _ _).1] at hp
    exact hs n hn hp
  | convert => exact hs
  | send => unfold World.send; split <;> exact hs
  | land a _ =>
    intro n hn hp
    have hq : (s.land encode a).queue = s.queue := by
      simp only [World.land]; split <;> rfl
    rw [hq] at hp
    obtain ⟨bytes, hb⟩ := hs n hn hp
    exact ⟨bytes, landing_retains _ _ _ _ _ hb⟩
  | acknowledge a ha stored =>
    intro n hn hp
    have next := head_next s.queue hv a ha
    have advance := finish_advances s.queue a ha
    change n ≤ s.queue.finish.settled at hp
    rw [advance] at hp
    by_cases earlier : n ≤ s.queue.settled
    · exact hs n hn earlier
    · have eq : n = s.queue.settled + 1 := by omega
      have number := congrArg WriteId.number next
      exact ⟨encode a, by simpa [number, eq] using stored⟩
  | wait => exact hs

def World.empty (device : Nat) : World := ⟨⟨device, 0, []⟩, fun _ => none, []⟩

inductive Reachable (encode : Seal) (device : Nat) : World → Prop where
  | start : Reachable encode device (.empty device)
  | next {s t} : Reachable encode device s → Step encode s t → Reachable encode device t

theorem reachable_invariants {encode : Seal} {d : Nat} {s : World}
    (h : Reachable encode d s) : s.queue.Valid ∧ StoredPrefix s := by
  induction h with
  | start => exact ⟨.nil _, by intro n hn hp; simp [World.empty] at hp; omega⟩
  | next _ step ih =>
    exact ⟨step_queue_valid step ih.1, step_stored_prefix step ih.1 ih.2⟩

/-- No later write can even be sent before every earlier number is confirmed
stored. A failed-but-landed head still blocks until its confirmation. -/
theorem uploads_in_order {encode : Seal} {d : Nat} {s : World} (h : Reachable encode d s)
    (a : Attempt) (ha : s.queue.head = some a) :
    ∀ n, 0 < n → n < a.record.identity.id.number → ∃ bytes, s.stored n = some bytes := by
  obtain ⟨valid, storedPrefix⟩ := reachable_invariants h
  have number := congrArg WriteId.number (head_next _ valid a ha)
  intro n hn hl
  apply storedPrefix n hn
  simp only at number
  omega

structure Run (encode : Seal) where
  state : Nat → World
  valid : (state 0).queue.Valid
  step : ∀ t, Step encode (state t) (state (t + 1))

theorem Run.queue_valid {encode : Seal} (run : Run encode) (t : Nat) :
    (run.state t).queue.Valid := by
  induction t with
  | zero => exact run.valid
  | succ t ih => exact step_queue_valid (run.step t) ih

theorem Run.monotone {encode : Seal} (run : Run encode) {t u : Nat} (h : t ≤ u) :
    (run.state t).queue.settled ≤ (run.state u).queue.settled ∧
    (run.state t).queue.reserved ≤ (run.state u).queue.reserved := by
  induction h with
  | refl => exact ⟨Nat.le_refl _, Nat.le_refl _⟩
  | @step u _ ih =>
    have next := step_monotone (run.step u)
    exact ⟨Nat.le_trans ih.1 next.1, Nat.le_trans ih.2 next.2⟩

/-- These are environmental assumptions, not axioms. The installation remains
usable and syncing. Required app updates, raises, reloads and keys eventually
permit an untried head's first attempt. Retried bytes eventually reach storage,
and confirmation reads and their local transactions eventually succeed. -/
structure Fair {encode : Seal} (run : Run encode) : Prop where
  prepare : ∀ t, (run.state t).queue.settled < (run.state t).queue.reserved →
    ∃ u, t ≤ u ∧ ((run.state t).queue.settled < (run.state u).queue.settled ∨
      ∃ a, (run.state u).queue.head = some a)
  storageAccepts : ∀ t a, (run.state t).queue.head = some a →
    ∃ u, t ≤ u ∧ (run.state u).stored a.record.identity.id.number = some (encode a)
  confirm : ∀ t a, (run.state t).queue.head = some a →
    (run.state t).stored a.record.identity.id.number = some (encode a) →
    ∃ u, t ≤ u ∧ (run.state u).queue.head = some a ∧
      (run.state (u + 1)).queue = (run.state u).queue.finish

theorem step_preserves_attempt {encode : Seal} {s t : World} (h : Step encode s t)
    (a : Attempt) (ha : s.queue.head = some a)
    (same : s.queue.settled = t.queue.settled) : t.queue.head = some a := by
  cases h with
  | commit =>
    cases hp : s.queue.waiting with
    | nil => simp [Queue.head, hp] at ha
    | cons p rest =>
      cases p with
      | untried _ => simp [Queue.head, hp] at ha
      | tried _ => simpa [Queue.head, Queue.commit, hp] using ha
  | prepare f requests keys app store r rest hp =>
    simp [Queue.head, hp] at ha
  | convert => exact head_attempt_preserved _ _ _ _ ha
  | send => unfold World.send; split <;> exact ha
  | land => simp only [World.land]; split <;> exact ha
  | acknowledge a' ha' _ =>
    have advance := finish_advances s.queue a' ha'
    change s.queue.settled = s.queue.finish.settled at same
    omega
  | wait => exact ha

theorem Run.attempt_persists {encode : Seal} (run : Run encode) {t u : Nat} (htu : t ≤ u)
    (a : Attempt) (ha : (run.state t).queue.head = some a)
    (same : (run.state t).queue.settled = (run.state u).queue.settled) :
    (run.state u).queue.head = some a := by
  induction htu with
  | refl => exact ha
  | @step u htu ih =>
    change (run.state t).queue.settled = (run.state (u + 1)).queue.settled at same
    have m := run.monotone htu
    have m' := step_monotone (run.step u)
    have same' : (run.state t).queue.settled = (run.state u).queue.settled := by omega
    exact step_preserves_attempt (run.step u) a (ih same') (by omega)

theorem head_eventually_settles {encode : Seal} (run : Run encode) (fair : Fair run)
    (t : Nat) (hp : (run.state t).queue.settled < (run.state t).queue.reserved) :
    ∃ u, t ≤ u ∧ (run.state t).queue.settled < (run.state u).queue.settled := by
  obtain ⟨u, htu, advanced | ⟨a, ha⟩⟩ := fair.prepare t hp
  · exact ⟨u, htu, advanced⟩
  · obtain ⟨v, huv, stored⟩ := fair.storageAccepts u a ha
    have mt := run.monotone htu
    have mu := run.monotone huv
    by_cases adv : (run.state t).queue.settled < (run.state v).queue.settled
    · exact ⟨v, Nat.le_trans htu huv, adv⟩
    · have same : (run.state u).queue.settled = (run.state v).queue.settled := by omega
      have hav := run.attempt_persists huv a ha same
      obtain ⟨z, hvz, haz, finish⟩ := fair.confirm v a hav stored
      have mz := run.monotone hvz
      have advance := finish_advances (run.state z).queue a haz
      refine ⟨z + 1, by omega, ?_⟩
      rw [finish, advance]
      omega

/-- Every committed number settles, even if commits keep extending the tail.
Storage merely resolving attempts is insufficient: `Fair.storageAccepts`
specifically requires the original bytes to become stored. -/
theorem every_queued_write_settles {encode : Seal} (run : Run encode) (fair : Fair run)
    (t n : Nat) (hn : n ≤ (run.state t).queue.reserved) :
    ∃ u, t ≤ u ∧ n ≤ (run.state u).queue.settled := by
  generalize hd : n - (run.state t).queue.settled = remaining
  induction remaining using Nat.strongRecOn generalizing t with
  | ind remaining ih =>
    by_cases settled : n ≤ (run.state t).queue.settled
    · exact ⟨t, Nat.le_refl _, settled⟩
    · obtain ⟨u, htu, advanced⟩ := head_eventually_settles run fair t (by omega)
      have hm := run.monotone htu
      have less : n - (run.state u).queue.settled < remaining := by omega
      obtain ⟨v, huv, hs⟩ := ih _ less u (by omega) rfl
      exact ⟨v, Nat.le_trans htu huv, hs⟩

end CovenQueue
