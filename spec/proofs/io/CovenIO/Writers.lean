import CovenIO.Retired
import CovenStorelog.Invariants

/-! §6 and sync-pass step 2: the writers a device probes. One set serves all
three logs: every remembered store-log folder, and every device a received
entry was written from or registers, whether replay keeps or drops that
entry. Drained writers (§6) leave it. Replay verdicts never shrink it, so a
write that counts is discovered by every reader holding its author's view. -/

namespace CovenIO.Writers

open CovenStorelog (Log EntrySet State lookup)

/-- The devices one entry makes known: its writing device and, for a device
registration, the device it registers. Creation registers its own writer. -/
def names (e : CovenStorelog.Entry) : List Nat :=
  match e.action with
  | .addDevice _ d => [e.device, d]
  | _ => [e.device]

/-- Known writers from remembered folder listings and the received entries,
kept and dropped alike. No replay verdict is an input. -/
def known (M : Log) (received listed : List Nat) : List Nat :=
  (listed ++ received.flatMap fun w => names (M w)).eraseDups

/-- The replaced rule: the devices the current replay registers. -/
def replayDevices (r : CovenStorelog.Result) : List Nat := r.state.devices.map Prod.fst

theorem mem_known (M : Log) (received listed : List Nat) (d : Nat) :
    d ∈ known M received listed ↔ d ∈ listed ∨ ∃ w ∈ received, d ∈ names (M w) := by
  simp [known, List.mem_eraseDups, List.mem_flatMap]

/-- Receiving more entries or remembering more folders never forgets a writer;
a later verdict, kept or dropped, is not an input at all. -/
theorem known_monotone (M : Log) {a b la lb : List Nat}
    (entries : ∀ w ∈ a, w ∈ b) (folders : ∀ d ∈ la, d ∈ lb) {d : Nat}
    (member : d ∈ known M a la) : d ∈ known M b lb := by
  rcases (mem_known M a la d).mp member with listed | ⟨w, hw, named⟩
  · exact (mem_known M b lb d).mpr (Or.inl (folders d listed))
  · exact (mem_known M b lb d).mpr (Or.inr ⟨w, entries w hw, named⟩)

/-- An effect adds a device only by registering it. -/
theorem effect_devices {s t : State} {w : Nat} {e : CovenStorelog.Entry}
    {remove : Nat → CovenStorelog.Circle → Nat → Option CovenStorelog.Circle}
    (h : CovenStorelog.effect s w e remove = some t) :
    ∀ p ∈ t.devices, p ∈ s.devices ∨ p.1 ∈ names e := by
  intro p hp
  obtain ⟨author, device, past, action⟩ := e
  cases action <;> simp only [CovenStorelog.effect] at h
  all_goals (try simp only [bind, Option.bind_eq_some_iff] at h)
  all_goals (try obtain ⟨_, _, h⟩ := h)
  all_goals (repeat' split at h)
  all_goals (try cases h)
  all_goals simp_all [names, CovenStorelog.put, CovenStorelog.erase]
  all_goals (rcases hp with rfl | ⟨hp, _⟩ <;> simp_all)

/-- Every device in a state was named by one of `entries`. -/
def Registered (M : Log) (entries : List Nat) (s : State) : Prop :=
  ∀ p ∈ s.devices, ∃ w ∈ entries, p.1 ∈ names (M w)

theorem scan_registered (M : Log) (views : Nat → State) (entries : List Nat)
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → CovenStorelog.Entry → Option State}
    (hp : ∀ s w t, realize s w (M w) = some t → ∀ p ∈ t.devices, p ∈ s.devices ∨ p.1 ∈ names (M w))
    {todo : List Nat} {r out : CovenStorelog.Result}
    (ht : ∀ w ∈ todo, w ∈ entries) (hr : Registered M entries r.state)
    (h : CovenStorelog.scan M views todo r conflict prefer realize = .complete out) :
    Registered M entries out.state := by
  induction todo generalizing r with
  | nil => cases h; exact hr
  | cons w ws ih =>
      have hws : ∀ x ∈ ws, x ∈ entries := fun x hx => ht x (List.mem_cons_of_mem w hx)
      simp only [CovenStorelog.scan] at h
      split at h
      · exact ih hws hr h
      · split at h
        · exact ih (r := { r with dropped := w :: r.dropped }) hws hr h
        · split at h
          · exact ih (r := { r with kept := w :: r.kept }) hws hr h
          · cases he : realize r.state w (M w) with
            | none =>
                simp only [he] at h
                exact ih (r := { r with dropped := w :: r.dropped }) hws hr h
            | some next =>
                simp only [he] at h
                split at h
                · exact ih (r := { r with dropped := w :: r.dropped }) hws hr h
                · split at h
                  · refine ih (r := { r with state := next, kept := w :: r.kept }) hws ?_ h
                    intro p member
                    rcases hp r.state w next he p member with old | named
                    · exact hr p old
                    · exact ⟨w, ht w List.mem_cons_self, named⟩
                  · cases h

theorem settleN_registered (M : Log) (views : Nat → State) (entries : List Nat)
    {conflict prefer : Nat → Nat → Bool}
    {realize : State → Nat → CovenStorelog.Entry → Option State}
    (hp : ∀ s w t, realize s w (M w) = some t → ∀ p ∈ t.devices, p ∈ s.devices ∨ p.1 ∈ names (M w))
    {fuel : Nat} {drops : List Nat} {out : CovenStorelog.Result}
    (h : CovenStorelog.settleN M views entries fuel drops conflict prefer realize = some out) :
    Registered M entries out.state := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      simp only [CovenStorelog.settleN] at h
      cases he : CovenStorelog.scan M views entries ⟨State.empty, [], drops⟩ conflict prefer realize with
      | complete r =>
          simp only [he, Option.some.injEq] at h
          subst out
          exact scan_registered M views entries hp (fun _ h => h)
            (by intro p hp; simp [State.empty] at hp) he
      | restart next => simp only [he] at h; exact ih h

/-- §7.1's authority for a write is its device registered in the replay of its
recorded view. That replay registers a device only through an entry of the
view naming it, kept there. -/
theorem registered_named (M : Log) (n : Nat) (S : EntrySet) {d m : Nat}
    (h : lookup (CovenStorelog.resolve M n S).state.devices d = some m) :
    ∃ w, w < n ∧ S w = true ∧ d ∈ names (M w) := by
  have registered := settleN_registered M (CovenStorelog.authorViews M n)
    ((List.range n).filter S)
    (fun s w t step => effect_devices (CovenStorelog.checkedEffect_sound step).1)
    (CovenStorelog.settle_eq_some M _ _)
  obtain ⟨w, hw, named⟩ := registered (d, m) (CovenStorelog.lookup_some_mem h)
  simp only [List.mem_filter, List.mem_range] at hw
  exact ⟨w, hw.1, hw.2, named⟩

/-- Convergence of discovery: a write that counts has its device registered in
the replay of its recorded view `view`. Every reader that received that view's
entries knows the device, whatever its own replay keeps or drops, and polls
it until its own drain completes. -/
theorem counting_write_discovered (M : Log) (n : Nat) (view : EntrySet)
    (received listed : List Nat) (life : Nat → Retired.Life) {writer author : Nat}
    (counts : lookup (CovenStorelog.resolve M n view).state.devices writer = some author)
    (caughtUp : ∀ w, w < n → view w = true → w ∈ received) :
    writer ∈ Retired.polling life (known M received listed) ∨ life writer = .drained := by
  obtain ⟨w, hw, inView, named⟩ := registered_named M n view counts
  have member : writer ∈ known M received listed :=
    (mem_known M received listed writer).mpr (Or.inr ⟨w, caughtUp w hw inView, named⟩)
  by_cases drained : life writer = .drained
  · exact Or.inr drained
  · exact Or.inl (List.mem_filter.mpr ⟨member, by simpa using drained⟩)

/-- Draining still bounds the idle pass: two listings and three next-number
GETs per undrained known writer, a dropped registration counting until its
device drains, and none naming a drained writer. -/
theorem known_requests (M : Log) (received listed : List Nat) (life : Nat → Retired.Life)
    (next : Kind → Nat → Nat) (writer : Nat) :
    (idle next (Retired.polling life (known M received listed))).length =
      2 + 3 * (Retired.polling life (known M received listed)).length ∧
    ∀ request ∈ idle next (Retired.polling (Retired.retire life writer) (known M received listed)),
      request.names writer = false :=
  ⟨idle_requests next _, fun request member =>
    Retired.drained_not_named life _ writer next request member⟩

end CovenIO.Writers
