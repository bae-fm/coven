import CovenSequence.Retention

/-!
# Which announcements a device keeps

Each device keeps the snapshot announcements it accepted, and the original slot
of each, from which it rebuilds them when it opens. Rules read four kinds:

- one a raise or reset entry applied names (`History.accept_admin`);
- the newest of its audience, and every one newer than the audience's newest
  ordinary one: the floor's names, the snapshot request and the circle reload
  start from the newest (usable) one;
- one at the audience's greatest coverage: acceptance and a raise or reset
  entry are checked against it;
- one at or after the floor's first kept slot, or named by a journaled floor
  capture: retention reads those.

Every other one is `forgettable`. A device forgets them: their original slots
and their place in its history go, and a refused announcement's original is
never stored. This module models acceptance (`accepts`), a raise or reset
entry (`activation`), a device's stored originals and the history it rebuilds
from them (`Device`, `rebuild`), and proves:

- `answer_same`: a device that forgets accepts or refuses every later entry
  exactly as a device that holds every announcement does;
- `rebuilt_same`: rebuilding from the stored originals gives the device's
  history;
- `equal_coverage_kept`: forgetting one only matched in coverage would change
  a raise's answer, which is why the greatest coverage is kept;
- `floor_names_kept` and `newest_kept`: what a reader loads is never
  forgettable;
- `listing_spares` and `listing_finds_superseded`: a listing of snapshot
  folders never deletes a body a kept announcement or an open operation
  names, and deletes every other body it lists.
-/

namespace CovenSequence.Announcements

/-- What an announced snapshot is for. -/
inductive Purpose where
  | ordinary
  | raise (version : Nat)
  | reset
  deriving DecidableEq

/-- What a raise or reset entry activates. -/
inductive Kind where
  | raise (version : Nat)
  | reset
  deriving DecidableEq

def Kind.purpose : Kind → Purpose
  | .raise version => .raise version
  | .reset => .reset

/-- An accepted announcement: its slot, audience, coverage, purpose and the
body it names. -/
structure Announcement (Aud : Type) where
  slot : Nat
  aud : Aud
  covers : Nat
  purpose : Purpose
  body : Nat
  deriving DecidableEq

/-- A raise or reset entry at `slot`, naming the announcement at `named`. -/
structure Activation where
  slot : Nat
  named : Nat
  kind : Kind

/-- An applied raise or reset: its entry's slot and the announcement it named. -/
structure Boundary (Aud : Type) where
  entry : Nat
  named : Announcement Aud

inductive Entry (Aud : Type) where
  | announce (a : Announcement Aud)
  | activate (x : Activation)

def Entry.slot {Aud : Type} : Entry Aud → Nat
  | .announce a => a.slot
  | .activate x => x.slot

/-- A device's accepted announcements, newest first, and its applied raises
and resets. -/
structure History (Aud : Type) where
  anns : List (Announcement Aud)
  bounds : List (Boundary Aud)

def History.empty {Aud : Type} : History Aud := ⟨[], []⟩

/-- The checks that read the store's state rather than its announcements:
authority, membership and versions. The state comes from admin entries, which
every device keeps, so the checks see the same state on every device. -/
structure Checks (Aud : Type) where
  allowed : List (Boundary Aud) → Announcement Aud → Bool
  permitted : List (Boundary Aud) → Activation → Announcement Aud → Bool
  inPlace : List (Boundary Aud) → Activation → Announcement Aud → Bool

variable {Aud : Type} [DecidableEq Aud]

/-- No announcement of `A` in `anns` covers more than `c`. -/
def coverAtMost (anns : List (Announcement Aud)) (A : Aud) (c : Nat) : Bool :=
  anns.all fun p => p.aud != A || decide (p.covers ≤ c)

/-- No raise or reset of `A` in `bounds` is past `c`. -/
def boundsAtMost (bounds : List (Boundary Aud)) (A : Aud) (c : Nat) : Bool :=
  bounds.all fun b => b.named.aud != A || decide (b.entry ≤ c)

/-- An announcement is accepted when its coverage is below its slot, no
announcement or raise or reset of its audience has passed it, and the state
allows it (`announcement_allowed`). -/
def accepts (C : Checks Aud) (h : History Aud) (a : Announcement Aud) : Bool :=
  decide (a.covers < a.slot) && coverAtMost h.anns a.aud a.covers &&
    boundsAtMost h.bounds a.aud a.covers && C.allowed h.bounds a

/-- The announcement a raise or reset entry names, found by its slot. -/
def named (anns : List (Announcement Aud)) (x : Activation) : Option (Announcement Aud) :=
  anns.find? fun a => a.slot == x.named

/-- A raise or reset applies when it names an earlier announcement of its
kind, the state permits it, it isn't in place already, and no announcement or
raise or reset of the audience has passed the named coverage
(`checked_change`). -/
def activates (C : Checks Aud) (h : History Aud) (x : Activation) (r : Announcement Aud) : Bool :=
  decide (r.slot < x.slot) && r.purpose == x.kind.purpose && C.permitted h.bounds x r &&
    !C.inPlace h.bounds x r && coverAtMost h.anns r.aud r.covers &&
    boundsAtMost h.bounds r.aud r.covers

def activation (C : Checks Aud) (h : History Aud) (x : Activation) : Option (Boundary Aud) :=
  (named h.anns x).bind fun r => if activates C h x r then some ⟨x.slot, r⟩ else none

def step (C : Checks Aud) (h : History Aud) : Entry Aud → History Aud
  | .announce a => if accepts C h a then ⟨a :: h.anns, h.bounds⟩ else h
  | .activate x =>
    match activation C h x with
    | some b => ⟨h.anns, b :: h.bounds⟩
    | none => h

/-- Whether a device stores the entry's original slot: every admin entry, and
an announcement only when it is accepted. -/
def stores (C : Checks Aud) (h : History Aud) : Entry Aud → Bool
  | .announce a => accepts C h a
  | .activate _ => true

/-- The history the stored originals give, applied in slot order. -/
def rebuild (C : Checks Aud) (es : List (Entry Aud)) : History Aud :=
  es.foldl (step C) History.empty

/-- An announcement no rule reads any more: no applied raise or reset names
it, another of its audience covers more, its slot is below the floor's first
kept slot `first`, an ordinary one of its audience is newer, and no journaled
floor capture names it (`journal`; a capture names only ordinary ones). -/
def forgettable (first : Nat) (journal : List Nat) (h : History Aud)
    (r : Announcement Aud) : Bool :=
  h.bounds.all (fun b => b.named.slot != r.slot) &&
    h.anns.any (fun p => p.aud == r.aud && decide (r.covers < p.covers)) &&
    decide (r.slot < first) &&
    h.anns.any (fun o => o.aud == r.aud && o.purpose == .ordinary && decide (r.slot < o.slot)) &&
    !(r.purpose == .ordinary && journal.contains r.slot)

/-- The history without the announcements `F` forgets. -/
def without (F : Announcement Aud → Bool) (h : History Aud) : History Aud :=
  ⟨h.anns.filter (fun r => !F r), h.bounds⟩

/-- Whether an original survives forgetting the announcements `F` names. -/
def survives (F : Announcement Aud → Bool) : Entry Aud → Bool
  | .announce a => !F a
  | .activate _ => true

/-- What a device does: apply the next entry, or forget what is forgettable
at the floor's `first` and the journaled captures' names. -/
inductive Event (Aud : Type) where
  | entry (x : Entry Aud)
  | forget (first : Nat) (journal : List Nat)

/-- A device's history and the originals it stores, in slot order. -/
structure Device (Aud : Type) where
  history : History Aud
  stored : List (Entry Aud)

def Device.next (C : Checks Aud) (d : Device Aud) : Event Aud → Device Aud
  | .entry x =>
    ⟨step C d.history x, if stores C d.history x then d.stored ++ [x] else d.stored⟩
  | .forget first journal =>
    let F := forgettable first journal d.history
    ⟨without F d.history, d.stored.filter (survives F)⟩

def Device.empty : Device Aud := ⟨History.empty, []⟩

def entriesOf : List (Event Aud) → List (Entry Aud)
  | [] => []
  | .entry x :: rest => x :: entriesOf rest
  | .forget _ _ :: rest => entriesOf rest

/-- Entries arrive in slot order, each at its own slot. -/
def Increasing (es : List (Entry Aud)) : Prop :=
  es.Pairwise fun a b => a.slot < b.slot


/-! ## Reading the checks -/

theorem coverAtMost_iff {anns : List (Announcement Aud)} {A : Aud} {c : Nat} :
    coverAtMost anns A c = true ↔ ∀ p ∈ anns, p.aud = A → p.covers ≤ c := by
  simp only [coverAtMost, List.all_eq_true, Bool.or_eq_true, bne_iff_ne, ne_eq,
    decide_eq_true_eq]
  exact ⟨fun h p hp ha => (h p hp).resolve_left (Classical.not_not.mpr ha),
    fun h p hp => (Classical.em (p.aud = A)).elim (fun ha => Or.inr (h p hp ha)) Or.inl⟩

theorem boundsAtMost_iff {bounds : List (Boundary Aud)} {A : Aud} {c : Nat} :
    boundsAtMost bounds A c = true ↔ ∀ b ∈ bounds, b.named.aud = A → b.entry ≤ c := by
  simp only [boundsAtMost, List.all_eq_true, Bool.or_eq_true, bne_iff_ne, ne_eq,
    decide_eq_true_eq]
  exact ⟨fun h b hb ha => (h b hb).resolve_left (Classical.not_not.mpr ha),
    fun h b hb => (Classical.em (b.named.aud = A)).elim (fun ha => Or.inr (h b hb ha)) Or.inl⟩

theorem forgettable_iff {first : Nat} {journal : List Nat} {h : History Aud}
    {r : Announcement Aud} :
    forgettable first journal h r = true ↔
      (∀ b ∈ h.bounds, b.named.slot ≠ r.slot) ∧
      (∃ p ∈ h.anns, p.aud = r.aud ∧ r.covers < p.covers) ∧
      r.slot < first ∧
      (∃ o ∈ h.anns, o.aud = r.aud ∧ o.purpose = .ordinary ∧ r.slot < o.slot) ∧
      (r.purpose ≠ .ordinary ∨ r.slot ∉ journal) := by
  simp [forgettable, List.all_eq_true, List.any_eq_true, and_assoc]

/-! ## Finding an announcement by its slot -/

/-- Newest first, each at its own slot. -/
def Ordered (anns : List (Announcement Aud)) : Prop :=
  anns.Pairwise fun newer older => older.slot < newer.slot

theorem slot_inj : ∀ {l : List (Announcement Aud)}, Ordered l →
    ∀ {a b : Announcement Aud}, a ∈ l → b ∈ l → a.slot = b.slot → a = b
  | [], _, _, _, ha, _, _ => by simp at ha
  | head :: tail, ordered, a, b, ha, hb, same => by
    rw [Ordered, List.pairwise_cons] at ordered
    rcases List.mem_cons.mp ha with rfl | ha'
    · rcases List.mem_cons.mp hb with rfl | hb'
      · rfl
      · have := ordered.1 b hb'; omega
    · rcases List.mem_cons.mp hb with rfl | hb'
      · have := ordered.1 a ha'; omega
      · exact slot_inj ordered.2 ha' hb' same

omit [DecidableEq Aud] in
theorem named_spec {l : List (Announcement Aud)} {x : Activation} {r : Announcement Aud}
    (h : named l x = some r) : r ∈ l ∧ r.slot = x.named := by
  refine ⟨List.mem_of_find?_eq_some h, ?_⟩
  have := List.find?_some h
  simpa using this

omit [DecidableEq Aud] in
theorem named_none {l : List (Announcement Aud)} {x : Activation} :
    named l x = none ↔ ∀ r ∈ l, r.slot ≠ x.named := by
  simp [named, List.find?_eq_none]

theorem named_of_mem {l : List (Announcement Aud)} (ordered : Ordered l) {x : Activation}
    {r : Announcement Aud} (hr : r ∈ l) (hs : r.slot = x.named) : named l x = some r := by
  cases h : named l x with
  | none => exact absurd hs (named_none.mp h r hr)
  | some r' =>
    obtain ⟨hm, hs'⟩ := named_spec h
    rw [slot_inj ordered hm hr (hs'.trans hs.symm)]

/-- Looking an announcement up in a sublist finds it when the sublist holds
it, and nothing otherwise. -/
theorem named_sublist {H G : List (Announcement Aud)} (sub : H.Sublist G) (ordered : Ordered G)
    (x : Activation) :
    (named G x = none → named H x = none) ∧
    ∀ r, named G x = some r → (r ∈ H → named H x = some r) ∧ (r ∉ H → named H x = none) := by
  have orderedH : Ordered H := ordered.sublist sub
  refine ⟨fun h => named_none.mpr fun r hr => named_none.mp h r (sub.subset hr), fun r h => ?_⟩
  obtain ⟨hm, hs⟩ := named_spec h
  refine ⟨fun hr => named_of_mem orderedH hr hs, fun hr => named_none.mpr fun r' hr' hs' => ?_⟩
  exact hr (slot_inj ordered hm (sub.subset hr') (hs.trans hs'.symm) ▸ hr')

/-! ## The greatest coverage -/

theorem exists_greatest : ∀ {l : List (Announcement Aud)} {q : Announcement Aud}, q ∈ l →
    ∃ m ∈ l, m.aud = q.aud ∧ q.covers ≤ m.covers ∧
      ∀ p ∈ l, p.aud = q.aud → p.covers ≤ m.covers
  | [], _, hq => by simp at hq
  | head :: tail, q, hq => by
    by_cases ht : ∃ t ∈ tail, t.aud = q.aud
    · obtain ⟨t, htm, hta⟩ := ht
      obtain ⟨m, hm, hma, hmt, hall⟩ := exists_greatest htm
      rw [hta] at hma hall
      by_cases hh : head.aud = q.aud ∧ m.covers < head.covers
      · refine ⟨head, List.mem_cons_self, hh.1, ?_, fun p hp hpa => ?_⟩
        · rcases List.mem_cons.mp hq with rfl | hq'
          · exact Nat.le_refl _
          · have := hall q hq' rfl; omega
        · rcases List.mem_cons.mp hp with rfl | hp'
          · exact Nat.le_refl _
          · have := hall p hp' hpa; omega
      · refine ⟨m, List.mem_cons_of_mem _ hm, hma, ?_, fun p hp hpa => ?_⟩
        · rcases List.mem_cons.mp hq with rfl | hq'
          · exact Nat.le_of_not_lt fun hlt => hh ⟨rfl, hlt⟩
          · exact hall q hq' rfl
        · rcases List.mem_cons.mp hp with rfl | hp'
          · exact Nat.le_of_not_lt fun hlt => hh ⟨hpa, hlt⟩
          · exact hall p hp' hpa
    · have hq' : q = head := by
        rcases List.mem_cons.mp hq with h | h
        · exact h
        · exact absurd ⟨q, h, rfl⟩ ht
      subst hq'
      refine ⟨q, List.mem_cons_self, rfl, Nat.le_refl _, fun p hp hpa => ?_⟩
      rcases List.mem_cons.mp hp with rfl | hp'
      · exact Nat.le_refl _
      · exact absurd ⟨p, hp', hpa⟩ ht

/-! ## A device that forgets answers as one that holds everything -/

/-- What a device that forgets (`H`) and one that holds every announcement
(`G`) share: the same raises and resets, a part of the same announcements,
for each one `G` holds one in `H` covering at least as much, and for each one
`H` lacks, another in `G` covering more. -/
structure Agree (H G : History Aud) : Prop where
  bounds : H.bounds = G.bounds
  sub : H.anns.Sublist G.anns
  ordered : Ordered G.anns
  dominated : ∀ p ∈ G.anns, ∃ q ∈ H.anns, q.aud = p.aud ∧ p.covers ≤ q.covers
  exceeded : ∀ r ∈ G.anns, r ∉ H.anns → ∃ p ∈ G.anns, p.aud = r.aud ∧ r.covers < p.covers

theorem coverAtMost_agree {H G : History Aud} (agree : Agree H G) (A : Aud) (c : Nat) :
    coverAtMost H.anns A c = coverAtMost G.anns A c := by
  apply Bool.eq_iff_iff.mpr
  rw [coverAtMost_iff, coverAtMost_iff]
  constructor
  · intro hH p hp hpa
    obtain ⟨q, hq, hqa, hle⟩ := agree.dominated p hp
    have := hH q hq (hqa.trans hpa); omega
  · intro hG p hp hpa
    exact hG p (agree.sub.subset hp) hpa

theorem accepts_agree (C : Checks Aud) {H G : History Aud} (agree : Agree H G)
    (a : Announcement Aud) : accepts C H a = accepts C G a := by
  simp only [accepts, coverAtMost_agree agree, agree.bounds]

theorem activation_agree (C : Checks Aud) {H G : History Aud} (agree : Agree H G)
    (x : Activation) : activation C H x = activation C G x := by
  have lookup := named_sublist agree.sub agree.ordered x
  unfold activation
  cases hG : named G.anns x with
  | none => rw [lookup.1 hG]; rfl
  | some r =>
    by_cases hr : r ∈ H.anns
    · rw [(lookup.2 r hG).1 hr]
      simp only [Option.bind_some, activates, coverAtMost_agree agree, agree.bounds]
      rfl
    · rw [(lookup.2 r hG).2 hr]
      obtain ⟨p, hp, hpa, hlt⟩ := agree.exceeded r (named_spec hG).1 hr
      have hcov : coverAtMost G.anns r.aud r.covers = false := by
        cases hc : coverAtMost G.anns r.aud r.covers with
        | false => rfl
        | true => have := coverAtMost_iff.mp hc p hp hpa; omega
      simp [activates, hcov]

theorem step_anns (C : Checks Aud) (h : History Aud) (e : Entry Aud) :
    ∀ p ∈ (step C h e).anns, p ∈ h.anns ∨ p.slot = e.slot := by
  intro p hp
  cases e with
  | announce a =>
    by_cases ha : accepts C h a = true
    · simp only [step, ha, ite_true, List.mem_cons] at hp
      rcases hp with rfl | hp'
      · exact Or.inr rfl
      · exact Or.inl hp'
    · simp only [step, ha] at hp
      exact Or.inl hp
  | activate x =>
    cases hx : activation C h x <;> simp only [step, hx] at hp <;> exact Or.inl hp

theorem agree_step (C : Checks Aud) {H G : History Aud} (agree : Agree H G) (e : Entry Aud)
    (fresh : ∀ p ∈ G.anns, p.slot < e.slot) : Agree (step C H e) (step C G e) := by
  cases e with
  | announce a =>
    simp only [step, accepts_agree C agree a]
    split
    · refine ⟨agree.bounds, agree.sub.cons_cons a, List.pairwise_cons.mpr ⟨fresh, agree.ordered⟩,
        fun p hp => ?_, fun r hr hn => ?_⟩
      · rcases List.mem_cons.mp hp with rfl | hp'
        · exact ⟨p, List.mem_cons_self, rfl, Nat.le_refl _⟩
        · obtain ⟨q, hq, hqa, hle⟩ := agree.dominated p hp'
          exact ⟨q, List.mem_cons_of_mem _ hq, hqa, hle⟩
      · have hra : r ≠ a := fun h => hn (h ▸ List.mem_cons_self)
        have hrG : r ∈ G.anns := (List.mem_cons.mp hr).resolve_left hra
        obtain ⟨p, hp, hpa, hlt⟩ :=
          agree.exceeded r hrG fun h => hn (List.mem_cons_of_mem _ h)
        exact ⟨p, List.mem_cons_of_mem _ hp, hpa, hlt⟩
    · exact agree
  | activate x =>
    simp only [step, activation_agree C agree x]
    split
    · exact ⟨by simp [agree.bounds], agree.sub, agree.ordered, agree.dominated, agree.exceeded⟩
    · exact agree

/-- Forgetting keeps the agreement: an announcement at its audience's greatest
coverage is never forgettable, and a forgotten one is passed by another. -/
theorem agree_forget {H G : History Aud} (agree : Agree H G) (first : Nat)
    (journal : List Nat) : Agree (without (forgettable first journal H) H) G := by
  refine ⟨agree.bounds, (List.filter_sublist).trans agree.sub, agree.ordered,
    fun p hp => ?_, fun r hr hn => ?_⟩
  · obtain ⟨q, hq, hqa, hle⟩ := agree.dominated p hp
    obtain ⟨m, hm, hma, hqm, hall⟩ := exists_greatest hq
    have kept : forgettable first journal H m = false := by
      cases hf : forgettable first journal H m with
      | false => rfl
      | true =>
        obtain ⟨_, ⟨p', hp', hpa', hlt⟩, _⟩ := forgettable_iff.mp hf
        have := hall p' hp' (hpa'.trans hma); omega
    exact ⟨m, List.mem_filter.mpr ⟨hm, by simp [kept]⟩, hma.trans hqa, Nat.le_trans hle hqm⟩
  · by_cases hrH : r ∈ H.anns
    · have hf : forgettable first journal H r = true := by
        cases hf : forgettable first journal H r with
        | true => rfl
        | false => exact absurd (List.mem_filter.mpr ⟨hrH, by simp [hf]⟩) hn
      obtain ⟨_, ⟨p, hp, hpa, hlt⟩, _⟩ := forgettable_iff.mp hf
      exact ⟨p, agree.sub.subset hp, hpa, hlt⟩
    · exact agree.exceeded r hr hrH

theorem agree_events (C : Checks Aud) : ∀ (events : List (Event Aud)) (d : Device Aud)
    (G : History Aud), Agree d.history G →
    (∀ p ∈ G.anns, ∀ e ∈ entriesOf events, p.slot < e.slot) →
    Increasing (entriesOf events) →
    Agree (events.foldl (Device.next C) d).history ((entriesOf events).foldl (step C) G)
  | [], _, _, agree, _, _ => agree
  | .entry x :: rest, d, G, agree, fresh, increasing => by
    simp only [entriesOf, Increasing, List.pairwise_cons] at increasing fresh
    simp only [List.foldl_cons, entriesOf]
    refine agree_events C rest _ _ (agree_step C agree x fun p hp => fresh p hp x
      List.mem_cons_self) (fun p hp e he => ?_) increasing.2
    rcases step_anns C G x p hp with hp' | hs
    · exact fresh p hp' e (List.mem_cons_of_mem _ he)
    · rw [hs]; exact increasing.1 e he
  | .forget first journal :: rest, d, G, agree, fresh, increasing => by
    simp only [entriesOf] at increasing fresh ⊢
    simp only [List.foldl_cons]
    exact agree_events C rest _ G (agree_forget agree first journal) fresh increasing

/-- **A device that forgets answers every later entry as one that holds
everything.** After any run of entries and forgetting, a device's history
accepts or refuses each announcement, and applies or refuses each raise or
reset, exactly as the history of every entry with nothing forgotten does. -/
theorem answer_same (C : Checks Aud) (events : List (Event Aud))
    (increasing : Increasing (entriesOf events)) :
    let d := events.foldl (Device.next C) Device.empty
    let G := rebuild C (entriesOf events)
    (∀ a, accepts C d.history a = accepts C G a) ∧
      (∀ x, activation C d.history x = activation C G x) ∧ d.history.bounds = G.bounds := by
  have agree := agree_events C events Device.empty History.empty
    ⟨rfl, List.Sublist.slnil, List.Pairwise.nil, fun _ h => by simp [History.empty] at h,
      fun _ h => by simp [History.empty] at h⟩
    (fun _ h => by simp [History.empty] at h) increasing
  exact ⟨accepts_agree C agree, activation_agree C agree, agree.bounds⟩

/-! ## Rebuilding from the stored originals -/

/-- What replaying entries in slot order leaves true: each announcement is
below its own slot, coverage never drops in slot order, and each raise or
reset names an earlier announcement that no earlier announcement passed. -/
structure Sound (h : History Aud) : Prop where
  ordered : Ordered h.anns
  below : ∀ a ∈ h.anns, a.covers < a.slot
  monotone : ∀ r ∈ h.anns, ∀ p ∈ h.anns, p.aud = r.aud → p.slot < r.slot → p.covers ≤ r.covers
  named : ∀ b ∈ h.bounds, b.named ∈ h.anns ∧ b.named.slot < b.entry
  passed : ∀ b ∈ h.bounds, ∀ p ∈ h.anns, p.slot < b.entry → p.aud = b.named.aud →
    p.covers ≤ b.named.covers

/-- Everything in `h` is at a slot below `s`. -/
def Below (h : History Aud) (s : Nat) : Prop :=
  (∀ a ∈ h.anns, a.slot < s) ∧ ∀ b ∈ h.bounds, b.entry < s

theorem activates_spec {C : Checks Aud} {h : History Aud} {x : Activation}
    {r : Announcement Aud} (hr : activates C h x r = true) :
    r.slot < x.slot ∧ r.purpose = x.kind.purpose ∧
      coverAtMost h.anns r.aud r.covers = true ∧ boundsAtMost h.bounds r.aud r.covers = true := by
  simp only [activates, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hr
  exact ⟨hr.1.1.1.1.1, hr.1.1.1.1.2, hr.1.2, hr.2⟩

theorem activation_spec {C : Checks Aud} {h : History Aud} {x : Activation}
    {b : Boundary Aud} (hb : activation C h x = some b) :
    ∃ r, named h.anns x = some r ∧ activates C h x r = true ∧ b = ⟨x.slot, r⟩ := by
  unfold activation at hb
  cases hn : named h.anns x with
  | none => rw [hn] at hb; cases hb
  | some r =>
    rw [hn, Option.bind_some] at hb
    by_cases ha : activates C h x r = true
    · simp only [ha, ite_true, Option.some.injEq] at hb
      exact ⟨r, rfl, ha, hb.symm⟩
    · simp [ha] at hb

theorem accepts_spec {C : Checks Aud} {h : History Aud} {a : Announcement Aud}
    (ha : accepts C h a = true) :
    a.covers < a.slot ∧ coverAtMost h.anns a.aud a.covers = true ∧
      boundsAtMost h.bounds a.aud a.covers = true := by
  simp only [accepts, Bool.and_eq_true, decide_eq_true_eq] at ha
  exact ⟨ha.1.1.1, ha.1.1.2, ha.1.2⟩

theorem step_mono (C : Checks Aud) (h : History Aud) (e : Entry Aud) :
    (∀ a ∈ h.anns, a ∈ (step C h e).anns) ∧ ∀ b ∈ h.bounds, b ∈ (step C h e).bounds := by
  cases e with
  | announce a =>
    by_cases ha : accepts C h a = true
    · simp only [step, ha, ite_true]
      exact ⟨fun _ h => List.mem_cons_of_mem _ h, fun _ h => h⟩
    · simp only [step, ha]
      exact ⟨fun _ h => h, fun _ h => h⟩
  | activate x =>
    cases hx : activation C h x <;> simp only [step, hx]
    · exact ⟨fun _ h => h, fun _ h => h⟩
    · exact ⟨fun _ h => h, fun _ h => List.mem_cons_of_mem _ h⟩

theorem step_bounds (C : Checks Aud) (h : History Aud) (e : Entry Aud) :
    ∀ b ∈ (step C h e).bounds, b ∈ h.bounds ∨ b.entry = e.slot := by
  intro b hb
  cases e with
  | announce a =>
    by_cases ha : accepts C h a = true
    · simp only [step, ha, ite_true] at hb; exact Or.inl hb
    · simp only [step, ha] at hb; exact Or.inl hb
  | activate x =>
    cases hx : activation C h x with
    | none => simp only [step, hx] at hb; exact Or.inl hb
    | some b' =>
      simp only [step, hx, List.mem_cons] at hb
      rcases hb with rfl | hb'
      · obtain ⟨_, _, _, rfl⟩ := activation_spec hx
        exact Or.inr rfl
      · exact Or.inl hb'

theorem fold_mono (C : Checks Aud) : ∀ (rest : List (Entry Aud)) (V : History Aud),
    (∀ a ∈ V.anns, a ∈ (rest.foldl (step C) V).anns) ∧
      ∀ b ∈ V.bounds, b ∈ (rest.foldl (step C) V).bounds
  | [], _ => ⟨fun _ h => h, fun _ h => h⟩
  | e :: rest, V => by
    obtain ⟨ha, hb⟩ := step_mono C V e
    obtain ⟨ha', hb'⟩ := fold_mono C rest (step C V e)
    exact ⟨fun a h => ha' a (ha a h), fun b h => hb' b (hb b h)⟩

theorem fold_bounds (C : Checks Aud) : ∀ (rest : List (Entry Aud)) (V : History Aud),
    ∀ b ∈ (rest.foldl (step C) V).bounds, b ∈ V.bounds ∨ ∃ e ∈ rest, b.entry = e.slot
  | [], _, _, hb => Or.inl hb
  | e :: rest, V, b, hb => by
    rcases fold_bounds C rest (step C V e) b hb with h | ⟨e', he', hs⟩
    · rcases step_bounds C V e b h with h' | hs
      · exact Or.inl h'
      · exact Or.inr ⟨e, List.mem_cons_self, hs⟩
    · exact Or.inr ⟨e', List.mem_cons_of_mem _ he', hs⟩

theorem below_step (C : Checks Aud) {h : History Aud} {e : Entry Aud} {s : Nat}
    (below : Below h s) (lt : e.slot < s) : Below (step C h e) s := by
  refine ⟨fun a ha => ?_, fun b hb => ?_⟩
  · rcases step_anns C h e a ha with h' | hs
    · exact below.1 a h'
    · omega
  · rcases step_bounds C h e b hb with h' | hs
    · exact below.2 b h'
    · omega

theorem sound_step (C : Checks Aud) {h : History Aud} (sound : Sound h) (e : Entry Aud)
    (below : Below h e.slot) : Sound (step C h e) := by
  cases e with
  | announce a =>
    by_cases ha : accepts C h a = true
    · obtain ⟨hlt, hcov, _⟩ := accepts_spec ha
      have hcov := coverAtMost_iff.mp hcov
      have fresh : ∀ p ∈ h.anns, p.slot < a.slot := below.1
      have bfresh : ∀ b ∈ h.bounds, b.entry < a.slot := below.2
      simp only [step, ha, ite_true]
      refine ⟨List.pairwise_cons.mpr ⟨fresh, sound.ordered⟩, fun p hp => ?_,
        fun r hr p hp hpa hps => ?_, fun b hb => ?_, fun b hb p hp hps hpa => ?_⟩
      · rcases List.mem_cons.mp hp with rfl | hp'
        · exact hlt
        · exact sound.below p hp'
      · rcases List.mem_cons.mp hr with rfl | hr'
        · rcases List.mem_cons.mp hp with rfl | hp'
          · omega
          · exact hcov p hp' hpa
        · rcases List.mem_cons.mp hp with rfl | hp'
          · have := fresh r hr'; omega
          · exact sound.monotone r hr' p hp' hpa hps
      · obtain ⟨hn, hs⟩ := sound.named b hb
        exact ⟨List.mem_cons_of_mem _ hn, hs⟩
      · rcases List.mem_cons.mp hp with rfl | hp'
        · have := bfresh b hb; omega
        · exact sound.passed b hb p hp' hps hpa
    · simp only [step, ha]
      exact sound
  | activate x =>
    cases hx : activation C h x with
    | none => simp only [step, hx]; exact sound
    | some b' =>
      obtain ⟨r, hn, hact, rfl⟩ := activation_spec hx
      obtain ⟨hrs, _, hcov, _⟩ := activates_spec hact
      have hcov := coverAtMost_iff.mp hcov
      simp only [step, hx]
      refine ⟨sound.ordered, sound.below, sound.monotone, fun b hb => ?_, fun b hb => ?_⟩
      · rcases List.mem_cons.mp hb with rfl | hb'
        · exact ⟨(named_spec hn).1, hrs⟩
        · exact sound.named b hb'
      · rcases List.mem_cons.mp hb with rfl | hb'
        · exact fun p hp _ hpa => hcov p hp hpa
        · exact sound.passed b hb'

theorem sound_fold (C : Checks Aud) : ∀ (rest : List (Entry Aud)) (V : History Aud),
    Sound V → (∀ e ∈ rest, Below V e.slot) → Increasing rest → Sound (rest.foldl (step C) V)
  | [], _, sound, _, _ => sound
  | e :: rest, V, sound, below, increasing => by
    rw [Increasing, List.pairwise_cons] at increasing
    exact sound_fold C rest _ (sound_step C sound e (below e List.mem_cons_self))
      (fun e' he' => below_step C (below e' (List.mem_cons_of_mem _ he')) (increasing.1 e' he'))
      increasing.2

omit [DecidableEq Aud] in
theorem sound_empty : Sound (History.empty : History Aud) :=
  ⟨List.Pairwise.nil, by simp [History.empty], by simp [History.empty],
    by simp [History.empty], by simp [History.empty]⟩

/-- Replaying `es` from `h` accepts every announcement among them: what a
device stores holds only the announcements it accepted. -/
def AllAccepted (C : Checks Aud) : History Aud → List (Entry Aud) → Prop
  | _, [] => True
  | h, .announce a :: rest => accepts C h a = true ∧ AllAccepted C (step C h (.announce a)) rest
  | h, .activate x :: rest => AllAccepted C (step C h (.activate x)) rest

theorem allAccepted_append (C : Checks Aud) : ∀ (l : List (Entry Aud)) (h : History Aud)
    (e : Entry Aud), AllAccepted C h l → stores C (l.foldl (step C) h) e = true →
    AllAccepted C h (l ++ [e])
  | [], h, e, _, hs => by
    cases e with
    | announce a => exact ⟨hs, trivial⟩
    | activate x => trivial
  | .announce a :: l, h, e, ⟨ha, rest⟩, hs => ⟨ha, allAccepted_append C l _ e rest hs⟩
  | .activate x :: l, h, e, rest, hs => allAccepted_append C l _ e rest hs

/-- **A raise whose named announcement survives forgetting is refused alike.**
In a replay, suppose a raise or reset names `r`, no applied raise or reset
of its audience passed `r`, and an announcement `M` before the raise covers
more than `r`. If `M` is forgettable, so is `r`: `r` is older than `M` (coverage
never drops in slot order), so it is below `first` and older than `M`'s newer
ordinary one, and no applied raise or reset names it, since `M` would have
refused one after it and the raise's own check would refuse one before. -/
theorem forgettable_of_passed {H V : History Aud} (soundH : Sound H) {first : Nat}
    {journal : List Nat} (x : Activation) {r M : Announcement Aud}
    (hrH : r ∈ H.anns) (hMH : M ∈ H.anns) (hMslot : M.slot < x.slot)
    (prefixBounds : ∀ b ∈ H.bounds, b.entry < x.slot → b ∈ V.bounds)
    (hbounds : boundsAtMost V.bounds r.aud r.covers = true)
    (hpurpose : r.purpose = x.kind.purpose)
    (hMa : M.aud = r.aud) (hMc : r.covers < M.covers)
    (hF : forgettable first journal H M = true) : forgettable first journal H r = true := by
  obtain ⟨_, _, hMfirst, ⟨o, ho, hoa, hop, hos⟩, _⟩ := forgettable_iff.mp hF
  have older : r.slot < M.slot := by
    rcases Nat.lt_trichotomy r.slot M.slot with h | h | h
    · exact h
    · have := slot_inj soundH.ordered hrH hMH h; subst this; omega
    · have := soundH.monotone r hrH M hMH hMa h; omega
  have hrbelow := soundH.below r hrH
  refine forgettable_iff.mpr ⟨fun b hb heq => ?_, ⟨M, hMH, hMa, hMc⟩, by omega,
    ⟨o, ho, hoa.trans hMa, hop, by omega⟩, Or.inl ?_⟩
  · obtain ⟨hbn, hbs⟩ := soundH.named b hb
    have hbr : b.named = r := slot_inj soundH.ordered hbn hrH heq
    have early : b.entry ≤ M.slot := by
      apply Nat.le_of_not_lt
      intro hlt
      have := soundH.passed b hb M hMH hlt (by rw [hbr]; exact hMa)
      rw [hbr] at this; omega
    have := boundsAtMost_iff.mp hbounds b (prefixBounds b hb (by omega)) (by rw [hbr])
    rw [hbr] at hbs; omega
  · rw [hpurpose]; cases x.kind <;> simp [Kind.purpose]

theorem coverAtMost_filter (F : Announcement Aud → Bool) {anns : List (Announcement Aud)}
    {A : Aud} {c : Nat} (h : coverAtMost anns A c = true) :
    coverAtMost (anns.filter fun r => !F r) A c = true :=
  coverAtMost_iff.mpr fun p hp hpa => coverAtMost_iff.mp h p (List.mem_filter.mp hp).1 hpa

/-- One raise or reset replayed without the forgettable announcements gets the
answer it gets with them. -/
theorem activation_filtered (C : Checks Aud) {H V : History Aud} (soundH : Sound H)
    (soundV : Sound V) (first : Nat) (journal : List Nat) (x : Activation)
    (sub : ∀ a ∈ V.anns, a ∈ H.anns) (below : Below V x.slot)
    (prefixBounds : ∀ b ∈ H.bounds, b.entry < x.slot → b ∈ V.bounds)
    (later : ∀ b ∈ (step C V (.activate x)).bounds, b ∈ H.bounds) :
    activation C (without (forgettable first journal H) V) x = activation C V x := by
  have lookup := named_sublist (List.filter_sublist (p := fun r => !(forgettable first journal H) r) (l := V.anns))
    soundV.ordered x
  unfold activation
  cases hV : named V.anns x with
  | none => rw [show named (without (forgettable first journal H) V).anns x = none from lookup.1 hV]; rfl
  | some r =>
    have hrV := (named_spec hV).1
    by_cases hF : (forgettable first journal H) r = true
    · have hnone : named (without (forgettable first journal H) V).anns x = none :=
        (lookup.2 r hV).2 fun h => by simp [hF] at h
      rw [hnone]
      by_cases hact : activates C V x r = true
      · exfalso
        have hb : (⟨x.slot, r⟩ : Boundary Aud) ∈ (step C V (.activate x)).bounds := by
          have : activation C V x = some ⟨x.slot, r⟩ := by
            simp [activation, hV, hact]
          simp [step, this]
        have unnamed := (forgettable_iff.mp hF).1 _ (later _ hb)
        exact unnamed rfl
      · simp [hact]
    · have hkept : r ∈ (without (forgettable first journal H) V).anns := List.mem_filter.mpr ⟨hrV, by simp [hF]⟩
      have hsome : named (without (forgettable first journal H) V).anns x = some r :=
        (lookup.2 r hV).1 hkept
      rw [hsome, Option.bind_some]
      have same : activates C (without (forgettable first journal H) V) x r = activates C V x r := by
        cases hact : activates C V x r with
        | true =>
          obtain ⟨_, _, hcov, _⟩ := activates_spec hact
          simp only [activates, Bool.and_eq_true] at hact ⊢
          exact ⟨⟨hact.1.1, coverAtMost_filter (forgettable first journal H) hcov⟩, hact.2⟩
        | false =>
          cases hK : activates C (without (forgettable first journal H) V) x r with
          | false => rfl
          | true =>
            exfalso
            obtain ⟨hslot, hpurpose, hcovK, hbounds⟩ := activates_spec hK
            have hcovV : coverAtMost V.anns r.aud r.covers = false := by
              cases hc : coverAtMost V.anns r.aud r.covers with
              | false => rfl
              | true =>
                have : activates C V x r = true := by
                  simp only [activates, Bool.and_eq_true] at hK ⊢
                  exact ⟨⟨hK.1.1, hc⟩, hK.2⟩
                rw [hact] at this; cases this
            have hex : ∃ M ∈ V.anns, M.aud = r.aud ∧ r.covers < M.covers := by
              apply Classical.byContradiction
              intro hno
              have : coverAtMost V.anns r.aud r.covers = true := coverAtMost_iff.mpr
                fun p hp hpa => Nat.le_of_not_lt fun hlt => hno ⟨p, hp, hpa, hlt⟩
              rw [this] at hcovV; cases hcovV
            obtain ⟨M, hMV, hMa, hMc⟩ := hex
            have hMF : (forgettable first journal H) M = true := by
              cases hm : (forgettable first journal H) M with
              | true => rfl
              | false =>
                have := coverAtMost_iff.mp hcovK M (List.mem_filter.mpr ⟨hMV, by simp [hm]⟩) hMa
                omega
            have := forgettable_of_passed soundH x (sub r hrV) (sub M hMV) (below.1 M hMV)
              prefixBounds hbounds hpurpose hMa hMc hMF
            exact hF this
      rw [same]
      rfl

/-- **Replaying without the forgettable announcements gives the history
without them.** Replaying the entries that remain after the announcements
forgettable in the replay's result `H` are dropped accepts each remaining
announcement and gives each raise or reset its answer, so it ends at `H`
without them. -/
theorem filtered_replay (C : Checks Aud) {H : History Aud} (soundH : Sound H) (first : Nat)
    (journal : List Nat) : ∀ (rest : List (Entry Aud)) (V : History Aud),
      rest.foldl (step C) V = H → Sound V → Increasing rest →
      (∀ e ∈ rest, Below V e.slot) → AllAccepted C V rest →
      (rest.filter (survives (forgettable first journal H))).foldl (step C)
          (without (forgettable first journal H) V) = without (forgettable first journal H) H ∧
        AllAccepted C (without (forgettable first journal H) V)
          (rest.filter (survives (forgettable first journal H)))
  | [], V, hH, _, _, _, _ => by subst hH; exact ⟨rfl, trivial⟩
  | e :: rest, V, hH, soundV, increasing, below, accepted => by
    rw [Increasing, List.pairwise_cons] at increasing
    have hH' : rest.foldl (step C) (step C V e) = H := hH
    have soundV' := sound_step C soundV e (below e List.mem_cons_self)
    have below' : ∀ e' ∈ rest, Below (step C V e) e'.slot := fun e' he' =>
      below_step C (below e' (List.mem_cons_of_mem _ he')) (increasing.1 e' he')
    cases e with
    | announce a =>
      obtain ⟨ha, accepted'⟩ := accepted
      have ih := filtered_replay C soundH first journal rest _ hH' soundV' increasing.2 below'
        accepted'
      by_cases hF : forgettable first journal H a = true
      · have hsame : without (forgettable first journal H) (step C V (.announce a)) =
            without (forgettable first journal H) V := by
          simp [without, step, ha, hF]
        simp only [List.filter_cons, survives, hF, Bool.not_true, Bool.false_eq_true, ite_false]
        rw [← hsame]
        exact ih
      · have hK : accepts C (without (forgettable first journal H) V) a = true := by
          obtain ⟨hlt, hcov, hbd⟩ := accepts_spec ha
          simp only [accepts, Bool.and_eq_true, decide_eq_true_eq] at ha ⊢
          exact ⟨⟨⟨hlt, coverAtMost_filter _ hcov⟩, hbd⟩, ha.2⟩
        have hstep : step C (without (forgettable first journal H) V) (.announce a) =
            without (forgettable first journal H) (step C V (.announce a)) := by
          simp only [step, hK, ha, ite_true]
          simp [without, hF]
        simp only [List.filter_cons, survives, hF, Bool.not_false, ite_true, List.foldl_cons]
        refine ⟨?_, hK, ?_⟩
        · rw [hstep]; exact ih.1
        · show AllAccepted C (step C _ (.announce a)) _
          rw [hstep]; exact ih.2
    | activate x =>
      have ih := filtered_replay C soundH first journal rest _ hH' soundV' increasing.2 below'
        accepted
      have sub : ∀ a ∈ V.anns, a ∈ H.anns := fun a h => hH ▸ (fold_mono C _ V).1 a h
      have later : ∀ b ∈ (step C V (.activate x)).bounds, b ∈ H.bounds :=
        fun b h => hH' ▸ (fold_mono C rest _).2 b h
      have prefixBounds : ∀ b ∈ H.bounds, b.entry < x.slot → b ∈ V.bounds := by
        intro b hb hlt
        rcases fold_bounds C (.activate x :: rest) V b (hH ▸ hb) with h | ⟨e', he', hs⟩
        · exact h
        · rcases List.mem_cons.mp he' with rfl | he''
          · change b.entry = x.slot at hs; omega
          · have h2 : x.slot < e'.slot := increasing.1 e' he''
            omega
      have hact := activation_filtered C soundH soundV first journal x sub
        (below _ List.mem_cons_self) prefixBounds later
      have hstep : step C (without (forgettable first journal H) V) (.activate x) =
          without (forgettable first journal H) (step C V (.activate x)) := by
        simp only [step, hact]
        split <;> rfl
      simp only [List.filter_cons, survives, ite_true, List.foldl_cons]
      refine ⟨?_, ?_⟩
      · rw [hstep]; exact ih.1
      · show AllAccepted C (step C _ (.activate x)) _
        rw [hstep]; exact ih.2

/-- What a device's stored originals give: its history, from originals each
accepted on replay, in slot order. -/
structure Rebuilds (C : Checks Aud) (d : Device Aud) : Prop where
  history : rebuild C d.stored = d.history
  accepted : AllAccepted C History.empty d.stored
  increasing : Increasing d.stored

omit [DecidableEq Aud] in
theorem below_empty (s : Nat) : Below (History.empty : History Aud) s := by
  simp [Below, History.empty]

theorem rebuilds_events (C : Checks Aud) : ∀ (events : List (Event Aud)) (d : Device Aud),
    Rebuilds C d → (∀ e ∈ d.stored, ∀ x ∈ entriesOf events, e.slot < x.slot) →
    Increasing (entriesOf events) → Rebuilds C (events.foldl (Device.next C) d)
  | [], _, r, _, _ => r
  | .entry x :: rest, d, r, fresh, increasing => by
    simp only [entriesOf, Increasing, List.pairwise_cons] at increasing fresh
    simp only [List.foldl_cons]
    refine rebuilds_events C rest _ ?_ ?_ increasing.2
    · by_cases hs : stores C d.history x = true
      · simp only [Device.next, hs, ite_true]
        refine ⟨?_, allAccepted_append C d.stored _ x r.accepted (by rw [← r.history] at hs; exact hs),
          List.pairwise_append.mpr ⟨r.increasing, List.pairwise_singleton _ _, fun a ha b hb => ?_⟩⟩
        · simp [rebuild, List.foldl_append, ← r.history]
        · rw [List.mem_singleton.mp hb]; exact fresh a ha x List.mem_cons_self
      · simp only [Device.next, hs]
        refine ⟨?_, r.accepted, r.increasing⟩
        cases x with
        | announce a =>
          simp only [stores] at hs
          simp [step, hs, r.history]
        | activate x' => simp [stores] at hs
    · by_cases hs : stores C d.history x = true
      · simp only [Device.next, hs, ite_true]
        intro e he x' hx'
        rcases List.mem_append.mp he with he | he
        · exact fresh e he x' (List.mem_cons_of_mem _ hx')
        · rw [List.mem_singleton.mp he]; exact increasing.1 x' hx'
      · simp only [Device.next, hs]
        exact fun e he x' hx' => fresh e he x' (List.mem_cons_of_mem _ hx')
  | .forget first journal :: rest, d, r, fresh, increasing => by
    simp only [entriesOf] at increasing fresh
    simp only [List.foldl_cons]
    have soundH : Sound d.history := r.history ▸
      sound_fold C d.stored History.empty sound_empty (fun _ _ => below_empty _) r.increasing
    have replay := filtered_replay C soundH first journal d.stored History.empty r.history
      sound_empty r.increasing (fun _ _ => below_empty _) r.accepted
    exact rebuilds_events C rest (Device.next C d (.forget first journal))
      ⟨replay.1, replay.2, r.increasing.sublist List.filter_sublist⟩
      (fun e he x hx => fresh e (List.mem_filter.mp he).1 x hx) increasing

/-- **Rebuilding from the stored originals gives the device's history.** A
device that stores only the announcements it accepts, and forgets the
forgettable ones' originals with them, rebuilds exactly the history it
holds, however its entries and its forgetting interleave. -/
theorem rebuilt_same (C : Checks Aud) (events : List (Event Aud))
    (increasing : Increasing (entriesOf events)) :
    rebuild C (events.foldl (Device.next C) Device.empty).stored =
      (events.foldl (Device.next C) Device.empty).history :=
  (rebuilds_events C events Device.empty ⟨rfl, trivial, List.Pairwise.nil⟩
    (fun _ h => by simp [Device.empty] at h) increasing).history

/-! ## Why the greatest coverage is kept -/

def openChecks : Checks Nat := ⟨fun _ _ => true, fun _ _ _ => true, fun _ _ _ => false⟩

/-- Ana's raise announcement at slot 50 covers 49; Ben's ordinary one at slot
60 covers 49 too. -/
def raiseAt50 : Announcement Nat := ⟨50, 0, 49, .raise 2, 7⟩
def ordinaryAt60 : Announcement Nat := ⟨60, 0, 49, .ordinary, 8⟩

/-- **A raise announcement matched in coverage stays.** Ana's raise entry at
slot 120 applies on a device holding both announcements: nothing passed 49.
Without her announcement it would be refused, so a device that forgot it
would part from the others. It is never forgettable: no announcement covers
more. -/
theorem equal_coverage_kept :
    (activation openChecks ⟨[ordinaryAt60, raiseAt50], []⟩ ⟨120, 50, .raise 2⟩).isSome = true ∧
    (activation openChecks ⟨[ordinaryAt60], []⟩ ⟨120, 50, .raise 2⟩).isSome = false ∧
    forgettable 100 [] ⟨[ordinaryAt60, raiseAt50], []⟩ raiseAt50 = false := by
  decide

/-! ## What a reader loads stays -/

theorem kept_of_forgettable_false {first : Nat} {journal : List Nat} {h : History Aud}
    {r : Announcement Aud} (hr : r ∈ h.anns) (kept : forgettable first journal h r = false) :
    r ∈ (without (forgettable first journal h) h).anns :=
  List.mem_filter.mpr ⟨hr, by simp [kept]⟩

/-- **A floor names announcements at or after `first`.** A floor write, the
current floor's or a journaled one, names each audience's newest applicable
announcement and keeps `first` at or above the current floor's
(`Retention.floor_write_rules`). Each announcement is after its coverage, so
each name is at or after the `first` a device forgets by. -/
theorem floor_names_kept {A : Type} (relevant : List A)
    (catalog : A → List Retention.Snapshot) (usable : A → Retention.Snapshot → Bool)
    (grace time current first : Nat) (covers : A → Nat)
    (ordered : ∀ a ∈ relevant,
      (catalog a).Pairwise (fun newer older => older.covers ≤ newer.covers))
    (kept : ∀ a ∈ relevant, current ≤ covers a + 1)
    (named : ∀ a ∈ relevant, ∃ s, Retention.newest (catalog a) (usable a) = some s ∧
      covers a ≤ s.covers)
    (announced : ∀ a ∈ relevant, ∀ s ∈ catalog a, s.covers < s.slot)
    (behind : first ≤ current) :
    ∀ a ∈ relevant, ∃ s, Retention.newest (catalog a) (usable a) = some s ∧ first ≤ s.slot := by
  obtain ⟨hcur, hnames⟩ :=
    Retention.floor_write_rules relevant catalog usable grace time current covers ordered kept named
  intro a ha
  obtain ⟨s, hs, _, hle⟩ := hnames a ha
  have hmem : s ∈ catalog a := List.mem_of_find?_eq_some hs
  have := announced a ha s hmem
  exact ⟨s, hs, by omega⟩

omit [DecidableEq Aud] in
theorem find_newest : ∀ {l : List (Announcement Aud)}, Ordered l →
    ∀ {p : Announcement Aud → Bool} {r : Announcement Aud}, l.find? p = some r →
      ∀ o ∈ l, p o = true → o.slot ≤ r.slot
  | [], _, _, _, h, _, _, _ => by simp at h
  | head :: tail, ordered, p, r, h, o, ho, hp => by
    rw [Ordered, List.pairwise_cons] at ordered
    by_cases hh : p head = true
    · simp only [List.find?_cons, hh, Option.some.injEq] at h
      subst h
      rcases List.mem_cons.mp ho with rfl | ho'
      · exact Nat.le_refl _
      · have := ordered.1 o ho'; omega
    · simp only [List.find?_cons, hh] at h
      rcases List.mem_cons.mp ho with rfl | ho'
      · exact absurd hp hh
      · exact find_newest ordered.2 h o ho' hp

/-- **The newest announcements stay.** The newest announcement of an
audience, which a circle reload starts from, and the newest usable one, which
a snapshot request and the floor name, are never forgettable: no ordinary one
of the audience is newer. Usable means ordinary, or named by an applied raise
or reset. -/
theorem newest_kept {first : Nat} {journal : List Nat} {h : History Aud}
    (ordered : Ordered h.anns) (A : Aud) (usable : Announcement Aud → Bool)
    (ordinary : ∀ a, a.purpose = .ordinary → usable a = true)
    (activated : ∀ a, usable a = true → a.purpose ≠ .ordinary →
      ∃ b ∈ h.bounds, b.named.slot = a.slot) {r : Announcement Aud}
    (newest : h.anns.find? (fun a => a.aud == A) = some r ∨
      h.anns.find? (fun a => a.aud == A && usable a) = some r) :
    forgettable first journal h r = false := by
  cases hf : forgettable first journal h r with
  | false => rfl
  | true =>
    obtain ⟨unnamed, _, _, ⟨o, ho, hoa, hop, hos⟩, _⟩ := forgettable_iff.mp hf
    rcases newest with hn | hn
    · have hra : r.aud = A := by simpa using List.find?_some hn
      have := find_newest ordered hn o ho (by simp [hoa, hra])
      omega
    · have hspec := List.find?_some hn
      simp only [Bool.and_eq_true, beq_iff_eq] at hspec
      by_cases hrp : r.purpose = .ordinary
      · have := find_newest ordered hn o ho (by simp [hoa, hspec.1, ordinary o hop])
        omega
      · obtain ⟨b, hb, hbs⟩ := activated r hspec.2 hrp
        exact absurd hbs (unnamed b hb)

/-! ## Listing snapshot folders -/

/-- The bodies a listing deletes: each listed one that no kept announcement
names and no open operation names (`journaled`: a snapshot being captured,
uploaded, announced or activated, a raise or reset waiting for its entry or
its undo, and a body queued for deletion). -/
def deleted (kept : List (Announcement Aud)) (journaled : List Nat) (bodies : List Nat) :
    List Nat :=
  bodies.filter fun b => !(kept.any fun r => r.body == b) && !journaled.contains b

omit [DecidableEq Aud] in
/-- **A listing spares every kept or journaled body.** With the theorems
above, that includes every body a reader can load: a floor's names, the
newest and newest usable of each audience, and the bodies applied raises and
resets name. File deletion reads the same kept announcements, so it protects
those bodies' files. -/
theorem listing_spares (kept : List (Announcement Aud)) (journaled bodies : List Nat)
    {b : Nat} (spared : (∃ r ∈ kept, r.body = b) ∨ b ∈ journaled) :
    b ∉ deleted kept journaled bodies := by
  intro hb
  simp only [deleted, List.mem_filter, Bool.and_eq_true, Bool.not_eq_true',
    List.any_eq_false, beq_iff_eq] at hb
  rcases spared with ⟨r, hr, rfl⟩ | hj
  · exact hb.2.1 r hr rfl
  · simp [hj] at hb

omit [DecidableEq Aud] in
/-- **A listing deletes every other body it lists.** A body whose
announcement was forgotten, and that no open operation names, goes with the
next listing of its folder. -/
theorem listing_finds_superseded (kept : List (Announcement Aud)) (journaled bodies : List Nat)
    {b : Nat} (listed : b ∈ bodies) (unnamed : ∀ r ∈ kept, r.body ≠ b) (open_ : b ∉ journaled) :
    b ∈ deleted kept journaled bodies := by
  simp only [deleted, List.mem_filter, Bool.and_eq_true, Bool.not_eq_true',
    List.any_eq_false, beq_iff_eq]
  exact ⟨listed, fun r hr => unnamed r hr, by simpa using open_⟩

end CovenSequence.Announcements
