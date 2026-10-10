import CovenStorelog.Model

namespace CovenStorelog

theorem lookup_some_mem [DecidableEq α] {xs : List (α × β)} {k : α} {v : β}
    (h : lookup xs k = some v) : (k, v) ∈ xs := by
  induction xs with
  | nil => cases h
  | cons p xs ih =>
      obtain ⟨key, value⟩ := p
      by_cases he : key = k
      · subst key
        simp [lookup] at h
        subst value
        exact List.mem_cons_self
      · simp [lookup, he] at h
        exact List.mem_cons_of_mem _ (ih h)

theorem lookup_erase_same [DecidableEq α] (xs : List (α × β)) (k : α) :
    lookup (erase xs k) k = none := by
  induction xs with
  | nil => rfl
  | cons p xs ih =>
      obtain ⟨key, value⟩ := p
      by_cases he : key = k <;> simp_all [erase, lookup]

theorem lookup_erase_other [DecidableEq α] (xs : List (α × β)) (k q : α)
    (h : q ≠ k) : lookup (erase xs k) q = lookup xs q := by
  induction xs with
  | nil => rfl
  | cons p xs ih =>
      obtain ⟨key, value⟩ := p
      by_cases hk : key = k
      · subst key
        simp_all [erase, lookup, Ne.symm h]
      · by_cases hq : key = q <;> simp_all [erase, lookup]

theorem lookup_put [DecidableEq α] (xs : List (α × β)) (k q : α) (v : β) :
    lookup (put xs k v) q = if k = q then some v else lookup xs q := by
  by_cases h : k = q
  · simp [put, lookup, h]
  · simp only [put, lookup, h, ite_false]
    exact lookup_erase_other xs k q (Ne.symm h)

theorem member_put (s : State) (m q : Nat) (r : Role) (h : member s q = true) :
    member { s with members := put s.members m r } q = true := by
  simp only [member, lookup_put]
  split
  · rfl
  · exact h

theorem member_erase (s : State) (m q : Nat) (h : member s q = true) (hne : q ≠ m) :
    member { s with members := erase s.members m } q = true := by
  simpa [member, lookup_erase_other _ _ _ hne] using h

/-- These are effects' invariants, not extra rejection conditions. -/
structure References (s : State) : Prop where
  devices : ∀ d m, (d, m) ∈ s.devices → member s m = true
  nonempty : ∀ c circle, (c, circle) ∈ s.circles → circle.members ≠ []
  circles : ∀ c circle, (c, circle) ∈ s.circles →
    ∀ m ∈ circle.members, member s m = true

theorem references_empty : References State.empty := by
  constructor <;> simp [State.empty]

theorem References.putMembers {s : State} (h : References s) (m : Nat) (role : Role) :
    References { s with members := put s.members m role } := by
  constructor
  · intro d q hd; exact member_put s m q role (h.devices d q hd)
  · exact h.nonempty
  · intro c circle hc q hq; exact member_put s m q role (h.circles c circle hc q hq)

theorem References.withDevices {s : State} (h : References s) (ds : List (Nat × Nat))
    (hd : ∀ d m, (d, m) ∈ ds → member s m = true) :
    References { s with devices := ds } := ⟨hd, h.nonempty, h.circles⟩

theorem References.withCircles {s : State} (h : References s) (cs : List (Nat × Circle))
    (hn : ∀ c circle, (c, circle) ∈ cs → circle.members ≠ [])
    (hc : ∀ c circle, (c, circle) ∈ cs → ∀ m ∈ circle.members, member s m = true) :
    References { s with circles := cs } := ⟨h.devices, hn, hc⟩

theorem References.putCircle {s : State} (h : References s) (c : Nat) (circle : Circle)
    (hn : circle.members ≠ []) (hc : ∀ m ∈ circle.members, member s m = true) :
    References { s with circles := put s.circles c circle } := by
  apply h.withCircles
  · intro d other ho
    rcases List.mem_cons.mp ho with he | ho
    · cases he; exact hn
    · exact h.nonempty d other (List.mem_filter.mp ho).1
  · intro d other ho m hm
    rcases List.mem_cons.mp ho with he | ho
    · cases he; exact hc m hm
    · exact h.circles d other (List.mem_filter.mp ho).1 m hm

theorem References.eraseCircle {s : State} (h : References s) (c : Nat) :
    References { s with circles := erase s.circles c } := by
  apply h.withCircles
  · intro d other ho; exact h.nonempty d other (List.mem_filter.mp ho).1
  · intro d other ho; exact h.circles d other (List.mem_filter.mp ho).1

theorem withoutMember_members {circle next : Circle} {m : Nat}
    (h : withoutMember circle m = some next) :
    next.members ≠ [] ∧ next.members = circle.members.filter (· != m) := by
  unfold withoutMember at h
  dsimp only at h
  split at h
  · cases h
  · cases h
    rename_i hn
    exact ⟨by simpa using hn, rfl⟩

theorem References.removeMember {s : State} (h : References s) (m : Nat) :
    References { s with
      members := erase s.members m,
      devices := s.devices.filter (fun p => p.2 != m),
      circles := removeFromCircles s.circles m } := by
  constructor
  · intro d q hd
    have hh := List.mem_filter.mp hd
    exact member_erase s m q (h.devices d q hh.1) (by simpa using hh.2)
  · intro c circle hc
    obtain ⟨⟨d, old⟩, _, he⟩ := List.mem_filterMap.mp hc
    obtain ⟨next, hn, hp⟩ := Option.map_eq_some_iff.mp he
    cases hp
    exact (withoutMember_members hn).1
  · intro c circle hc q hq
    obtain ⟨⟨d, old⟩, ho, he⟩ := List.mem_filterMap.mp hc
    obtain ⟨next, hn, hp⟩ := Option.map_eq_some_iff.mp he
    cases hp
    rw [(withoutMember_members hn).2] at hq
    have hq' := List.mem_filter.mp hq
    exact member_erase s m q (h.circles c old ho q hq'.1) (by simpa using hq'.2)

/-- Device and circle references are preserved by each effect itself. The
replay checks only authority, existence, remaining admins, and conflicts. -/
theorem effect_references {s t : State} {w : Nat} {e : Entry}
    (hs : References s) (h : effect s w e = some t) : References t := by
  cases ha : e.action with
  | create access =>
      simp only [effect, ha] at h
      split at h
      · cases h
      · cases h
        constructor
        · intro d m hm
          simp only [List.mem_singleton, Prod.mk.injEq] at hm
          obtain ⟨rfl, rfl⟩ := hm
          simp [member, lookup]
        · simp [State.empty]
        · simp [State.empty]
  | addMember m role access =>
      simp only [effect, ha] at h
      split at h
      · cases h
        have hh := hs.putMembers m role
        exact ⟨hh.devices, hh.nonempty, hh.circles⟩
      · cases h
  | setAccess m access =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact ⟨hs.devices, hs.nonempty, hs.circles⟩
      · cases h
  | changeRole m role =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact hs.putMembers m role
      · cases h
  | removeMember m circleKeys =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact hs.removeMember m
      · cases h
  | addDevice m d =>
      simp only [effect, ha] at h
      split at h
      · rename_i hg
        cases h
        apply hs.withDevices
        intro k q hq
        rcases List.mem_cons.mp hq with he | hq
        · cases he; exact (Bool.and_eq_true_iff.mp hg).1
        · exact hs.devices k q (List.mem_filter.mp hq).1
      · cases h
  | removeDevice m d =>
      simp only [effect, ha] at h
      split at h
      · cases h
        exact hs.withDevices _ (fun k q hq => hs.devices k q (List.mem_filter.mp hq).1)
      · cases h
  | makeCircle c name =>
      simp only [effect, ha] at h
      split at h
      · rename_i hg
        cases h
        apply hs.putCircle c ⟨name, [e.author]⟩ (by simp)
        intro m hm
        have he : m = e.author := List.mem_singleton.mp hm
        subst m
        exact (Bool.and_eq_true_iff.mp hg).1
      · cases h
  | renameCircle c name =>
      simp only [effect, ha] at h
      cases he : lookup s.circles c with
      | none => simp [he] at h
      | some circle =>
          simp only [he, bind, Option.bind, Option.some.injEq] at h
          subst t
          have hc := lookup_some_mem he
          exact hs.putCircle c _ (hs.nonempty c circle hc) (hs.circles c circle hc)
  | deleteCircle c =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact hs.eraseCircle c
      · cases h
  | addToCircle c m =>
      simp only [effect, ha] at h
      cases he : lookup s.circles c with
      | none => simp [he] at h
      | some circle =>
          simp only [he, bind, Option.bind] at h
          split at h
          · rename_i hm
            cases h
            apply hs.putCircle c _ (by simp)
            intro q hq
            rcases List.mem_cons.mp hq with hq | hq
            · subst q; exact hm
            · exact hs.circles c circle (lookup_some_mem he) q hq
          · cases h
  | removeFromCircle c m =>
      simp only [effect, ha] at h
      cases he : lookup s.circles c with
      | none => simp [he] at h
      | some circle =>
          simp only [he, bind, Option.bind] at h
          split at h
          · cases hn : withoutMember circle m with
            | none => simp only [hn, Option.some.injEq] at h; subst t; exact hs.eraseCircle c
            | some next =>
                simp only [hn, Option.some.injEq] at h
                subst t
                apply hs.putCircle c next (withoutMember_members hn).1
                intro q hq
                rw [(withoutMember_members hn).2] at hq
                exact hs.circles c circle (lookup_some_mem he) q (List.mem_filter.mp hq).1
          · cases h
  | raiseSchema version snapshot =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact ⟨hs.devices, hs.nonempty, hs.circles⟩
      · cases h
  | reset snapshot =>
      simp only [effect, ha] at h
      split at h
      · cases h; exact ⟨hs.devices, hs.nonempty, hs.circles⟩
      · cases h

  | rotateKey audience key =>
      simp only [effect, ha, Option.some.injEq] at h
      subst t
      exact hs

end CovenStorelog
