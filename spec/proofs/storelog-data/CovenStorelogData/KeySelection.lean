import CovenStorelog.Model

/-! §11: immutable key introductions, member disclosure, and device-local
selection. An excluded device of a remaining member does not rotate keys (§13).
Cryptographic possession is abstracted by a receipt, not by membership. -/
namespace CovenStorelogData.KeySelection
open CovenStorelog

structure Key where
  id : Nat
  entry : Nat
  audience : Audience
  deriving DecidableEq, Repr

structure Knowledge where
  custody : List Key
  delivered : List (Key × Nat)
  retired : List Key
  deriving DecidableEq, Repr

def exposed (members : Audience → List Nat) (known : Knowledge) (key : Key) : Bool :=
  known.delivered.any fun (k, m) => k == key && m ∉ members key.audience

/-- Run after adopting membership or learning a delivery. Retired ids never
return to selection, even if the exposing entry later returns (§9, §11). -/
def learn (members : Audience → List Nat) (known : Knowledge)
    (receipts : List (Key × Nat)) : Knowledge :=
  let next := { known with delivered := known.delivered ++ receipts }
  { next with retired := known.retired ++
      (next.delivered.map Prod.fst).filter (exposed members next) }

def usable (members : Audience → List Nat) (authorized : Nat → Bool)
    (known : Knowledge) (key : Key) : Bool :=
  authorized key.entry && key ∈ known.custody && key ∉ known.retired &&
    !exposed members known key

def newer (a b : Key) : Bool :=
  b.entry < a.entry || (b.entry == a.entry && b.id < a.id)

/-- Keys are ordered by introduction timestamp (the entry id in Appendix C),
then id. Multiple concurrent rotations remain candidates. -/
def newest : List Key → Option Key
  | [] => none
  | k :: ks => match newest ks with
    | none => some k
    | some other => some (if newer k other then k else other)

def select (members : Audience → List Nat) (authorized : Nat → Bool)
    (known : Knowledge) (who : Nat) (audience : Audience) : Option Key :=
  if who ∈ members audience then
    newest (known.custody.filter fun key => key.audience == audience &&
      usable members authorized known key)
  else none

/-- §11 permits sharing historical keys, including retired ones, with the
current audience. The caller checks that the device has not stopped (§10).
The completed receipt updates the sharing device's knowledge only. -/
def share (members : Audience → List Nat) (known : Knowledge)
    (recipient : Nat) (key : Key) : Option Knowledge :=
  if key ∈ known.custody ∧ recipient ∈ members key.audience then
    some { known with delivered := known.delivered ++ [(key, recipient)] }
  else none

theorem newest_mem {keys : List Key} {key : Key} (h : newest keys = some key) :
    key ∈ keys := by
  induction keys with
  | nil => cases h
  | cons k ks ih =>
      simp only [newest] at h
      cases hn : newest ks with
      | none => simp [hn] at h; subst key; simp
      | some other =>
          simp only [hn, Option.some.injEq] at h
          split at h
          · subst key; simp
          · subst key; exact List.mem_cons_of_mem _ (ih hn)

theorem newer_trans {a b c : Key} (ab : newer a b = true) (bc : newer b c = true) :
    newer a c = true := by
  simp only [newer, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq] at *
  omega

theorem newest_maximal {keys : List Key} {key : Key} (h : newest keys = some key) :
    ∀ other ∈ keys, newer other key = false := by
  induction keys generalizing key with
  | nil => cases h
  | cons k ks ih =>
      simp only [newest] at h
      cases hn : newest ks with
      | none =>
          have empty : ks = [] := by
            cases ks with
            | nil => rfl
            | cons a rest =>
                simp only [newest] at hn
                cases hr : newest rest <;> simp [hr] at hn
          subst ks
          simp only [hn, Option.some.injEq] at h
          subst key
          simp [newer]
      | some winner =>
          have greatest := ih hn
          simp only [hn, Option.some.injEq] at h
          split at h
          · rename_i wins
            subst key
            intro other present
            rcases List.mem_cons.mp present with rfl | present
            · simp [newer]
            · cases ho : newer other k
              · rfl
              · have ht := newer_trans ho wins
                simp [greatest other present] at ht
          · rename_i loses
            subst key
            intro other present
            rcases List.mem_cons.mp present with rfl | present
            · exact Bool.eq_false_iff.mpr loses
            · exact greatest other present

theorem selection_is_newest (members : Audience → List Nat) (authorized : Nat → Bool)
    (known : Knowledge) (who : Nat) (audience : Audience) (key : Key)
    (selected : select members authorized known who audience = some key)
    (other : Key) (held : other ∈ known.custody) (same : other.audience = audience)
    (allowed : usable members authorized known other = true) : newer other key = false := by
  unfold select at selected
  split at selected
  · exact newest_maximal selected other (List.mem_filter.mpr ⟨held, by simp [same, allowed]⟩)
  · cases selected

theorem first_attempt_safe (members : Audience → List Nat) (authorized : Nat → Bool)
    (known : Knowledge) (who : Nat) (audience : Audience) (key : Key)
    (sent : select members authorized known who audience = some key) :
    who ∈ members audience ∧ key.audience = audience ∧
    authorized key.entry = true ∧ key ∈ known.custody ∧ key ∉ known.retired ∧
    exposed members known key = false := by
  unfold select at sent
  split at sent
  · rename_i hm
    have h := (List.mem_filter.mp (newest_mem sent)).2
    simp only [Bool.and_eq_true, beq_iff_eq, usable, decide_eq_true_eq,
      Bool.not_eq_true'] at h
    exact ⟨hm, h.1, h.2.1.1.1, h.2.1.1.2, h.2.1.2, h.2.2⟩
  · cases sent

theorem known_excluded_cannot_read (members : Audience → List Nat)
    (authorized : Nat → Bool) (known : Knowledge) (who : Nat) (audience : Audience)
    (key : Key) (sent : select members authorized known who audience = some key)
    (excluded : m ∉ members audience) : (key, m) ∉ known.delivered := by
  intro delivered
  have hs := first_attempt_safe members authorized known who audience key sent
  have hx : exposed members known key = true := by
    apply List.any_eq_true.mpr
    exact ⟨(key, m), delivered, by simp [hs.2.1, excluded]⟩
  simp [hs.2.2.2.2.2] at hx

theorem retirement_persists (members : Audience → List Nat) (known : Knowledge)
    (receipts : List (Key × Nat)) (key : Key) (h : key ∈ known.retired) :
    key ∈ (learn members known receipts).retired := List.mem_append_left _ h

theorem observed_exposure_retires (members : Audience → List Nat) (known : Knowledge)
    (receipts : List (Key × Nat)) (key : Key) (m : Nat)
    (delivered : (key, m) ∈ known.delivered ++ receipts)
    (excluded : m ∉ members key.audience) :
    key ∈ (learn members known receipts).retired := by
  apply List.mem_append_right
  apply List.mem_filter.mpr
  constructor
  · exact List.mem_map.mpr ⟨(key, m), delivered, rfl⟩
  · apply List.any_eq_true.mpr
    exact ⟨(key, m), delivered, by simp [excluded]⟩

structure Attempt where
  key : Key
  bytes : List Nat
  deriving DecidableEq, Repr

/-- §3 and §18: an attempted object retries its original key and bytes. -/
def retry (attempt : Attempt) : Attempt := attempt

theorem retry_fixed (attempt : Attempt) :
    (retry attempt).key = attempt.key ∧ (retry attempt).bytes = attempt.bytes := ⟨rfl, rfl⟩

/-- This extra premise is exactly what would turn local knowledge into actual
secrecy. §11 does not supply it for unobserved historical-key sharing. -/
theorem revocation_with_complete_knowledge (members : Audience → List Nat)
    (authorized : Nat → Bool) (known : Knowledge) (actual : List (Key × Nat))
    (complete : ∀ receipt ∈ actual, receipt ∈ known.delivered)
    (who : Nat) (audience : Audience) (key : Key)
    (sent : select members authorized known who audience = some key)
    (excluded : m ∉ members audience) : (key, m) ∉ actual := by
  exact fun h => known_excluded_cannot_read members authorized known who audience key sent
    excluded (complete _ h)

end CovenStorelogData.KeySelection
