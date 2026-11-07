/-!
# The person's saved entries for one store: the order of their steps (§13.1)

A store's entries are the S3 key and the restore code. The code is what
`saved_restore_codes` lists, one copy per keychain group of the app. Two
operations take several keychain steps, and either can stop part way:

- `forget_saved_restore_code` deletes the S3 key, then the code. A failure part
  way leaves the store listed, never an unlisted key, and calling again
  finishes (`forget_cut_listed`, `forget_finishes`). `code_first_strands` is the
  counterexample: deleting the code first leaves a key no listing shows.
- Replacing an entry writes the first group's copy, then deletes the others
  one at a time. A failure part way leaves the entry ambiguous, never absent,
  and repeating the change finishes (`replace_cut_present`,
  `replace_cut_ambiguous`, `replace_finishes`). `delete_then_write_loses` is the
  counterexample: deleting the copies and then writing leaves the entry absent
  between the two.
-/
namespace CovenSequence.SavedEntries

variable {Bytes : Type}

/-- Whether an S3 key is saved, and the restore code's copies, one per keychain
group that holds one. -/
structure Entries (Bytes : Type) where
  s3 : Bool
  code : List Bytes
  deriving DecidableEq

/-- The listing shows the store while a copy of its code exists, readable or not. -/
def listed (k : Entries Bytes) : Bool := !k.code.isEmpty

/-- No key is saved for a store the listing doesn't show. -/
def NoUnlistedKey (k : Entries Bytes) : Prop := k.s3 = true → listed k = true

/-- What a read of the code finds. -/
inductive Read (Bytes : Type) where
  | absent
  | saved (bytes : Bytes)
  | ambiguous

def read : List Bytes → Read Bytes
  | [] => .absent
  | [b] => .saved b
  | _ :: _ :: _ => .ambiguous

/-- One keychain step. -/
inductive Step (Bytes : Type) where
  | deleteKey
  | deleteCode
  /-- Write the first group's copy. -/
  | write (bytes : Bytes)
  /-- Delete one of the other groups' copies. -/
  | dropOther

def Step.apply : Step Bytes → Entries Bytes → Entries Bytes
  | .deleteKey, k => { k with s3 := false }
  | .deleteCode, k => { k with code := [] }
  | .write b, k => { k with code := b :: k.code.drop 1 }
  | .dropOther, k =>
    { k with code := match k.code with | h :: _ :: t => h :: t | c => c }

/-- Run steps in order. -/
def run (k : Entries Bytes) (steps : List (Step Bytes)) : Entries Bytes :=
  steps.foldl (fun k s => s.apply k) k

/-- Forgetting: the S3 key first, then the code, which the listing shows. -/
def forgetPlan : List (Step Bytes) := [.deleteKey, .deleteCode]

/-- Replacing the entry with `b`: the first group's copy, then the others. -/
def replacePlan (b : Bytes) (k : Entries Bytes) : List (Step Bytes) :=
  .write b :: List.replicate (k.code.length - 1) .dropOther

/-- **A failure part way through forgetting leaves the store listed, never an
unlisted key.** -/
theorem forget_cut_listed (k : Entries Bytes) (inv : NoUnlistedKey k) (n : Nat) :
    NoUnlistedKey (run k ((forgetPlan : List (Step Bytes)).take n)) := by
  match n with
  | 0 => exact inv
  | 1 => simp [run, forgetPlan, Step.apply, NoUnlistedKey]
  | n + 2 => simp [run, forgetPlan, Step.apply, NoUnlistedKey, listed]

/-- **Calling again finishes.** From the state after any failure, forgetting
leaves neither entry, as it does from the start; an absent entry succeeds. -/
theorem forget_finishes (k : Entries Bytes) (n : Nat) :
    run (run k ((forgetPlan : List (Step Bytes)).take n)) forgetPlan = ⟨false, []⟩ := by
  match n with
  | 0 => simp [run, forgetPlan, Step.apply]
  | 1 => simp [run, forgetPlan, Step.apply]
  | n + 2 => simp [run, forgetPlan, Step.apply]

/-- **Counterexample: the code first.** Cut after the code is deleted, the key
stays and the listing no longer shows its store. -/
theorem code_first_strands :
    ∃ k : Entries Nat, NoUnlistedKey k ∧
      ¬ NoUnlistedKey (run k [Step.deleteCode]) :=
  ⟨⟨true, [0]⟩, by simp [NoUnlistedKey, listed], by simp [run, Step.apply, NoUnlistedKey, listed]⟩

theorem run_cons (k : Entries Bytes) (s : Step Bytes) (l : List (Step Bytes)) :
    run k (s :: l) = run (s.apply k) l := rfl

theorem dropOther_cons (s3 : Bool) (h : Bytes) (t : List Bytes) :
    Step.apply .dropOther (⟨s3, h :: t⟩ : Entries Bytes) = ⟨s3, h :: t.drop 1⟩ := by
  cases t <;> simp [Step.apply]

/-- Deleting `j` of the other groups' copies leaves the first and the rest. -/
theorem drops (s3 : Bool) (h : Bytes) :
    ∀ (j : Nat) (t : List Bytes),
      run ⟨s3, h :: t⟩ (List.replicate j .dropOther) = ⟨s3, h :: t.drop j⟩
  | 0, t => by simp [run]
  | j + 1, t => by
    rw [List.replicate_succ, run_cons, dropOther_cons, drops s3 h j (t.drop 1), List.drop_drop]
    simp [Nat.add_comm]

/-- After the write and `j` of the deletions, the entry is the new copy and what
is left of the others. -/
theorem replace_state (b : Bytes) (s3 : Bool) (c : List Bytes) (j : Nat) :
    run ⟨s3, c⟩ (.write b :: List.replicate j .dropOther) = ⟨s3, b :: (c.drop 1).drop j⟩ := by
  rw [run_cons]
  have := drops s3 b j (c.drop 1)
  simpa [Step.apply] using this

/-- The plan cut after `n + 1` steps. -/
theorem take_plan (b : Bytes) (k : Entries Bytes) (n : Nat) :
    (replacePlan b k).take (n + 1) =
      .write b :: List.replicate (min n (k.code.length - 1)) .dropOther := by
  simp [replacePlan, List.take_succ_cons, List.take_replicate]

/-- **A failure part way through replacing an entry that exists leaves it
present.** -/
theorem replace_cut_present (b : Bytes) (k : Entries Bytes) (there : k.code ≠ []) (n : Nat) :
    (run k ((replacePlan b k).take n)).code ≠ [] := by
  cases n with
  | zero => simpa [run] using there
  | succ n =>
    rw [take_plan]
    obtain ⟨s3, c⟩ := k
    simp [replace_state]

/-- **Cut before the end, with copies in several groups, the entry reads as
ambiguous: never absent, and never one of the old copies taken for the entry.** -/
theorem replace_cut_ambiguous (b : Bytes) (k : Entries Bytes) (several : 2 ≤ k.code.length)
    (n : Nat) (cut : n < (replacePlan b k).length) :
    read (run k ((replacePlan b k).take n)).code = Read.ambiguous := by
  obtain ⟨s3, c⟩ := k
  change 2 ≤ c.length at several
  have len : (replacePlan b ⟨s3, c⟩).length = c.length := by
    simp only [replacePlan, List.length_cons, List.length_replicate]
    change c.length - 1 + 1 = c.length
    omega
  cases n with
  | zero =>
    rcases c with _ | ⟨x, _ | ⟨y, t⟩⟩ <;> simp_all [run, read]
  | succ n =>
    rw [take_plan, replace_state]
    rw [len] at cut
    have two : 2 ≤ (b :: (c.drop 1).drop (min n (c.length - 1))).length := by
      simp only [List.length_cons, List.length_drop]
      omega
    revert two
    generalize (c.drop 1).drop (min n (c.length - 1)) = r
    cases r <;> simp [read]

/-- **Repeating the change finishes it**, from the state after any failure: one
copy, the new bytes. -/
theorem replace_finishes (b : Bytes) (k : Entries Bytes) (n : Nat) :
    let cut := run k ((replacePlan b k).take n)
    (run cut (replacePlan b cut)).code = [b] := by
  intro cut
  obtain ⟨s3, c⟩ := cut
  simp only [replacePlan]
  rw [replace_state]
  have : (c.drop 1).drop (c.length - 1) = [] :=
    List.drop_eq_nil_of_le (by rw [List.length_drop]; omega)
  show b :: (c.drop 1).drop (c.length - 1) = [b]
  rw [this]

/-- **Counterexample: delete, then write.** Cut after the deletion, an entry that
existed is absent, and a name rewrite of it then stays absent. -/
theorem delete_then_write_loses :
    ∃ k : Entries Nat, k.code ≠ [] ∧
      (run k (([Step.deleteCode, Step.write 1] : List (Step Nat)).take 1)).code = [] :=
  ⟨⟨false, [0]⟩, by simp, by simp [run, Step.apply]⟩

end CovenSequence.SavedEntries
