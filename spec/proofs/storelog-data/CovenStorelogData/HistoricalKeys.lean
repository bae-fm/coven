import CovenStorelogData.KeyPhases
import CovenStorelogData.Snapshots

/-! §9, §11, §14.4. The same accepted historical key opens parts and
snapshots. Membership replay changes neither authorization nor applied data. -/
namespace CovenStorelogData.HistoricalKeys
open CovenStorelog

/-- A receipt abstracts a successfully opened, identity/hash-checked member
copy. Merely listing another member's box is not a custody receipt. -/
def acquire (H : CurrentReplay.History) (W n : Nat) (received : EntrySet)
    (custody : List KeySelection.Key) (opened : KeySelection.Copies)
    (who : Nat) (key : KeySelection.Key) : Except Pending.Reason (List KeySelection.Key) :=
  if !KeySelection.receivedAuthorized H W n received key then .error .refused
  else if (key, who) ∉ opened then .error (.keyUnavailable key.audience key.key.id)
  else .ok ((custody ++ [key]).eraseDups)

theorem shared_copy_acquired (H : CurrentReplay.History) (W n : Nat) (received : EntrySet)
    (custody : List KeySelection.Key) (opened : KeySelection.Copies)
    (who : Nat) (key : KeySelection.Key)
    (authorized : KeySelection.receivedAuthorized H W n received key = true)
    (copy : (key, who) ∈ opened) :
    acquire H W n received custody opened who key = .ok ((custody ++ [key]).eraseDups) := by
  simp [acquire, authorized, copy]

theorem acquired_keys_retained (custody : List KeySelection.Key) (key old : KeySelection.Key)
    (held : old ∈ custody) : old ∈ (custody ++ [key]).eraseDups := by
  simp [held]

def openKey (H : CurrentReplay.History) (W n : Nat) (received : EntrySet)
    (custody : List KeySelection.Key) (who : Nat) (key : KeySelection.Key) :
    Except Pending.Reason Unit :=
  if !KeySelection.receivedAuthorized H W n received key then .error .refused
  else if who ∉ audienceMembers (CurrentReplay.resolve H W n received).state key.audience then
    .error .removed
  else if key ∉ custody then .error (.keyUnavailable key.audience key.key.id)
  else .ok ()

structure Reader (W Col : Type) where
  received : List Nat
  data : Reloaded W Col

def replay {W Col : Type} (reader : Reader W Col) (received : List Nat) : Reader W Col :=
  { reader with received }

def loadPart {W Col : Type} [DecidableEq W]
    (H : CurrentReplay.History) (window n who : Nat) (custody : List KeySelection.Key)
    (key : KeySelection.Key) (writes : CovenMerge.Writes W Row Col)
    (headers : W → Header W) (boundaries : List (Boundary W))
    (author : Author) (reader : Reader W Col) (w : W) :
    Except Pending.Reason (Reader W Col) := do
  if !author.entries.all (· ∈ reader.received) then
    throw (.waits (.object "store-log past"))
  let past := (CurrentReplay.resolve H window n (entrySet author.entries)).state
  if !(running past author.member author.device &&
      author.member ∈ audienceMembers past key.audience) then throw .refused
  let _ ← openKey H window n (entrySet reader.received) custody who key
  return { reader with data := applyDownloaded writes headers key.audience boundaries reader.data w }

def loadSnapshot {W Col : Type} (H : CurrentReplay.History) (window n who : Nat)
    (received : EntrySet) (custody : List KeySelection.Key) (key : KeySelection.Key)
    (snapshot : Snapshot W Col) : Except Pending.Reason (Reloaded W Col) := do
  let _ ← openKey H window n received custody who key
  return ⟨snapshot.data, snapshot.frozen, snapshot.rejected⟩

theorem membership_preserves_applied {W Col : Type} (reader : Reader W Col)
    (received : List Nat) : (replay reader received).data = reader.data := rfl

theorem historical_key_stays_readable (H : CurrentReplay.History) (window n who : Nat)
    (before after : EntrySet) (custody : List KeySelection.Key) (key : KeySelection.Key)
    (accepted : KeySelection.receivedAuthorized H window n before key = true)
    (retained : after key.entry = true) (held : key ∈ custody)
    (current : who ∈ audienceMembers (CurrentReplay.resolve H window n after).state key.audience) :
    openKey H window n after custody who key = .ok () := by
  simp [openKey, KeySelection.authorized_introduction_persists H window n
    before after key accepted retained, held, current]

theorem snapshots_stay_loadable {W Col : Type} (H : CurrentReplay.History) (window n who : Nat)
    (before after : EntrySet) (custody : List KeySelection.Key) (key : KeySelection.Key)
    (snapshot : Snapshot W Col)
    (accepted : KeySelection.receivedAuthorized H window n before key = true)
    (retained : after key.entry = true) (held : key ∈ custody)
    (current : who ∈ audienceMembers (CurrentReplay.resolve H window n after).state key.audience) :
    loadSnapshot H window n who after custody key snapshot =
      .ok ⟨snapshot.data, snapshot.frozen, snapshot.rejected⟩ := by
  simp [loadSnapshot, historical_key_stays_readable H window n who before after
    custody key accepted retained held current]
  rfl

theorem parts_stay_loadable {W Col : Type} [DecidableEq W]
    (H : CurrentReplay.History) (window n who : Nat) (before : EntrySet)
    (custody : List KeySelection.Key) (key : KeySelection.Key)
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (boundaries : List (Boundary W)) (author : Author) (reader : Reader W Col) (w : W)
    (accepted : KeySelection.receivedAuthorized H window n before key = true)
    (retained : entrySet reader.received key.entry = true) (held : key ∈ custody)
    (current : who ∈ audienceMembers
      (CurrentReplay.resolve H window n (entrySet reader.received)).state key.audience)
    (pastReceived : author.entries.all (· ∈ reader.received) = true)
    (writer : running (CurrentReplay.resolve H window n (entrySet author.entries)).state
      author.member author.device = true)
    (audience : author.member ∈ audienceMembers
      (CurrentReplay.resolve H window n (entrySet author.entries)).state key.audience) :
    loadPart H window n who custody key writes headers boundaries author reader w =
      .ok { reader with data := applyDownloaded writes headers key.audience boundaries reader.data w } := by
  simp [loadPart, pastReceived, writer, audience,
    historical_key_stays_readable H window n who before (entrySet reader.received)
      custody key accepted retained held current]
  rfl

end CovenStorelogData.HistoricalKeys
