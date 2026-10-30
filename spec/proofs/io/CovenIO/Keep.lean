namespace CovenIO.Keep

/-! §17.1, §17.7: what an attempt at a pin coven keeps reads. An app file's
request keeps coven's copy until the cache copy is made from it; the attempt
reads that copy, checking each chunk against the hash its staging recorded
and the whole file against its content hash, and reads storage only if the
copy doesn't check out. A user's file's attempt reads storage. A user's
original is never read for the cache. -/

inductive Kind where
  | app | user
  deriving DecidableEq, Repr

/-- Where an attempt reads bytes from. -/
inductive Source where
  | covenCopy | storage | original
  deriving DecidableEq, Repr

/-- The sources one attempt reads, in order: `kept` says whether the request
still keeps coven's copy, `checks` whether that copy checks out. -/
def reads : Kind → Bool → Bool → List Source
  | .user, _, _ => [.storage]
  | .app, true, true => [.covenCopy]
  | .app, true, false => [.covenCopy, .storage]
  | .app, false, _ => [.storage]

/-- An app file whose copy checks out is kept with no request to storage. -/
theorem copy_reads_no_storage : Source.storage ∉ reads .app true true := by decide

/-- A copy that doesn't check out falls back to the download a pin makes. -/
theorem failed_copy_downloads : reads .app true false = [.covenCopy, .storage] := rfl

/-- A user's file downloads, as any pin does. -/
theorem user_downloads (kept checks : Bool) : reads .user kept checks = [.storage] := rfl

/-- No attempt ever reads a user's original for the cache. -/
theorem original_never_read (kind : Kind) (kept checks : Bool) :
    Source.original ∉ reads kind kept checks := by
  cases kind <;> cases kept <;> cases checks <;> decide

end CovenIO.Keep
