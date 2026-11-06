namespace CovenIO.Keep

/-! §17.1, §17.8: what an attempt at a pin coven keeps reads. An app file's
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
  | covenCopy | storage | original | cache
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

/-! §17.1, §17.7: where a read's bytes come from. A read opens the file where
its record says it is: a file on this device only reads its bytes here, a
user's original or coven's copy of an app file; a stored file reads the cache
or storage; a file on another device only fails with `DeviceOnly`. Opening
finds the place from the record, then opens the bytes, and the upload can
complete in between: bytes missing at the open are judged by the record as it
stands then. -/

/-- Where a record keeps a file. -/
inductive Place where
  | here | elsewhere | stored
  deriving DecidableEq, Repr

/-- The sources a read of a file at a place reads. -/
def readSources : Kind → Place → List Source
  | .user, .here => [.original]
  | .app, .here => [.covenCopy]
  | _, .stored => [.cache, .storage]
  | _, .elsewhere => []

/-- A read that found the file at `found`, opening it while the record says
`now`, with its bytes `there` or not: what it reads, or `none` when it fails
because the record still names bytes that are missing. -/
def openRead (kind : Kind) (found now : Place) (there : Bool) : Option (List Source) :=
  if found = .here ∧ there = false then
    if now = .here then none else some (readSources kind now)
  else some (readSources kind found)

/-- **A read reaches a user's original only while the record keeps the file
on this device and its bytes are there.** Once the upload has switched the
record to storage, every read uses the cache or storage, and an open that
raced the switch goes where the record now points. -/
theorem original_only_here (kind : Kind) (found now : Place) (there : Bool)
    (srcs : List Source) (opened : openRead kind found now there = some srcs)
    (reads : Source.original ∈ srcs) :
    kind = .user ∧ found = .here ∧ there = true := by
  cases kind <;> cases found <;> cases now <;> cases there <;>
    simp [openRead, readSources] at opened <;> subst opened <;> simp_all

/-- A stored file never reads an original. -/
theorem stored_reads_no_original (kind : Kind) (now : Place) (there : Bool) :
    openRead kind .stored now there = some [.cache, .storage] := by
  cases kind <;> simp [openRead, readSources]

end CovenIO.Keep
