import CovenStorage.Objects

namespace CovenIO

abbrev Bytes := CovenStorage.Bytes
abbrev Path := CovenStorage.Path

/-- Exact paths stand for provider object ids. Metadata never carries the
body; equal size and time still require comparing the revision. -/
structure Metadata where
  size : Nat
  storedAt : Nat
  revision : Nat
  deriving DecidableEq, Repr

inductive Failure where
  | network | storage | disk | database | protocol
  deriving DecidableEq, Repr

end CovenIO
