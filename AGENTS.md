# Project Instructions

## Greenfield Development

coven is greenfield software. Implement the intended design directly. Do not
add or retain backward compatibility, compatibility shims or branches, legacy
formats or readers and writers, fallback paths, or migrations for earlier coven
development states unless the user explicitly requests them.

Delete superseded paths and update every caller, test, fixture, and document to
the single current shape. The application-facing schema migration system is a
product capability; it does not authorize preserving obsolete coven internals.

## Ownership and Composition

Objects do not expose or hand their retained dependencies to callers. An object
uses the dependencies it owns to perform its capabilities, and callers use that
object as the capability. Compose the object graph through owner methods; do not
unpack an owner into a database, storage provider, key, runtime, or other
internal so another layer can perform its work.

## Rust Build Feedback

Coven's Cargo dev and test profiles compile incrementally. Do not set
`CARGO_INCREMENTAL=0` when building or testing this repository.

## File and impl layout

- Keep files at or under 1,000 lines, ideally. When a file's test module
  pushes it past that, split the tests into a sibling `*_test.rs` (or
  `tests.rs`) file rather than growing the production file.
- Never split a type's inherent impl across separate blocks in one file:
  all of a type's methods live in one `impl` block. Within an impl,
  `#[cfg(test)]` methods come after the production methods.
- When a type carries many test-only methods and its tests are split out,
  those test-only methods move to an `impl` block in the test file.
