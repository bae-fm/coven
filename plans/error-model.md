# Error model: typed causes end to end

Status: implemented and verified.

## Contract

1. An error value is never converted to display text and stored in another
   error's payload. Crossing a boundary uses a typed variant holding the source.
   `Box<dyn Error + Send + Sync>` is reserved for genuine dependency-inversion
   boundaries such as injected blob-staging and cloud-provider implementations.
2. Wrapping variants include their source in `#[error(...)]`, preserving useful
   display output and a matchable source chain.
3. String-carrying variants represent invariant or provider text that has no
   source error. They are built from literals and domain data, not an error's
   `Display` implementation.
4. Error types have no `From<String>` implementation and APIs have no bare
   `Result<_, String>` or `Error = String` boundary.
5. Each failure is represented by the layer where it originates. Callers add
   typed context instead of laundering it through an unrelated error domain.

## Implemented transition

- Retyped error boundaries across foundation, keys, protocol, database,
  storage, replication, domain, and the public facade. Callers and tests match
  the retained cause rather than reconstructed display strings.
- Replaced database message formatting with typed `DbError` sources while
  retaining `DbError::Message` for database invariants with no source error.
- Split previously conflated storage, pull, initialization, Circle operation,
  membership, blob, OAuth, and host-write failures into their originating
  error types.
- Replaced bare string errors in the owner-construction checker with typed read,
  parse, path, directory, and module-dependency failures.
- Boxed the public and sync error variants whose source types exceeded the
  result-size threshold, with tests fixing the intended maximum representation.
- Enabled Argon's standard error source and retained it through key derivation
  failures.

## Verification

- Detection searches find no `type Error = String`, `impl From<String>`, bare
  string error result, or direct `map_err` conversion of an error to display
  text in `crates/` or `tools/`.
- `scripts/check.sh` passes restricted-path and ownership gates, formatting,
  all-target/all-feature Clippy with warnings denied, documentation links,
  shipped feature combinations, all-feature tests, default-feature tests, and
  documentation tests.
- The all-feature replication suite passes all 657 tests.
