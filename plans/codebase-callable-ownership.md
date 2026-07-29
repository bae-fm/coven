# Codebase-wide callable ownership

## Goal

Reveal the ownership model of every executable Rust path in Coven. Inventory
every callable, resolve its callers and callees, identify the state and services
it uses, and place each stateful workflow on the type whose lifetime and
invariants own that work.

This applies to both Rust crates, not only Store authorization:

- `crates/coven-core/**/*.rs`; and
- `crates/coven/**/*.rs`.

Library code, unit tests, integration tests, feature-gated code, trait defaults,
macros that emit callables, and associated functions without a receiver are all
included. Generated build output and third-party sources are excluded.

The audit does not ban free functions. A free function is the correct form when
it transforms already-selected inputs without loading related state, choosing
authority, consulting an ambient service, or requiring identity to remain
coherent across calls.

## Terms

- **Owner**: the type whose lifetime matches a dependency or stateful resource
  and whose invariants determine how it may be used.
- **Retained dependency**: a database, storage provider, identity, encryption
  service, journal, root, configuration, clock, identifier source, runtime,
  cancellation source, cache, filesystem root, or other stateful handle whose
  identity must remain coherent across multiple calls.
- **Operation capability**: an owned or borrowed value proving that a workflow
  has the exact dependencies and validated state it requires.
- **Already-selected input**: an immutable value whose selection, provenance,
  authorization, and freshness decisions have already occurred. A function
  consuming it does not reload or choose related state.
- **Boundary**: the place that creates or opens a retained dependency and
  constructs its owner.
- **Workflow**: a callable that loads, chooses, interprets, changes, publishes,
  or persists state.
- **Transformation**: a callable whose result depends only on its explicit
  already-selected inputs.

“Closed value” is not used as an ownership term. Plans and code use the
specific phrase that applies: already-selected input, verified value, complete
object graph, or exhaustive enum.

## Questions answered for every callable

Each callable receives one durable disposition:

1. What calls it, directly and transitively?
1. What does it call?
1. Which public API, test, task, callback, or protocol entry point reaches it?
1. Which explicit dependencies does it accept?
1. Which dependencies does it obtain from globals, static APIs, environment,
   process state, thread-local state, or constructors inside its body?
1. Which values flow unchanged through its callers and sibling calls?
1. Which combinations of arguments must describe the same Store, database,
   provider, identity, filesystem, operation, or transaction?
1. Does an existing caller already own those values?
1. Does it establish or refresh state, or consume state already selected by its
   caller?
1. What durable or external effects can it perform?
1. Could two individually valid arguments be combined into an invalid
   operation?
1. Should it be an owner method, an operation-capability method, a trait method,
   a private transformation, a boundary constructor, or deleted duplication?

No item is classified from its signature alone. The body and complete calling
chain determine its disposition.

## Semantic index

Create a repository analysis command under `tools/ownership-audit/`. It uses
Cargo metadata for source roots and rust-analyzer's Rust semantic model for
symbol resolution and call hierarchy. Text search may discover candidates, but
it is never treated as the call graph.

The index covers these callable forms:

- module free functions;
- inherent and trait methods;
- associated functions without `self`;
- trait default implementations;
- closures passed or stored as callbacks;
- function pointers;
- async blocks that retain captured state;
- macro-generated callables, attributed to their source macro invocation;
- test and feature-gated callables under every Cargo target and feature set.

For each Rust symbol, the index records:

```text
symbol identity
crate and module
source file and definition span
cfg and target conditions
visibility
callable kind
receiver type, when present
parameter and return types
direct callers
direct callees
dynamic-dispatch candidates
captured values
ambient dependency reads
stateful parameters
external and durable effects
```

The generated symbol identity comes from semantic resolution, not a line
number. A moved function receives its new qualified identity; the ledger must
record whether the prior identity was deleted or replaced by that new owner
method.

### Configuration coverage

Build separate semantic views for:

- each library with default features;
- each library with all features;
- unit-test configurations;
- every integration-test target;
- each platform-specific source set represented in Cargo metadata.

The merged graph preserves the configuration attached to each edge. An edge
present only in a test or platform build is not discarded.

### Calls the tool cannot resolve exactly

Trait objects, function pointers, callbacks crossing external libraries,
generated code without source mapping, and platform code unavailable on the
host are recorded as unresolved edges with every known candidate. They require
manual disposition. An unresolved edge can never silently count as “no
callers.”

## Durable ledger

The generated graph lives under `target/ownership-audit/`. Durable decisions
live in `tools/ownership-audit/decisions.toml`, keyed by qualified symbol name
and normalized signature.

Each decision contains:

```toml
classification = "owner-method | operation-method | boundary | transformation | dependency-primitive | callback-adapter | delete"
owner = "fully::qualified::Type"
reason = "invariant that determines this placement"
status = "classified | relocated | verified"
replaced_by = "qualified owner method, when relocation deletes this symbol"
```

`owner` is omitted only for transformations, dependency primitives, callback
adapters, and deleted duplication. Those classifications still require a
reason grounded in the function body and callers. `replaced_by` is present only
when relocation deletes the recorded symbol in favor of an owner method.

The command exposes:

```text
ownership-audit inventory
ownership-audit show <symbol>
ownership-audit callers <symbol>
ownership-audit callees <symbol>
ownership-audit retained-dependencies <symbol>
ownership-audit unclassified
ownership-audit check
```

`show` renders both directions around a symbol and labels each edge with the
source argument expressions passed through that call. It records provenance
when a parameter, local alias, or receiver field can be resolved and marks the
rest unresolved. Recursive call groups are collapsed into one node so recursion
does not produce a false ordering.

## Dependency and effect classification

The analysis detects explicit and ambient access to:

- application and protocol databases, SQLite connections, transactions, and
  write guards;
- cloud, exact-slot, protocol-object, blob, and filesystem storage;
- Store roots, verified histories, membership, registrations, device signers,
  grants, and operation journals;
- user identities, device identities, key custody, encryption, and keyrings;
- provider bindings, HTTP clients, OAuth state, and remote API clients;
- runtime handles, tasks, channels, locks, progress sinks, cancellation, and
  retry state;
- clocks, randomness, identifier providers, environment variables, and process
  configuration;
- caches, retained materializations, staging directories, and temporary-file
  owners;
- log and tracing context when it carries operation identity.

Effects are recorded independently from dependencies:

- database reads and writes;
- storage and filesystem reads, writes, moves, and deletes;
- network requests;
- cryptographic signing, decryption, and key derivation;
- task creation, cancellation, channel publication, and blocking work;
- clock, randomness, environment, and process access;
- mutation through references, locks, transactions, and interior mutability.

This separates a stateful workflow from a transformation that happens to
process a database-shaped value.

## Finding the owner

For each stateful callable, inspect its complete caller tree and argument
provenance.

The owner is already present when:

- every caller obtains the dependency from the same receiver field;
- the same group of arguments is repeatedly forwarded together;
- the function accepts values that must belong to the same root, database,
  provider, identity, transaction, or operation;
- callers reload a value that their receiver already retains;
- tests must reconstruct production state to call the function;
- changing one argument independently would violate an invariant; or
- the function returns a value that exists only while the caller's state,
  lock, transaction, or verified session remains valid.

Compose existing owners and domain types before introducing another type. Add
an operation capability only when the calling graph proves a distinct lifetime
or invariant that no existing owner represents.

If sibling modules need the same capability, move it to their direct common
owner. A grandchild does not construct a grandparent's internal authority.
Restricted-path visibility and forwarding facades are not substitutes for
placing the operation at its owner.

## Valid free functions

A callable may remain free when all of the following hold:

- it consumes already-selected inputs;
- it does not load, discover, refresh, or choose related state;
- it does not consult an ambient dependency;
- no argument combination can mix unrelated owner instances;
- its callers do not repeatedly forward a retained dependency bundle;
- its lifetime does not depend on a hidden lock, transaction, session, cache,
  or journal;
- its module is the implementation home of the transformation; and
- making it a method would only change spelling, not enforce an invariant.

Examples include canonical encoding, hashing bytes, parsing against an explicit
expected reference, deterministic comparison, and exhaustive transformation of
an enum.

A low-level storage or database primitive may remain separate only when its
passed handle is the complete subject of that primitive and no cross-call
coherence is hidden. Otherwise it belongs on the storage or database owner.

## Bottom-up relocation

Collapse recursive call groups, then order the graph from callees toward entry
points. Work one ownership boundary at a time:

1. Read every function in the selected call group completely.
1. Classify its dependencies, effects, callers, and callees.
1. Resolve any unclassified callee first.
1. Identify the existing owner available at all legitimate call sites.
1. Move the body onto that owner or a capability derived from it.
1. Remove parameters already retained by the receiver.
1. Move nested stateful callees encountered during the relocation before
   returning to the suspended caller.
1. Delete the original free or associated function and every re-export.
1. Tighten constructors, fields, loaders, and helpers so the old assembly path
   fails to compile.
1. Repair callers from the compiler errors; do not preserve a forwarding
   wrapper.
1. Rebuild the semantic index because ownership and call edges have changed.
1. Mark the ledger item verified only after searches, focused tests, strict
   Clippy, and the relevant failure-path tests pass.

Moving the deepest callable first does not mean turning transformations into
methods. A leaf transformation is classified and left in place; the nearest
stateful caller is the relocation target.

## Cross-layer reach-throughs

The index separately reports:

- `super::super::` and deeper ancestor paths;
- absolute paths from a child into an ancestor's implementation module;
- sibling implementation imports;
- re-exports of implementation functions;
- associated calls used as namespace-qualified free functions;
- methods that accept fields already retained by `self`;
- constructors invoked below their owning boundary; and
- pairs of raw fields extracted from a capability and passed onward.

Each report is reviewed against the call graph. Domain-type imports may be
replaced with explicit module imports. Workflow calls and capability
construction move behind the immediate owner. Replacing a relative path with
an absolute path does not resolve a reach-through.

## Ambient dependency sweep

The free-function inventory is combined with searches for:

```text
Utc::now / SystemTime::now / Instant::now
rand / UUID generation
std::env
process-global configuration
runtime acquisition and task spawning
filesystem access without a retained root
global or thread-local mutable state
singleton clients and caches
```

Read each complete caller chain. Initialize the dependency at the operation
boundary and retain it on the owner when its identity spans the operation.
Reuse an existing clock, identifier provider, runtime, directory, client, or
configuration type. Do not invent an injected service without an actual
consumer.

## Tests

Tests participate in the same ownership graph.

- Integration fixtures construct production owners and call domain operations.
- Tests do not assemble databases, storage, roots, identities, verifiers, or
  journals into a private capability.
- A test-only method may expose a domain result, never raw owner fields or an
  alternate constructor.
- Tests for private transformations live beside their implementation.
- Analyzer fixtures prove that free transformations pass, retained dependency
  bundles fail, hidden ambient dependencies fail, receiver-owned duplicate
  parameters fail, and unresolved dynamic calls remain visible.

The audit cannot be satisfied by changing production code while retaining an
alternate test architecture.

## Relationship to Store authorization work

`plans/capability-based-store-authorization.md` is one ownership region in this
Rust call graph. Its retained root, history, membership, identity,
database, storage, writer, join, restore, snapshot, and Circle capabilities
become ledger owners rather than a separate audit vocabulary.

Existing Store relocations remain valid when the complete caller graph supports
their owner. Any Store function found outside that model is added to the same
bottom-up stack. The Rust ownership audit does not recreate removed Serial
engine, compatibility, migration, facade, or legacy paths.

## Commit and landing discipline

Commit one ownership boundary after:

- every production and test caller uses the owner;
- the prior entry point and re-export are deleted;
- constructor and field visibility enforce the new path;
- the semantic index contains no unresolved bypass for that boundary;
- focused tests and strict Clippy pass; and
- the durable ledger records the invariant behind the placement.

Rebase the branch onto `origin/main`, fast-forward `main`, and push after each
commit. The workspace gate runs periodically and before each push; focused
checks run at every boundary.

## Acceptance criteria

The work is verified when:

- every callable in every supported target and feature configuration has a
  durable disposition;
- every unresolved dynamic edge has a reviewed candidate set and disposition;
- no free or receiverless associated function establishes state that an owner
  could retain;
- no method accepts a dependency or state value already retained by its
  receiver;
- no workflow accepts a loose argument group whose members must describe one
  owner instance;
- no lower layer reloads, reconstructs, or independently selects state already
  retained above it;
- no grandchild constructs a grandparent capability or reaches through its
  parent to invoke an internal workflow;
- no forwarding facade or compatibility path preserves a relocated entry
  point;
- ambient dependencies are read at their owning boundary or represented by an
  existing injected primitive;
- test code uses the production object graph;
- compiler privacy makes each removed assembly path unavailable;
- rebuilding the semantic graph introduces no unclassified callable;
- `rg` searches find no stale names, imports, re-exports, comments, fixtures,
  or documentation for relocated paths; and
- formatting, strict Clippy, every feature/target build, focused tests, and the
  repository test gate pass.

## Progress journal

- `b67c06d6` defined the codebase-wide callable ownership audit and its
  acceptance evidence.
- `2652f103` restricted verified history construction to founder, join, and
  snapshot boundaries.
- The callable-index command inventories named functions, methods, associated
  functions, trait callables, closures, configuration variants, retained
  dependencies, ambient access, effects, and semantic call edges. It waits for
  rust-analyzer to load the complete Cargo workspace and rejects degraded
  analyzer health instead of writing a partial graph.
