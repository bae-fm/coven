# Codebase-wide callable ownership

## Goal

Reveal the ownership model of every executable Rust path in Coven. Inventory
every callable, resolve its callers and callees, identify the state and services
it uses, and place each stateful workflow on the type whose lifetime and
invariants own that work.

The working method is a bottom-up ownership queue. Build a graph of retained
owners and unowned stateful call groups, then select groups that do not call any
other unowned stateful group. For each ready group, choose its exact disposition:
bind it to its invariant-bearing owner, internalize it into its caller, leave a
verified transformation in place, or delete duplication. Repair callers through
compiler errors, record the decision, commit that group, and rebuild the graph.
Each disposition exposes the next ready groups. Continue upward until every
stateful workflow is owned and every remaining free function is a verified
transformation, boundary, dependency primitive, or callback adapter.

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

The generated graph lives under `target/ownership-audit/`. Decisions that must
survive graph rebuilds live in the Git-ignored local file
`tools/ownership-audit/decisions.toml`, keyed by qualified symbol name and
normalized signature.

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
ownership-audit graph
ownership-audit check
```

`show` renders both directions around a symbol and labels each edge with the
source argument expressions passed through that call. It records provenance
when a parameter, local alias, or receiver field can be resolved and marks the
rest unresolved. Recursive call groups are collapsed into one node so recursion
does not produce a false ordering.

`graph` writes a generated review artifact under `target/ownership-audit/`.
Its normal modes are complete ordered lists: ready workflow leaves, every
unowned workflow, retained owners, and construction boundaries. Selecting a
row shows its callables, dependencies, effects, callers, and owner evidence.
The graph is reserved for the selected row's dependency neighborhood.
Construction boundaries have their own list showing each constructor or
opener, its lexical body, its direct callers, its visibility, and its calls
into other construction boundaries. Callers are the complete repository caller
set; public visibility separately exposes calls that downstream hosts may make.
Construction boundaries do not participate in the workflow dependency order.

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

An adjacent existing type is evidence, not a required destination. When no
existing type owns the workflow's complete lifetime and invariant, create the
missing owner and attach it to the object graph. Do not leave the workflow free
or force it onto a neighboring type to avoid introducing that owner.

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

A ready group authorizes changing that group only. When its correct disposition
is to internalize it into a caller, move its body into the caller and delete the
selected entry point, but do not relocate the caller unless the rebuilt graph
also marks that caller ready. The caller can still depend on other unowned
groups. Crossing that boundary forces either a reach-through or duplicate
logic.

For example, given:

```text
mark_make_remote_cancelling_on  ready
gated_root_gate_col             ready
cancel_make_remote              blocked by both
```

internalize `mark_make_remote_cancelling_on` into `cancel_make_remote` and
commit that disposition while leaving `cancel_make_remote` in its existing
module. After `gated_root_gate_col` receives its own disposition and the graph
is rebuilt, `cancel_make_remote` becomes eligible for relocation.

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

### Constructor boundaries

A receiverless associated callable that produces its receiver type is a
constructor, not an unowned workflow. A free function that has no direct
or ambient effects, has no unresolved calls, and only forwards construction
into such a callable is the same boundary, including chains of those forwarding
functions. A receiverless function that creates and returns a retained
capability from inputs that are not retained capabilities is also a
construction boundary; this covers constructors supplied by external crates.
A function that derives a value from an existing database, storage, identity,
or authority remains a workflow rather than being classified from its return
type. Lexically nested closures, async blocks, and local functions are part of
their construction boundary. Constructors remain in the complete callable
ledger and receive a separate boundary disposition, including review of which
parent boundary may call them. They do not enter the workflow relocation queue.
Separate stateful helpers called by a constructor remain in the graph and
receive their own dispositions.

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
- Cargo-target and module traversal distinguishes executable sources from
  orphan files. The traversal exposed and removed the undeclared
  `sync/publish_blobs.rs` implementation; the audit rejects future orphan Rust
  sources.
- Semantic call sites inside closures are attributed to the closure rather than
  its enclosing function. The graph collapses recursive call groups and assigns
  every group a callee-first rank for ownership review.
- Callable parameters, return types, receiver types, call arguments, and closure
  bindings are recorded. Callback invocations trace their implementations
  through borrowed and forwarded parameters; trait-dispatched callbacks and
  named external function pointers remain explicit candidate sources.
- Default-feature libraries, all-feature libraries, and all-feature test and
  integration targets are indexed as separate semantic views, with each call
  site retaining the views that resolve it. The audit pins rust-analyzer 1.97.1
  independently after the compiler-pinned Rust 1.95 analyzer logged internal
  type-analysis failures; analyzer error logs now fail the index build. The
  three-view run resolved 14,856 callables into 72,473 edges and 102,038 call
  sites without analyzer errors.
- Async blocks are indexed as callables, and closures and async blocks record
  outer parameters and locals captured from their lexical scopes. Cargo target
  traversal now carries module-level configuration conditions into definitions
  and call sites.
- Item macro expansions are read from rust-analyzer and attributed to their
  source invocations. Direct calls into generated methods resolve to the
  generated callable, while trait calls retain every generated implementation
  permitted by the receiver. Validation caught and removed a false syntactic
  edge from a standard thread-local expansion to an unrelated Coven method.
- Cross-layer reports inventory deep ancestor paths, ancestor and sibling
  imports, public re-exports, receiverless stateful associated functions,
  duplicate receiver dependencies, constructor calls, and raw receiver-field
  bundles. The generated review graph contains 15,060 callables across 397
  modules, 3,799 directed module edges, and 11,699 reach-through candidates.
- The module hierarchy rendering proved too dense and did not encode the
  relocation order. The review artifact now propagates stateful behavior
  through the collapsed call graph, combines receiver-bound components into
  retained-owner nodes, and leaves receiverless stateful components as
  individual ownership decisions.
- The default graph view is the bottom-up work queue: an unowned component is
  ready only when none of its stateful callees are also unowned. It shows
  candidate owners and required capabilities without treating either as a
  decision. Rebuilding the graph after a verified classification or relocation
  removes that blocker and exposes its callers.
- Receiver constructors, effect-free forwarding factories, and their lexical
  bodies now remain in a construction-boundary queue rather than the workflow
  queue. The same queue includes functions that create retained capabilities
  through external constructors and shows every direct caller. Elsewhere,
  lexically nested closures and async blocks contribute explicit
  parent-to-child edges, so a parent cannot appear ready while its nested
  stateful work remains unowned. The current complete index produces 1,266
  production unowned stateful call groups, of which 488 are ready, alongside
  250 production retained owners and 490 production construction boundaries.
- The generated artifact presents those four review sets as full lists. A
  selected row can switch to its dependency graph, and an unowned workflow with
  no suitable adjacent type explicitly permits creating its missing owner.
- Readiness now authorizes disposition of the selected group only. Internalizing
  a ready group into a blocked caller leaves that caller in place; its other
  blockers receive separate commits before the caller can relocate.
- `d1568a60` records that ready-item scope in the plan and generated queue.
- `e5e0fa7d` deletes `mark_make_remote_cancelling_on` and internalizes its SQL
  into its only caller without relocating that blocked caller.
- `587db0fe` deletes `notify_write_status_in`; `Database::notify_write_status`
  now accesses its retained sender map directly.
- The circle-roster and Store-membership conflict and state hashes are verified
  transformations over explicit inputs; they remain private free functions.
