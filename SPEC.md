# Coven specification outline

1. **Purpose and boundaries:** Define Coven as an embedded data and synchronization
   library, covering the application’s responsibilities, Coven’s ownership of
   database connections, and operation with or without connected storage.
2. **Local data:** Specify schema declarations, row identity, transactions, reads,
   live queries, schema evolution, and the rules determining which data remains
   local or becomes shared.
3. **Synchronization:** Describe change capture, publication, remote acceptance,
   concurrent edits, durable write status, blocked operations, retries, and the
   guarantees applications can rely on.
4. **Files and storage:** Define how files belong to rows, move between local and
   remote storage, stream to callers, and interact with upload queues, cache
   budgets, and offline pinning.
5. **Identity and sharing:** Specify encryption, key custody, device pairing,
   membership, ownership, Circles, revocation, recovery, and the security and
   privacy limits of each.
