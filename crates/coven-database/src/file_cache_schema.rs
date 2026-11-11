//! The cache's entries of stored bytes, each namespace's budget and the cached
//! bytes it holds, and the cache ranges being written with no writer held
//! (§17.8, Appendix C.3).

macro_rules! coven_cache_tables {
    ($visit:ident) => {
        // Recorded bytes are removed once the writer is free, with no check
        // of their own: its triggers record a removal only for a path no row
        // names, and let no row name a path recorded for removal (Appendix C.3).
        $visit!(_coven_cache, "
            CREATE TABLE _coven_cache (
                namespace TEXT NOT NULL,
                object_path TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>={CACHE_COMPLETE_FILE}),
                chunks INTEGER NOT NULL DEFAULT 1 CHECK(chunks>=1) CHECK(chunk>=0 OR chunks=1),
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size>=0),
                last_read INTEGER NOT NULL,
                pinned INTEGER NOT NULL CHECK(pinned IN (0,1)) CHECK(pinned=0 OR chunk={CACHE_COMPLETE_FILE}),
                checked_hash BLOB CHECK((chunk={CACHE_COMPLETE_FILE})=(checked_hash IS NOT NULL)) CHECK(checked_hash IS NULL OR length(checked_hash)=32),
                PRIMARY KEY(namespace,object_path,chunk)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cache_lru ON _coven_cache(namespace,pinned,last_read);
            CREATE INDEX _coven_cache_recency ON _coven_cache(last_read);
            CREATE TRIGGER _coven_cache_unremoved BEFORE INSERT ON _coven_cache BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.path);
            END;
        ");
        // Each declared namespace's budget and the bytes the cache records
        // hold, kept with them so a budget check needs no sum: `cached` is
        // the unpinned entries' bytes, which the budget counts, and `pinned`
        // the pinned entries', which it doesn't. Cache records live only in
        // declared namespaces (FileRef validation).
        $visit!(_coven_cache_budgets, "
            CREATE TABLE _coven_cache_budgets (
                namespace TEXT PRIMARY KEY NOT NULL,
                bytes BLOB NOT NULL CHECK(length(bytes)=8),
                cached INTEGER NOT NULL DEFAULT 0 CHECK(cached>=0),
                pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned>=0)
            ) STRICT, WITHOUT ROWID;
            CREATE TRIGGER _coven_cache_added AFTER INSERT ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET
                    cached=cached+CASE WHEN NEW.pinned=0 THEN NEW.size ELSE 0 END,
                    pinned=pinned+CASE WHEN NEW.pinned=1 THEN NEW.size ELSE 0 END
                WHERE namespace=NEW.namespace;
            END;
            CREATE TRIGGER _coven_cache_removed AFTER DELETE ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET
                    cached=cached-CASE WHEN OLD.pinned=0 THEN OLD.size ELSE 0 END,
                    pinned=pinned-CASE WHEN OLD.pinned=1 THEN OLD.size ELSE 0 END
                WHERE namespace=OLD.namespace;
            END;
            CREATE TRIGGER _coven_cache_moved AFTER UPDATE OF pinned, size ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET
                    cached=cached
                        -CASE WHEN OLD.pinned=0 THEN OLD.size ELSE 0 END
                        +CASE WHEN NEW.pinned=0 THEN NEW.size ELSE 0 END,
                    pinned=pinned
                        -CASE WHEN OLD.pinned=1 THEN OLD.size ELSE 0 END
                        +CASE WHEN NEW.pinned=1 THEN NEW.size ELSE 0 END
                WHERE namespace=NEW.namespace;
            END;
        ");
        // A pin's download that stopped, kept apart from the cache: the first
        // `size` bytes of a file's encrypted copy, synced, so the next prepare
        // of the file continues from them. The cache never counts or evicts
        // them; the app's unpin, the pin they became, or the stored bytes
        // leaving ends them (§17.8, Appendix C.3).
        $visit!(_coven_cache_partials, "
            CREATE TABLE _coven_cache_partials (
                namespace TEXT NOT NULL,
                object_path TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size>=0),
                PRIMARY KEY(namespace,object_path)
            ) STRICT, WITHOUT ROWID;
            CREATE TRIGGER _coven_cache_partials_unremoved BEFORE INSERT ON _coven_cache_partials BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.path);
            END;
        ");
        // Cache ranges whose bytes are being written with no writer held;
        // opening makes the abandoned ones removals (Appendix C.3).
        $visit!(_coven_cache_writes, "
            CREATE TABLE _coven_cache_writes (path TEXT NOT NULL PRIMARY KEY) STRICT, WITHOUT ROWID;
        ");
    };
}
