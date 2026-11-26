//! The SQL text that the schema's views and the code that reads them share, as macros that expand
//! to string literals so a statement can `concat!` them: the sync state's facts, the counts of
//! unsent work, and the kind of an invite.

/// The facts, as SQL expressions over `coven_sync_status AS s` and
/// `coven_retry_effect AS r`, in the order `SyncFacts::decode` reads them:
/// the status row's columns; whether sync runs and its cooldown; the send
/// queue's writes and the upload queue's uploads, counted; whether a bound
/// upload has bytes to send; the bound uploads the state can show, as a JSON
/// array; the pending records the state reads, as a JSON array; the sync
/// scope's backoff; and each operation's backoff, as a JSON array.
///
/// `facts!` reads each kind of bound upload the state can show from the
/// table where that kind has the fewest rows: the uploads with a record from
/// the pending records, the first one sending from the files in flight, the
/// first queued one with no record and no retry by walking the bound queue
/// in id order to the first match, and the first with no record that waits
/// out a retry from the retry rows.
/// The outer read then reads only those rows, and only bound ones. Each is
/// `[id, stored, sending, retry seconds, retry nanos, record position]`, the
/// retry time as `_coven_unix_seconds` and `_coven_unix_nanos` give it, since
/// JSON holds no bytes. A record is `[position, record, cause]`, in the order
/// the records arose, and an operation's backoff `[operation, seconds,
/// nanos]`.
#[macro_export]
macro_rules! facts {
    () => {
        concat!(
            "s.storage,s.provider,s.last_pass,s.moved,s.last_synced,r.syncing,r.cooling_until,",
            $crate::unsent_counts!(),
            ",EXISTS(SELECT 1 FROM _coven_file_uploads AS b
                 WHERE b.holds IS NOT NULL AND NOT b.stored
                   AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=b.id)),
             (SELECT json_group_array(json_array(u.id,u.stored,
                     u.id IN (SELECT c.position FROM coven_sending AS c),
                     iif(t.upload IS NULL,NULL,_coven_unix_seconds(t.not_before)),
                     iif(t.upload IS NULL,NULL,_coven_unix_nanos(t.not_before)),e.id)
                     ORDER BY u.id)
                 FROM _coven_file_uploads AS u
                 LEFT JOIN _coven_retries AS t ON t.upload=u.id
                 LEFT JOIN _coven_pending AS e
                     ON e.work='upload' AND e.device=u.device AND e.object=u.object
                 WHERE u.id IN (
                     SELECT f.id FROM _coven_pending AS p
                     JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object
                     WHERE p.work='upload'
                     UNION
                     SELECT MIN(g.position) FROM coven_sending AS g
                     JOIN _coven_file_uploads AS b ON b.id=g.position
                     WHERE b.holds IS NOT NULL
                     UNION
                     SELECT * FROM (
                         SELECT f.id FROM _coven_file_uploads AS f
                         WHERE NOT f.stored AND f.holds IS NOT NULL
                           AND f.id NOT IN (SELECT position FROM coven_sending)
                           AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id)
                           AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p
                                          WHERE p.work='upload' AND p.device=f.device
                                            AND p.object=f.object)
                           AND NOT EXISTS(SELECT 1 FROM _coven_retries AS t WHERE t.upload=f.id)
                         ORDER BY f.id LIMIT 1)
                     UNION
                     SELECT MIN(f.id) FROM _coven_retries AS t
                     CROSS JOIN _coven_file_uploads AS f ON f.id=t.upload
                     WHERE t.upload IS NOT NULL
                       AND NOT f.stored AND f.holds IS NOT NULL
                       AND f.id NOT IN (SELECT position FROM coven_sending)
                       AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id)
                       AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p
                                      WHERE p.work='upload' AND p.device=f.device
                                        AND p.object=f.object))
                   AND u.holds IS NOT NULL
                   AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)),
             (SELECT json_group_array(json_array(p.id,json(CAST(p.value AS TEXT)),p.cause)
                     ORDER BY p.id)
                 FROM _coven_pending AS p
                 WHERE (p.pin IS NOT NULL
                     OR p.work='download'
                     OR (p.work='upload' AND NOT EXISTS(
                         SELECT 1 FROM _coven_file_uploads AS f
                         WHERE f.device=p.device AND f.object=p.object AND f.holds IS NOT NULL))
                     OR (p.move IS NOT NULL AND NOT EXISTS(
                         SELECT 1 FROM _coven_file_uploads AS f
                         WHERE f.move=p.move AND f.holds IS NOT NULL))) IS NOT TRUE),
             (SELECT t.not_before FROM _coven_retries AS t WHERE t.scope='sync'),
             (SELECT json_group_array(json_array(t.operation,_coven_unix_seconds(t.not_before),
                     _coven_unix_nanos(t.not_before)))
                 FROM _coven_retries AS t WHERE t.operation>0)"
        )
    };
}

/// The writes waiting in the send queue and the uploads waiting in the
/// upload queue, counted, as two SQL expressions. Each counts a column, not
/// rows, so that SQLite names the `coven_sync_state` view to the authorizer
/// for the read when the view counts them (`sync_facts.rs`).
#[macro_export]
macro_rules! unsent_counts {
    () => {
        "(SELECT count(w.number) FROM _coven_uploads AS w),
         (SELECT count(q.id) FROM _coven_file_uploads AS q)"
    };
}

/// An invite's kind, as schema SQL spells it.
#[macro_export]
macro_rules! invite_kind {
    () => {
        "invite"
    };
}
