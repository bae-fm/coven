//! A workload of inserts that fire triggers, for measurements of what one
//! triggered write costs.

/// Seven tables, a view over six of them, and three triggers on `scans`.
/// The view `placed` joins each scan with its row in `edits`, `verdicts`,
/// `origins` and `skipped`, and reads `matches` in two subqueries, to give
/// the scan's tab, state, title and top folder. After an insert or an update
/// of a scan, a trigger writes the scan's row of `placed` into `listing`;
/// after a delete, a trigger deletes the scan's row from `listing`. So each
/// insert into `scans` reads the view once and writes `listing` once.
pub const SCHEMA: &str = "CREATE TABLE scans(id TEXT NOT NULL PRIMARY KEY, path TEXT NOT NULL, title TEXT NOT NULL);
CREATE TABLE edits(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE verdicts(id TEXT NOT NULL PRIMARY KEY, kind TEXT, position INTEGER);
CREATE TABLE matches(id TEXT NOT NULL, position INTEGER NOT NULL, pressing TEXT, failure TEXT, PRIMARY KEY(id, position));
CREATE TABLE origins(id TEXT NOT NULL PRIMARY KEY, created_at INTEGER);
CREATE TABLE skipped(path TEXT NOT NULL PRIMARY KEY);
CREATE TABLE listing(id TEXT NOT NULL PRIMARY KEY, tab TEXT NOT NULL, state TEXT, title TEXT NOT NULL, unit_path TEXT NOT NULL);
CREATE VIEW placed AS
  SELECT s.id, CASE WHEN o.id IS NOT NULL THEN 'done' WHEN k.path IS NOT NULL THEN 'skipped' ELSE 'pending' END AS tab,
         CASE WHEN v.kind IS NULL THEN 'not_looked_up'
              WHEN (SELECT count(DISTINCT pressing) FROM matches m WHERE m.id=s.id) <> 1 THEN 'several'
              WHEN EXISTS(SELECT 1 FROM matches m WHERE m.id=s.id AND m.failure IS NOT NULL) THEN 'unread'
              WHEN v.kind = 'found' THEN 'identified' ELSE 'needs_you' END AS state,
         coalesce(e.title, s.title) AS title,
         CASE WHEN instr(s.path,'/')>0 THEN substr(s.path,1,instr(s.path,'/')-1) ELSE s.path END AS unit_path
  FROM scans s LEFT JOIN edits e ON e.id=s.id LEFT JOIN verdicts v ON v.id=s.id
  LEFT JOIN origins o ON o.id=s.id LEFT JOIN skipped k ON k.path=s.path;
CREATE TRIGGER scans_listed AFTER INSERT ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;
CREATE TRIGGER scans_relisted AFTER UPDATE ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;
CREATE TRIGGER scans_unlisted AFTER DELETE ON scans BEGIN DELETE FROM listing WHERE id=OLD.id; END;";
