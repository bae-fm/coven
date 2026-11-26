//! Moves of this device's files to the cloud (§17.3).

macro_rules! coven_file_moves_tables {
    ($visit:ident) => {
        // Each move of files of this device to the cloud (§17.3): the files
        // one write staged for the cloud, or the files the app moved. Every
        // upload belongs to one; a move switches its files together once all
        // its uploads are stored, or fails or is cancelled whole. A move
        // keeps its row once it ends, so it is never mistaken for another.
        $visit!(
            _coven_moves,
            "
            CREATE TABLE _coven_moves (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL CHECK(state IN ('moving','failed','cancelled','finished'))
            ) STRICT;
            CREATE INDEX _coven_moves_moving ON _coven_moves(id) WHERE state='moving';
        "
        );
        // The files each moving or failed move lists (§17.3), so the app can
        // see what a move holds and cancel a failed one: written in the write
        // that records the move, and as a new file over a moving file's bytes
        // joins it. A file leaves as its record goes. A switch or a cancel
        // ends the move's list; a failure keeps it, its files waiting for no
        // upload and in no other move until the app cancels it: a file is in
        // at most one move. The triggers keep `coven_moves` (`internal_schema_moves.rs`)
        // listing each file with its move's state, for the app's triggers to
        // follow (§9.4).
        $visit!(
            _coven_move_files,
            "
            CREATE TABLE _coven_move_files (
                file TEXT PRIMARY KEY NOT NULL,
                move INTEGER NOT NULL REFERENCES _coven_moves(id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_move_files_move ON _coven_move_files(move);
            CREATE TRIGGER _coven_move_files_listed AFTER INSERT ON _coven_move_files BEGIN
                INSERT INTO coven_moves(move,state,file)
                VALUES(NEW.move,(SELECT state FROM _coven_moves WHERE id=NEW.move),NEW.file);
            END;
            CREATE TRIGGER _coven_move_files_ended AFTER DELETE ON _coven_move_files BEGIN
                DELETE FROM coven_moves WHERE file=OLD.file;
            END;
            CREATE TRIGGER _coven_moves_changed AFTER UPDATE OF state ON _coven_moves BEGIN
                UPDATE coven_moves SET state=NEW.state WHERE move=NEW.id;
            END;
        "
        );
    };
}
