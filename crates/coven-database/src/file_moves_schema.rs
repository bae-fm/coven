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
    };
}
