//! The new person's name a join's request carried, which the store the join
//! published keeps until the app acknowledges it (§13.2).

macro_rules! coven_joined_invitee_tables {
    ($visit:ident) => {
        $visit!(
            _coven_joined_invitee,
            "
            CREATE TABLE _coven_joined_invitee (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                name TEXT NOT NULL
            ) STRICT;
        "
        );
    };
}
