// The handle rule of the policy of this folder: each line that holds a
// finding of `clippy::handle_not_from_its_owner` holds a `finding` comment.

use std::sync::Arc;

use crate::Clock;

// Only a handle's owner, the handle and what holds the owner hand it out;
// what holds the handle itself, or holds no owner of it, hands out none.
pub struct State {
    clock: Clock,
}

#[derive(Clone)]
pub struct Requests {
    state: Arc<State>,
}

pub struct Query {
    state: Arc<State>,
}

impl Query {
    pub fn requests(&self) -> Requests {
        Requests { state: self.state.clone() }
    }
}

pub struct Holding {
    query: Query,
}

impl Holding {
    pub fn requests(&self) -> Requests {
        self.query.requests()
    }
}

pub struct Outermost {
    holding: Arc<Holding>,
}

impl Outermost {
    pub fn requests(&self) -> Requests {
        self.holding.requests()
    }
}

pub struct Holder {
    requests: Requests,
}

impl Holder {
    pub fn requests(/* finding */ &self) -> Requests {
        self.requests.clone()
    }
}

pub struct Keeper {
    query: Query,
    kept: Holder,
}

impl Keeper {
    pub fn requests(/* finding */ &self) -> Requests {
        self.query.requests()
    }
}

pub struct Stranger {
    clock: Clock,
}

impl Stranger {
    pub fn share(/* finding */ &self) -> Requests {
        Requests { state: Arc::new(State { clock: Clock::new() }) }
    }
}
