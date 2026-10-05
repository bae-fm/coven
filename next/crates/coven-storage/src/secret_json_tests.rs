use super::*;
use std::cell::Cell;

struct ChangingText {
    calls: Cell<usize>,
    first: &'static str,
    second: &'static str,
}
impl Serialize for ChangingText {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let calls = self.calls.get();
        self.calls.set(calls + 1);
        serializer.serialize_str(if calls == 0 { self.first } else { self.second })
    }
}

#[test]
fn a_changed_second_pass_fails_instead_of_growing_the_secret_buffer() {
    for (first, second) in [("a", "longer"), ("longer", "a")] {
        let value = ChangingText {
            calls: Cell::new(0),
            first,
            second,
        };
        assert!(encode(&value).is_err());
        assert_eq!(value.calls.get(), 2);
    }
}
