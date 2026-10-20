use super::*;

/// A cause below another, as reqwest and hyper nest them.
#[derive(Debug)]
struct Layer(&'static str, Option<Box<Layer>>);
impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for Layer {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.1.as_deref().map(|layer| layer as _)
    }
}

/// Every layer prints, in order, and each URL keeps only its scheme, host
/// and port: a path, a query and credentials never print.
#[test]
fn every_cause_prints_with_its_urls_cut_to_the_host() {
    let error = WithCauses(Layer(
        "error sending request for url (https://ana:hunter2@api.example:8443/2/files/Gifts.txt?token=deadbeef)",
        Some(Box::new(Layer(
            "client error (Connect)",
            Some(Box::new(Layer("dns error: no such host", None))),
        ))),
    ));
    let expected = "error sending request for url (https://api.example:8443/…): \
        client error (Connect): dns error: no such host";
    assert_eq!(error.to_string(), expected);
    let debug = format!("{error:?}");
    assert!(debug.contains(expected), "{debug}");
    for hidden in ["hunter2", "ana", "Gifts", "deadbeef", "token"] {
        assert!(!debug.contains(hidden), "{hidden} in {debug}");
    }
    assert!(error.source().unwrap().is::<Layer>());
}

#[test]
fn text_that_is_not_a_url_is_kept_and_a_malformed_one_is_hidden() {
    assert_eq!(redact_urls("no url here"), "no url here");
    assert_eq!(
        redact_urls("at http://[bad and https://ok.example/x"),
        "at <url> and https://ok.example/…"
    );
}
