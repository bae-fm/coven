//! Process-global OAuth client-credential registration contract. One ordered
//! test because the credentials are stored in a process-wide `OnceLock`:
//! parallel tests would race the registry.

use std::collections::HashMap;

use coven::OAuthClientCreds;

#[test]
fn oauth_client_creds_registration_contract() {
    let creds = HashMap::from([(
        "google_drive".to_string(),
        OAuthClientCreds {
            client_id: "id".to_string(),
            client_secret: None,
        },
    )]);

    coven::set_oauth_client_creds(creds.clone()).expect("first registration");

    // Re-registration: the same map is a no-op, a differing map is a startup
    // contradiction.
    coven::set_oauth_client_creds(creds).expect("same-value re-registration is a no-op");

    let other = HashMap::from([(
        "google_drive".to_string(),
        OAuthClientCreds {
            client_id: "different".to_string(),
            client_secret: None,
        },
    )]);
    let err = coven::set_oauth_client_creds(other)
        .expect_err("differing re-registration is a startup contradiction");
    assert!(
        err.to_string().contains("already registered"),
        "error names the conflict: {err}"
    );
}
