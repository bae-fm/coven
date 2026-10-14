use super::*;
use crate::providers::tests::{json, response, TestServer};
use axum::{body::Bytes, extract::State, http::Uri, Router};
use coven_foundation::clock::SystemClock;
use std::sync::Mutex;

#[test]
fn cli_commands_name_only_the_chosen_profile_or_account() {
    let args = |command: &tokio::process::Command| {
        command
            .as_std()
            .get_args()
            .map(|arg| arg.to_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let aws = aws_cli(Some("storage-admin"));
    assert_eq!(aws.as_std().get_program(), "aws");
    assert_eq!(
        args(&aws),
        [
            "configure",
            "export-credentials",
            "--format",
            "process",
            "--profile",
            "storage-admin"
        ]
    );
    assert_eq!(
        args(&aws_cli(None)),
        ["configure", "export-credentials", "--format", "process"]
    );
    let gcloud = gcloud_cli(Some("ana@example.test"));
    assert_eq!(gcloud.as_std().get_program(), "gcloud");
    assert_eq!(
        args(&gcloud),
        ["auth", "print-access-token", "ana@example.test"]
    );
}

#[test]
fn cli_output_becomes_an_in_memory_credential() {
    let AdminCredential::AccessKey {
        access_key_id,
        secret_access_key,
        session_token,
    } = process_credentials(
        br#"{"Version":1,"AccessKeyId":"ASIA","SecretAccessKey":"s3cr3t","SessionToken":"t0k3n","Expiration":"2026-10-14T01:00:00Z"}"#,
    )
    .unwrap()
    else {
        panic!("access key")
    };
    assert_eq!(access_key_id, "ASIA");
    assert_eq!(secret_access_key.as_str(), "s3cr3t");
    assert_eq!(session_token.unwrap().as_str(), "t0k3n");
    assert!(
        process_credentials(br#"{"Version":2,"AccessKeyId":"A","SecretAccessKey":"s"}"#).is_err()
    );
    let AdminCredential::BearerToken(token) = bearer(b"ya29.token\n").unwrap() else {
        panic!("bearer token")
    };
    assert_eq!(token.as_str(), "ya29.token");
    assert!(bearer(b"  \n").is_err());
    let rendered = format!(
        "{:?}",
        AdminAccessSource::Pasted(AdminCredential::BearerToken(SecretText::new(
            "secret".into()
        )))
    );
    assert!(!rendered.contains("secret"));
}

#[tokio::test]
async fn a_failing_cli_reports_its_status_but_not_its_output() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("fake-cli");
    std::fs::write(
        &script,
        "#!/bin/sh\necho leaked-secret\necho leaked-error >&2\nexit 3\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let error = run(tokio::process::Command::new(&script))
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Authentication);
    let text = format!("{error} {error:?}");
    assert!(!text.contains("leaked"));
    let ok = directory.path().join("ok-cli");
    std::fs::write(&ok, "#!/bin/sh\necho ya29.token\n").unwrap();
    std::fs::set_permissions(&ok, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    assert!(matches!(
        bearer(&run(tokio::process::Command::new(&ok)).await.unwrap()).unwrap(),
        AdminCredential::BearerToken(token) if token.as_str() == "ya29.token"
    ));
}

#[derive(Default)]
struct Center {
    polls: u32,
    deny: bool,
    shown: Vec<(String, String)>,
}

async fn center(
    State(state): State<Arc<Mutex<Center>>>,
    uri: Uri,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    match uri.path() {
        "/oidc/client/register" => {
            json(serde_json::json!({"clientId": "client", "clientSecret": "client-secret"}))
        }
        "/oidc/device_authorization" => {
            assert_eq!(body["startUrl"], "https://example.awsapps.com/start");
            json(serde_json::json!({
                "deviceCode": "device", "userCode": "ABCD-EFGH",
                "verificationUriComplete": "https://device.sso/?user_code=ABCD-EFGH",
                "interval": 1, "expiresIn": 60,
            }))
        }
        "/oidc/token" => {
            state.polls += 1;
            if state.deny {
                return response(
                    400,
                    serde_json::json!({"error": "access_denied"}).to_string(),
                );
            }
            if state.polls == 1 {
                return response(
                    400,
                    serde_json::json!({"error": "authorization_pending"}).to_string(),
                );
            }
            json(serde_json::json!({"accessToken": "sso-token"}))
        }
        "/portal/federation/credentials" => {
            assert_eq!(headers["x-amz-sso_bearer_token"], "sso-token");
            assert_eq!(
                uri.query().unwrap(),
                "account_id=123456789012&role_name=StorageAdmin"
            );
            json(serde_json::json!({"roleCredentials": {
                "accessKeyId": "ASIA", "secretAccessKey": "role-secret", "sessionToken": "session",
            }}))
        }
        _ => response(404, "{}"),
    }
}

struct Shown(Arc<Mutex<Center>>);
#[async_trait::async_trait]
impl DeviceSignInPresenter for Shown {
    async fn show(&self, url: &str, code: &str) -> Result<(), StorageError> {
        self.0.lock().unwrap().shown.push((url.into(), code.into()));
        Ok(())
    }
}

#[tokio::test]
async fn identity_center_waits_for_the_person_then_returns_role_credentials() {
    for deny in [false, true] {
        let state = Arc::new(Mutex::new(Center {
            deny,
            ..Center::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(center).with_state(state.clone())).await;
        let base = Url::parse(&server.url).unwrap();
        let result = identity_center(
            &IdentityCenterEndpoints {
                oidc: base.join("/oidc/").unwrap(),
                portal: base.join("/portal/").unwrap(),
            },
            "https://example.awsapps.com/start",
            "123456789012",
            "StorageAdmin",
            &Shown(state.clone()),
            Arc::new(SystemClock),
        )
        .await;
        assert_eq!(
            state.lock().unwrap().shown,
            [(
                "https://device.sso/?user_code=ABCD-EFGH".to_owned(),
                "ABCD-EFGH".to_owned()
            )]
        );
        if deny {
            assert_eq!(
                result.unwrap_err().failure(),
                StorageFailure::PermissionDenied
            );
            continue;
        }
        let AdminCredential::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } = result.unwrap()
        else {
            panic!("access key")
        };
        assert_eq!(access_key_id, "ASIA");
        assert_eq!(secret_access_key.as_str(), "role-secret");
        assert_eq!(session_token.unwrap().as_str(), "session");
        assert_eq!(state.lock().unwrap().polls, 2);
    }
}

#[test]
fn identity_center_endpoints_follow_the_region() {
    let endpoints = IdentityCenterEndpoints::for_region("eu-west-1").unwrap();
    assert_eq!(
        endpoints.oidc.as_str(),
        "https://oidc.eu-west-1.amazonaws.com/"
    );
    assert_eq!(
        endpoints.portal.as_str(),
        "https://portal.sso.eu-west-1.amazonaws.com/"
    );
    assert!(IdentityCenterEndpoints::for_region("eu/west").is_err());
}
