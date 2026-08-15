use super::*;
use crate::keys::{test_keyring, CloudHomeCredentials, StoreKeys};

fn s3(access_key: &str, secret_key: &str) -> CloudHomeCredentials {
    CloudHomeCredentials::S3 {
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
    }
}

fn assert_s3(credentials: CloudHomeCredentials, expected_access: &str, expected_secret: &str) {
    let CloudHomeCredentials::S3 {
        access_key,
        secret_key,
    } = credentials
    else {
        panic!("expected S3 credentials");
    };
    assert_eq!(access_key, expected_access);
    assert_eq!(secret_key, expected_secret);
}

#[test]
fn proposed_credentials_remain_in_memory_until_commit() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-commit".to_string());
    keys.set_cloud_home_credentials(&s3("old-access", "old-secret"))
        .expect("seed previous credentials");
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let staged = owner.stage(Some(s3("proposed-access", "proposed-secret")));

    assert_s3(
        staged
            .unlock()
            .expect("unlock staged credentials")
            .expect("staged credentials exist"),
        "proposed-access",
        "proposed-secret",
    );
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read durable credentials")
            .expect("previous credentials remain durable"),
        "old-access",
        "old-secret",
    );

    staged.commit().expect("commit credentials");

    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read committed credentials")
            .expect("committed credentials exist"),
        "proposed-access",
        "proposed-secret",
    );
}

#[test]
fn rollback_restores_the_credentials_replaced_by_commit() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-rollback".to_string());
    keys.set_cloud_home_credentials(&s3("old-access", "old-secret"))
        .expect("seed previous credentials");
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let staged = owner.stage(Some(s3("proposed-access", "proposed-secret")));
    staged.commit().expect("commit proposed credentials");

    staged.rollback().expect("roll back credentials");

    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read restored credentials")
            .expect("previous credentials restored"),
        "old-access",
        "old-secret",
    );
}

#[test]
fn provider_refreshes_update_the_proposal_then_follow_the_committed_keyring() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-refresh".to_string());
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let staged = owner.stage(Some(s3("initial-access", "initial-secret")));

    staged
        .persist(&s3("precommit-access", "precommit-secret"))
        .expect("refresh staged credentials");
    assert!(
        keys.get_cloud_home_credentials()
            .expect("read durable credentials before commit")
            .is_none(),
        "a refresh before commit remains proposed",
    );
    staged.commit().expect("commit refreshed proposal");
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read first committed credentials")
            .expect("first committed credentials exist"),
        "precommit-access",
        "precommit-secret",
    );

    staged
        .persist(&s3("durable-access", "durable-secret"))
        .expect("refresh committed credentials");
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read refreshed durable credentials")
            .expect("refreshed durable credentials exist"),
        "durable-access",
        "durable-secret",
    );
}

#[test]
fn a_superseded_provider_cannot_overwrite_committed_credentials() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-supersede".to_string());
    keys.set_cloud_home_credentials(&s3("old-access", "old-secret"))
        .expect("seed previous credentials");
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let old_provider = owner.current();
    let staged = owner.stage(Some(s3("new-access", "new-secret")));

    staged.commit().expect("commit replacement credentials");
    let error = old_provider
        .persist(&s3("late-access", "late-secret"))
        .expect_err("the replaced provider must not overwrite new credentials");

    assert!(matches!(error, KeyError::CloudCredentialsSuperseded));
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read durable credentials")
            .expect("replacement credentials remain"),
        "new-access",
        "new-secret",
    );
}

#[test]
fn rollback_restores_the_previous_provider_lease() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-restore-lease".to_string());
    keys.set_cloud_home_credentials(&s3("old-access", "old-secret"))
        .expect("seed previous credentials");
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let old_provider = owner.current();
    let staged = owner.stage(Some(s3("new-access", "new-secret")));

    old_provider
        .persist(&s3("racing-access", "racing-secret"))
        .expect("the active provider remains valid while replacement is prepared");
    staged.commit().expect("commit replacement credentials");
    assert!(matches!(
        old_provider.persist(&s3("late-access", "late-secret")),
        Err(KeyError::CloudCredentialsSuperseded)
    ));
    staged
        .rollback()
        .expect("roll back replacement credentials");
    old_provider
        .persist(&s3("refreshed-access", "refreshed-secret"))
        .expect("the restored provider lease writes again");

    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read durable credentials")
            .expect("restored credentials exist"),
        "refreshed-access",
        "refreshed-secret",
    );
}

#[test]
fn credential_removal_commits_and_rolls_back_atomically() {
    test_keyring::install();
    let keys = StoreKeys::bind("staged-credentials-remove".to_string());
    keys.set_cloud_home_credentials(&s3("old-access", "old-secret"))
        .expect("seed previous credentials");
    let owner = CloudHomeCredentialsOwner::new(keys.clone());
    let staged = owner.stage(None);

    assert!(staged
        .unlock()
        .expect("unlock proposed credentials")
        .is_none());
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read durable credentials")
            .expect("credentials remain until commit"),
        "old-access",
        "old-secret",
    );
    staged.commit().expect("commit credential removal");
    assert!(keys
        .get_cloud_home_credentials()
        .expect("read removed credentials")
        .is_none());

    staged.rollback().expect("roll back credential removal");
    assert_s3(
        keys.get_cloud_home_credentials()
            .expect("read restored credentials")
            .expect("previous credentials restored"),
        "old-access",
        "old-secret",
    );
}
