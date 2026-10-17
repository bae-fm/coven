use super::*;

#[tokio::test]
async fn an_absent_user_is_not_listed_as_keyless() {
    for profile in [
        S3ProfileName::Aws,
        S3ProfileName::SeaweedFs,
        S3ProfileName::CephRgw,
        S3ProfileName::VersityGw,
    ] {
        let fake = Fake::new().await;
        let admin = fake.admin(profile);
        let scope = scope();
        let name = &scope.key_name;
        fake.state.lock().unwrap().listings.insert(
            "ListUsers".into(),
            format!("<ListUsersResponse><ListUsersResult><IsTruncated>false</IsTruncated><Users><member><UserName>{name}</UserName></member></Users></ListUsersResult></ListUsersResponse>").into_bytes(),
        );
        assert_eq!(
            admin.list_keys(scope.store).await.unwrap_err().failure(),
            StorageFailure::NotFound,
            "{profile:?}"
        );
    }
}

#[tokio::test]
async fn malformed_key_listings_are_errors() {
    for profile in [
        S3ProfileName::Aws,
        S3ProfileName::SeaweedFs,
        S3ProfileName::CephRgw,
        S3ProfileName::VersityGw,
    ] {
        let fake = Fake::new().await;
        let admin = fake.admin(profile);
        let scope = scope();
        admin.mint(&scope).await.unwrap();
        for (action, collection, fields) in [
            ("ListUsers", "Users", "<UserName>user</UserName>"),
            (
                "ListAccessKeys",
                "AccessKeyMetadata",
                "<AccessKeyId>key</AccessKeyId>",
            ),
            (
                "ListPolicies",
                "Policies",
                "<PolicyName>policy</PolicyName><Arn>arn</Arn>",
            ),
        ] {
            if action == "ListPolicies" && profile != S3ProfileName::SeaweedFs {
                continue;
            }
            for body in [
                format!("<{action}Response/>"),
                format!("<{collection}/><IsTruncated>false</IsTruncated>"),
                format!("<{action}Response><{action}Result><{collection}/></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>maybe</IsTruncated><{collection}/></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>true</IsTruncated><{collection}/></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>false</IsTruncated><{collection}><member/></{collection}></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>false</IsTruncated><{collection}><member>{fields}</wrong></{collection}></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>false</IsTruncated><IsTruncated>false</IsTruncated><{collection}/></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>false</IsTruncated><{collection}><member>{fields}{fields}</member></{collection}></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>false</IsTruncated><{collection}>missing records</{collection}></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>true</IsTruncated><Marker/><{collection}/></{action}Result></{action}Response>"),
                format!("<{action}Response><{action}Result><IsTruncated>true</IsTruncated><Marker>repeat</Marker><{collection}/></{action}Result></{action}Response>"),
            ] {
                fake.state.lock().unwrap().listings.insert(action.into(), body.clone().into_bytes());
                assert_eq!(admin.list_keys(scope.store).await.unwrap_err().failure(), StorageFailure::Protocol, "{profile:?} {action}: {body}");
            }
            fake.state.lock().unwrap().listings.remove(action);
        }
    }
}

#[tokio::test]
async fn malformed_key_listing_encoding_is_an_error() {
    let fake = Fake::new().await;
    fake.state.lock().unwrap().listings.insert(
        "ListUsers".into(),
        b"<ListUsersResponse><ListUsersResult><IsTruncated>false</IsTruncated><Users><member><UserName>\xff</UserName></member></Users></ListUsersResult></ListUsersResponse>".to_vec(),
    );
    let admin = fake.admin(S3ProfileName::Aws);
    assert_eq!(
        admin.list_keys(scope().store).await.unwrap_err().failure(),
        StorageFailure::Protocol,
    );
}
