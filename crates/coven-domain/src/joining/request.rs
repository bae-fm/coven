use serde::{Deserialize, Serialize};

use coven_foundation::code_envelope::{self, EnvelopeError};

/// The public identity and optional provider account address a joining device
/// hands to an existing member before that member creates its invitation.
#[derive(Serialize, Deserialize, Debug)]
pub struct JoinRequest {
    pub public_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Generate a join request for `keypair`.
pub fn generate_join_request_for_keypair(
    keypair: &coven_keys::keys::UserKeypair,
    email: Option<String>,
) -> String {
    encode_join_request(&JoinRequest {
        public_key: hex::encode(keypair.public_key()),
        email,
    })
}

/// Encode the request exchanged between the joining and admitting devices.
pub fn encode_join_request(request: &JoinRequest) -> String {
    code_envelope::encode_code("", request)
}

pub fn decode_join_request(value: &str) -> Result<JoinRequest, JoinRequestError> {
    Ok(code_envelope::decode_code("", value)?)
}

/// Build a join request and retain its pending device identity until the join
/// completes or the request is abandoned.
pub fn generate_join_request(email: Option<String>) -> Result<String, coven_keys::keys::KeyError> {
    let keypair = coven_keys::keys::mint_pending_identity()?;
    Ok(generate_join_request_for_keypair(&keypair, email))
}

/// Discard the pending identity retained for an abandoned request.
pub fn abandon_join_request(request_code: &str) -> Result<(), AbandonJoinRequestError> {
    let request = decode_join_request(request_code)?;
    coven_keys::keys::discard_pending_identity(&request.public_key)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum AbandonJoinRequestError {
    #[error("invalid join request: {0}")]
    Request(#[from] JoinRequestError),
    #[error("failed to discard pending identity: {0}")]
    Key(#[from] coven_keys::keys::KeyError),
}

#[derive(Debug, thiserror::Error)]
#[error("invalid join request: {0}")]
pub struct JoinRequestError(#[from] EnvelopeError);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_request_round_trips_with_email() {
        let request = JoinRequest {
            public_key: "abcdef1234567890".into(),
            email: Some("user@example.com".into()),
        };
        let decoded = decode_join_request(&encode_join_request(&request)).unwrap();
        assert_eq!(decoded.public_key, "abcdef1234567890");
        assert_eq!(decoded.email, Some("user@example.com".to_string()));
    }

    #[test]
    fn join_request_round_trips_without_email() {
        let request = JoinRequest {
            public_key: "deadbeef".into(),
            email: None,
        };
        let decoded = decode_join_request(&encode_join_request(&request)).unwrap();
        assert_eq!(decoded.public_key, "deadbeef");
        assert_eq!(decoded.email, None);
    }

    #[test]
    fn join_request_trims_whitespace() {
        let request = JoinRequest {
            public_key: "aabbccdd".into(),
            email: None,
        };
        let encoded = format!("  {} \n", encode_join_request(&request));
        assert_eq!(
            decode_join_request(&encoded).unwrap().public_key,
            "aabbccdd"
        );
    }
}
