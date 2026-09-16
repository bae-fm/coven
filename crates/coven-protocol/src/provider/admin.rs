use super::probe::*;
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilityProof {
    pub exact_slots: ExactSlotProbeReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderProviderAdminGrant {
    pub grant_id: ProviderAdminGrantId,
    pub provider: ProviderDeviceBinding,
    pub access: ProviderAccessLocator,
    pub capability: ProviderCapabilityProof,
}

impl FounderProviderAdminGrant {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_test_label(label: &str) -> Self {
        let probe_id =
            ProviderProbeId::from_bytes(*ObjectHash::digest(label.as_bytes()).as_bytes());
        let slot = ObjectSlot::logical(format!("store-v1/test/{label}/provider-probe/exact"))
            .expect("valid exact-probe test slot");
        let first = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
        let second = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
        let accepted =
            ExactObjectRef::new(slot.clone(), first.len() as u64, ObjectHash::digest(&first));
        let conditional_slot =
            ObjectSlot::logical(format!("store-v1/test/{label}/provider-probe/conditional"))
                .expect("valid conditional-update test slot");
        let device = ProviderDeviceBinding {
            principal: crate::objects::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
        };
        let store = StoreProviderBinding::S3 {
            endpoint: crate::objects::S3EndpointBinding::Custom {
                origin: "https://test.invalid".to_string(),
            },
            region: "test-region".to_string(),
            bucket: format!("{label}-bucket"),
            key_prefix: None,
        };
        let transcript = ExactSlotProbeTranscript {
            probe_id,
            logical_key: slot.logical_key().to_string(),
            slot,
            contenders: [
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&first),
                    outcome: ProbeCreateOutcome::Created,
                },
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&second),
                    outcome: ProbeCreateOutcome::RejectedOccupied,
                },
            ],
            accepted: accepted.clone(),
            full_read_hash: accepted.stored_hash(),
            range: ProbeRangeReceipt {
                start: PROBE_RANGE_START,
                end: PROBE_RANGE_END,
                bytes_hash: ObjectHash::digest(
                    &first[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize],
                ),
            },
            conditional: crate::provider::test_fixtures::test_conditional_receipt(
                probe_id,
                conditional_slot,
            ),
        };
        Self {
            grant_id: ProviderAdminGrantId(ObjectHash::digest(
                format!("{label} provider admin grant").as_bytes(),
            )),
            provider: device.clone(),
            access: ProviderAccessLocator::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
            capability: ProviderCapabilityProof {
                exact_slots: ExactSlotProbeReceipt::from_transcript(transcript, &store, &device),
            },
        }
    }
}

impl ProviderCapabilityProof {
    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        device: &ProviderDeviceBinding,
    ) -> Result<(), ProviderProbeError> {
        self.exact_slots.verify(store, device)
    }
}
