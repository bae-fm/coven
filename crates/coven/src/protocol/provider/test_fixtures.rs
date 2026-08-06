//! Provider values every layer's tests build against: the bindings a store and
//! a device are probed under, and a receipt that verifies against both.

use super::*;
use crate::protocol::store_commit::ObjectHash;

pub(crate) fn test_store_binding() -> crate::protocol::objects::StoreProviderBinding {
    crate::protocol::objects::StoreProviderBinding::S3 {
        endpoint: crate::protocol::objects::S3EndpointBinding::Aws {
            partition: "aws".to_string(),
        },
        region: "us-east-1".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: None,
    }
}

pub(crate) fn test_device_binding() -> crate::protocol::objects::ProviderDeviceBinding {
    crate::protocol::objects::ProviderDeviceBinding {
        principal: crate::protocol::objects::ProviderPrincipalId::Aws {
            account_id: "123456789012".to_string(),
            principal: crate::protocol::objects::AwsPrincipal::Root,
        },
    }
}

pub(crate) fn test_exact_receipt() -> ExactSlotProbeReceipt {
    let probe_id = ProviderProbeId::from_bytes([7; 32]);
    let slot =
        crate::protocol::objects::ObjectSlot::logical("store-v1/probes/exact".to_string()).unwrap();
    let first = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
    let second = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
    let accepted = crate::protocol::objects::ExactObjectRef::new(
        slot.clone(),
        first.len() as u64,
        ObjectHash::digest(&first),
    );
    let lost_slot =
        crate::protocol::objects::ObjectSlot::logical("store-v1/probes/lost".to_string()).unwrap();
    let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
    let lost_ref = crate::protocol::objects::ExactObjectRef::new(
        lost_slot.clone(),
        lost_payload.len() as u64,
        ObjectHash::digest(&lost_payload),
    );
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
        lost_response: LostResponseProbeReceipt {
            logical_key: "store-v1/probes/lost".to_string(),
            slot: lost_slot,
            payload_hash: ObjectHash::digest(&lost_payload),
            settled: lost_ref,
            readback_hash: ObjectHash::digest(&lost_payload),
        },
    };
    ExactSlotProbeReceipt::from_transcript(
        transcript,
        &test_store_binding(),
        &test_device_binding(),
    )
}
