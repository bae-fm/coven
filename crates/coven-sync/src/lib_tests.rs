pub(crate) fn io_runner(cases: &[serde_json::Value]) -> Vec<serde_json::Value> {
    serde_json::from_value(run_lean("COVEN_IO_LEAN", &serde_json::json!(cases))).unwrap()
}

pub(crate) fn run_lean(variable: &str, cases: &serde_json::Value) -> serde_json::Value {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let runner = std::env::var(variable).expect("Lean runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

pub(crate) async fn apply_write(
    database: &coven_database::Database,
    record: coven_format::write::WriteRecord,
) -> Result<coven_database::ApplyOutcome, coven_database::DbError> {
    use coven_database::{DownloadedPartStream, DownloadedWriteStream, IncomingSlot, SlotContent};
    let position = record.header.position;
    let encoder = coven_format::write_stream::WriteEncoder::new(&record)?;
    let stream = DownloadedWriteStream {
        header: encoder.header().clone(),
        signer: coven_database::test_utils::signer(),
        authorship: coven_database::test_utils::TestAuthorship::write(&record),
        parts: (0..record.parts.len())
            .map(|index| {
                let chunks = encoder.part_chunks(index)?.collect::<Result<Vec<_>, _>>()?;
                Ok(DownloadedPartStream::Opened(std::io::Cursor::new(
                    chunks.concat(),
                )))
            })
            .collect::<Result<_, coven_format::Error>>()?,
    };
    database
        .apply_slot::<crate::PendingRecord, _, _>(
            IncomingSlot {
                slot: coven_merge::Slot(position.number),
                digest: [1; 32],
                header: coven_format::sequence::SlotHeader {
                    device: position.device,
                    claim: position.number,
                    author: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(
                        position.device.0 as u128,
                    )),
                    after: position.number - 1,
                },
                content: SlotContent::Writes(vec![stream]),
            },
            || Ok(()),
            None,
        )
        .await
}
