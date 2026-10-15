//! Received and local write parts kept while an audience's data still depends
//! on a non-final boundary entry (§9). Retention starts when the audience has a
//! retained data input; every later part after that input's positions is kept.
use crate::{
    sqlite::DatabaseConnection, write_encoding::audience_text, DbError, DownloadedPart,
    DownloadedWrite,
};
use coven_format::{
    dismissal::WriteFrame,
    merge_fields,
    write::{WriteHeader, WritePart, WriteRecord},
};
use coven_merge::Audience;
use rusqlite::params;

/// Whether this audience has a retained input that later writes must extend.
pub(crate) fn retaining(db: &DatabaseConnection, audience: &Audience) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_data_inputs WHERE audience=?1)",
        [audience_text(audience)],
        |r| r.get(0),
    )
}

pub(crate) fn download(db: &DatabaseConnection, write: &DownloadedWrite) -> Result<(), DbError> {
    for part in &write.parts {
        if let DownloadedPart::Opened(part) = part {
            part_frames(db, &write.header, part)?;
        }
    }
    Ok(())
}

pub(crate) fn record(db: &DatabaseConnection, write: &WriteRecord) -> Result<(), DbError> {
    for part in &write.parts {
        part_frames(db, &write.header, part)?;
    }
    Ok(())
}

/// A whole part is retained once; a reload that supplies it again keeps the first copy.
fn part_frames(
    db: &DatabaseConnection,
    header: &WriteHeader,
    part: &WritePart,
) -> Result<(), DbError> {
    if !retaining(db, &part.audience)? {
        return Ok(());
    }
    let present: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_write_inputs WHERE audience=?1 AND timestamp=?2)",
        params![
            audience_text(&part.audience),
            merge_fields::encode_timestamp(&header.timestamp)?
        ],
        |r| r.get(0),
    )?;
    if present {
        return Ok(());
    }
    let frames = part
        .records()
        .map(|record| match record {
            coven_format::write::PartRecord::Change(row) => WriteFrame::Change(row.clone()),
            coven_format::write::PartRecord::Execution(execution) => {
                WriteFrame::Execution(execution.clone())
            }
        })
        .chain(part.dismissals.iter().cloned().map(WriteFrame::Dismissal));
    for frame in frames {
        self::frame(db, header, &part.audience, &frame)?;
    }
    Ok(())
}

/// Keep one streamed frame, in stream order, beside its write header.
pub(crate) fn frame(
    db: &DatabaseConnection,
    header: &WriteHeader,
    audience: &Audience,
    frame: &WriteFrame,
) -> Result<(), DbError> {
    if !retaining(db, audience)? {
        return Ok(());
    }
    let audience = audience_text(audience);
    let timestamp = merge_fields::encode_timestamp(&header.timestamp)?;
    db.internal_execute(
        "INSERT INTO _coven_write_inputs(audience,timestamp,header) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
        params![audience, timestamp, merge_fields::encode_write_header(header)?],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_write_input_frames(audience,timestamp,ordinal,frame)
         VALUES(?1,?2,(SELECT count(*) FROM _coven_write_input_frames WHERE audience=?1 AND timestamp=?2),?3)",
        params![audience, timestamp, merge_fields::encode_write_frame(frame)?],
    )?;
    Ok(())
}

/// Every retained write of these audiences, in timestamp order, which is
/// causal order, each once with all of its retained parts among them: a
/// write's foreign-key actions
/// can reach rows of several of them, and their verdicts are decided
/// together (§8.8).
pub(crate) fn visit(
    db: &DatabaseConnection,
    targets: &std::collections::BTreeSet<Audience>,
    mut visit: impl FnMut(DownloadedWrite) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let mut parts = Vec::new();
    for audience in targets {
        for (timestamp, header) in db.query(
            "SELECT timestamp,header FROM _coven_write_inputs WHERE audience=?1 ORDER BY timestamp",
            [audience_text(audience)],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )? {
            let header =
                merge_fields::decode_write_header(&header).map_err(|_| DbError::DamagedDatabase)?;
            parts.push((timestamp, header, audience));
        }
    }
    parts.sort_by(|a, b| {
        (&a.0, a.1.position.device, a.1.position.number, a.2).cmp(&(
            &b.0,
            b.1.position.device,
            b.1.position.number,
            b.2,
        ))
    });
    let mut current: Option<DownloadedWrite> = None;
    for (timestamp, header, audience) in parts {
        let part = DownloadedPart::Opened(part(db, audience, &timestamp)?);
        match &mut current {
            Some(write) if write.header.position == header.position => write.parts.push(part),
            _ => {
                if let Some(write) = current.take() {
                    visit(write)?;
                }
                current = Some(DownloadedWrite {
                    header,
                    parts: vec![part],
                });
            }
        }
    }
    if let Some(write) = current {
        visit(write)?;
    }
    Ok(())
}

/// One retained part's frames.
fn part(
    db: &DatabaseConnection,
    audience: &Audience,
    timestamp: &[u8],
) -> Result<WritePart, DbError> {
    let mut part = WritePart {
        audience: audience.clone(),
        rows: Vec::new(),
        executions: Vec::new(),
        dismissals: Vec::new(),
    };
    for frame in db.query(
        "SELECT frame FROM _coven_write_input_frames WHERE audience=?1 AND timestamp=?2 ORDER BY ordinal",
        params![audience_text(audience), timestamp],
        |r| r.get::<_, Vec<u8>>(0),
    )? {
        match WriteFrame::decode(&frame).map_err(|_| DbError::DamagedDatabase)? {
            WriteFrame::Change(row) => part.rows.push(row),
            WriteFrame::Execution(execution) => part.executions.push(execution),
            WriteFrame::Dismissal(dismissal) => part.dismissals.push(dismissal),
        }
    }
    Ok(part)
}

/// The headers of an audience's retained writes, in timestamp order.
pub(crate) fn headers(
    db: &DatabaseConnection,
    audience: &Audience,
) -> Result<Vec<WriteHeader>, DbError> {
    db.query(
        "SELECT header FROM _coven_write_inputs WHERE audience=?1 ORDER BY timestamp",
        [audience_text(audience)],
        |r| r.get::<_, Vec<u8>>(0),
    )?
    .into_iter()
    .map(|header| merge_fields::decode_write_header(&header).map_err(|_| DbError::DamagedDatabase))
    .collect()
}

/// Forget the retained parts once no replay can need them.
pub(crate) fn release(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_write_inputs WHERE audience=?1",
        [audience_text(audience)],
    )?;
    Ok(())
}
