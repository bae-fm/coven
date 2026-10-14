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
        .rows
        .iter()
        .cloned()
        .map(WriteFrame::Change)
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

/// Visit this audience's retained parts in timestamp order, which is causal order.
pub(crate) fn visit(
    db: &DatabaseConnection,
    target: &Audience,
    mut visit: impl FnMut(DownloadedWrite) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let text = audience_text(target);
    let headers = db.query(
        "SELECT timestamp,header FROM _coven_write_inputs WHERE audience=?1 ORDER BY timestamp",
        [&text],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )?;
    for (timestamp, header) in headers {
        let header =
            merge_fields::decode_write_header(&header).map_err(|_| DbError::DamagedDatabase)?;
        let mut part = WritePart {
            audience: target.clone(),
            rows: Vec::new(),
            dismissals: Vec::new(),
        };
        for frame in db.query(
            "SELECT frame FROM _coven_write_input_frames WHERE audience=?1 AND timestamp=?2 ORDER BY ordinal",
            params![text, timestamp],
            |r| r.get::<_, Vec<u8>>(0),
        )? {
            match WriteFrame::decode(&frame).map_err(|_| DbError::DamagedDatabase)? {
                WriteFrame::Change(row) => part.rows.push(row),
                WriteFrame::Dismissal(dismissal) => part.dismissals.push(dismissal),
            }
        }
        visit(DownloadedWrite {
            header,
            parts: vec![DownloadedPart::Opened(part)],
        })?;
    }
    Ok(())
}

/// Forget the retained parts once no replay can need them.
pub(crate) fn release(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_write_inputs WHERE audience=?1",
        [audience_text(audience)],
    )?;
    Ok(())
}
