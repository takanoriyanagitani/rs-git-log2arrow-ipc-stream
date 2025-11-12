use arrow::array::{ArrayRef, ListBuilder, StringBuilder, TimestampSecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use gix::Repository;
use gix::bstr::ByteSlice;
use std::io;
use std::io::Write;
use std::sync::Arc;

pub fn get_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("commit_hash", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("author_name", DataType::Utf8, false),
        Field::new("author_email", DataType::Utf8, false),
        Field::new(
            "author_timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("committer_name", DataType::Utf8, false),
        Field::new("committer_email", DataType::Utf8, false),
        Field::new(
            "committer_timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new(
            "parent_hashes",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ])
}

pub fn log2arrow_ipc_stream_writer<W>(
    repo: &Repository,
    wtr: &mut W,
    trim_message: bool,
    max_count: usize,
    author_filter: Option<String>,
    since: Option<String>,
    until: Option<String>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let schema = get_arrow_schema();
    let mut writer = StreamWriter::try_new(wtr, &schema).map_err(io::Error::other)?;

    let head = repo.head_commit().map_err(io::Error::other)?;

    let since_timestamp = since
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.timestamp());
    let until_timestamp = until
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.timestamp());

    let mut hash_builder = StringBuilder::new();
    let mut message_builder = StringBuilder::new();
    let mut author_name_builder = StringBuilder::new();
    let mut author_email_builder = StringBuilder::new();
    let mut author_timestamp_builder = TimestampSecondBuilder::new();
    let mut committer_name_builder = StringBuilder::new();
    let mut committer_email_builder = StringBuilder::new();
    let mut committer_timestamp_builder = TimestampSecondBuilder::new();
    let mut parent_hashes_builder = ListBuilder::new(StringBuilder::new());

    let commits = head.ancestors().all().map_err(io::Error::other)?;
    for commit_info in commits.take(max_count) {
        let commit_info = commit_info.map_err(io::Error::other)?;
        let commit = commit_info.object().map_err(io::Error::other)?;

        let author = commit.author().map_err(io::Error::other)?;

        if let Some(filter_author) = &author_filter
            && author.name != *filter_author
        {
            continue;
        }

        if let Some(since) = since_timestamp
            && author.time.seconds < since
        {
            continue;
        }

        if let Some(until) = until_timestamp
            && author.time.seconds > until
        {
            continue;
        }

        hash_builder.append_value(commit_info.id.to_string());

        let message = commit.message_raw().map_err(io::Error::other)?;
        if trim_message {
            message_builder.append_value(&String::from_utf8_lossy(message.trim_end()));
        } else {
            message_builder.append_value(message.to_string());
        }

        author_name_builder.append_value(author.name.to_string());
        author_email_builder.append_value(author.email.to_string());
        author_timestamp_builder.append_value(author.time.seconds);

        let committer = commit.committer().map_err(io::Error::other)?;
        committer_name_builder.append_value(committer.name.to_string());
        committer_email_builder.append_value(committer.email.to_string());
        committer_timestamp_builder.append_value(committer.time.seconds);

        let parents_builder = parent_hashes_builder.values();
        for parent_id in commit.parent_ids() {
            parents_builder.append_value(parent_id.to_string());
        }
        parent_hashes_builder.append(true);
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(hash_builder.finish()) as ArrayRef,
            Arc::new(message_builder.finish()) as ArrayRef,
            Arc::new(author_name_builder.finish()) as ArrayRef,
            Arc::new(author_email_builder.finish()) as ArrayRef,
            Arc::new(author_timestamp_builder.finish()) as ArrayRef,
            Arc::new(committer_name_builder.finish()) as ArrayRef,
            Arc::new(committer_email_builder.finish()) as ArrayRef,
            Arc::new(committer_timestamp_builder.finish()) as ArrayRef,
            Arc::new(parent_hashes_builder.finish()) as ArrayRef,
        ],
    )
    .map_err(io::Error::other)?;

    writer.write(&batch).map_err(io::Error::other)?;
    writer.finish().map_err(io::Error::other)?;

    Ok(())
}
