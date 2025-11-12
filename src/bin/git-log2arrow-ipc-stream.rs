use clap::Parser;
use std::io;

use rs_git_log2arrow_ipc_stream::log2arrow_ipc_stream_writer;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Trim trailing newline from commit messages
    #[arg(long)]
    trim_message: bool,

    /// Maximum number of log entries to process
    #[arg(long, default_value_t = 1024)]
    max_count: usize,

    /// Filter by author
    #[arg(long)]
    author: Option<String>,

    /// Show commits more recent than a specific date
    #[arg(long)]
    since: Option<String>,

    /// Show commits older than a specific date
    #[arg(long)]
    until: Option<String>,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let repo = gix::discover(".").map_err(io::Error::other)?;
    let mut stdout = io::stdout();

    log2arrow_ipc_stream_writer(
        &repo,
        &mut stdout,
        args.trim_message,
        args.max_count,
        args.author,
        args.since,
        args.until,
    )?;

    Ok(())
}
