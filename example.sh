#!/bin/sh

echo example 1
./target/release/git-log2arrow-ipc-stream \
	--max-count 3 \
	--trim-message |
	arrow-cat

echo
echo example 2 using sql
./target/release/git-log2arrow-ipc-stream \
	--since=2025-11-09T20:32:00.0Z \
	--trim-message |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'git_log' \
	--sql "
		SELECT
			commit_hash,
			author_name,
			author_timestamp,
			message
		FROM git_log
		WHERE author_name LIKE 'y%'
		ORDER BY author_timestamp DESC
	" |
	rs-arrow-ipc-stream-cat
