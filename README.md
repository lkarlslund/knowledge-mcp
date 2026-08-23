# wikipedia-multistream-mcp

A local-first MCP server for downloading, indexing, searching, and reading
[Wikimedia multistream dumps](https://meta.wikimedia.org/wiki/Data_dumps/Dump_format#Multistream_dumps).
Large page bodies stay in Wikimedia's compressed bzip2 files. A compact title
index is published first, so title search and page reads become available while
the full-text body index continues building in the background.

> Wikimedia marks the XML dump family as deprecated. This project deliberately
> targets `pages-articles-multistream` because its companion offset index enables
> efficient random access to compressed page groups.

## Build and run

Go 1.25 or newer is required.

```sh
go build -o wikipedia-multistream-mcp .
./wikipedia-multistream-mcp serve
```

The backend listens on `http://127.0.0.1:8765/mcp` and stores runtime data in
`./data`. Both can be changed:

```sh
./wikipedia-multistream-mcp serve --listen 127.0.0.1:9000 --data-dir /srv/wiki-data
```

Downloads and indexing use independent worker pools, so indexing one wiki does
not block downloads of others. The defaults allow three download/update jobs
and two indexing jobs at once; tune them with `--download-workers` and
`--index-workers`. Files of at least 64 MiB use up to four parallel HTTP ranges,
shared globally across active downloads; tune that limit independently with
`--download-connections`. HTTP 429/503 responses back off and retry. Range
progress is persisted beside the staging file and safely resumes after restarts.

The listener is intentionally restricted to an explicit loopback IP. There is
no authentication layer.

Open `http://127.0.0.1:8765/` for the lightweight local dashboard. It shows
downloaded wikis, available upgrades, current/recent job progress, and online
dumps, and can submit download or update jobs. The page polls job snapshots; it
never holds a request open for background work.

## CLI

All commands below talk to the already-running backend and return after one MCP
request. Download and update jobs continue in the backend and must be polled.

```sh
# Browse completed online multistream dumps.
./wikipedia-multistream-mcp wiki available dawiki

# Submit a first download; the JSON response contains the job ID.
./wikipedia-multistream-mcp wiki download dawiki

# Poll once by ID or by wiki. Repeat from your shell as needed.
./wikipedia-multistream-mcp wiki status JOB_ID
./wikipedia-multistream-mcp wiki status --wiki dawiki

# List installed datasets and their title/full-text readiness.
./wikipedia-multistream-mcp wiki list

# Submit an update check for an installed wiki.
./wikipedia-multistream-mcp wiki update dawiki

./wikipedia-multistream-mcp search dawiki "Copenhagen architecture"
./wikipedia-multistream-mcp read dawiki "København"
./wikipedia-multistream-mcp read dawiki --page-id 12345 --format wikitext
```

Set `WIKIPEDIA_MULTISTREAM_MCP_SERVER` or pass `--server` when the backend uses
a non-default loopback port.

## MCP clients

Clients supporting Streamable HTTP can connect directly to:

```text
http://127.0.0.1:8765/mcp
```

For clients that only support stdio, configure this command after starting the
backend:

```sh
/absolute/path/wikipedia-multistream-mcp mcp stdio
```

## User service

The included systemd user unit follows the local installation layout used by
this repository:

```sh
mkdir -p ~/.local/lib/wikipedia-multistream-mcp ~/.config/systemd/user
go build -o ~/.local/lib/wikipedia-multistream-mcp/wikipedia-multistream-mcp .
cp contrib/systemd/wikipedia-multistream-mcp.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now wikipedia-multistream-mcp.service
```

Inspect it with `systemctl --user status wikipedia-multistream-mcp.service` or
`journalctl --user -u wikipedia-multistream-mcp.service`. Runtime wiki data
stays in this checkout's ignored `data/` directory.

The server offers these action-specific tools:

| Tool | Purpose |
| --- | --- |
| `wiki_list_available` | Filter and paginate online multistream dumps. |
| `wiki_list_local` | List installed wikis and index readiness. |
| `wiki_download` | Submit a first-time background download. |
| `wiki_update` | Submit an update or finish a missing body index. |
| `wiki_job_status` | Poll one job snapshot by ID or wiki. |
| `wiki_search` | Search titles, then full text when ready. |
| `wiki_read` | Read an exact page as text or wikitext. |

A download job ends after all compressed files pass size and checksum
verification. The server then creates a separate indexing job. Fresh downloads
appear under local wikis immediately with search mode `none`; title search and
page reads become available after the first indexing stage, while full-text
indexing continues. During an update the new generation remains staged, and
replaces the old one only after its title index is ready. Retrying failed
indexing does not download the dump again.

## Storage

Runtime data is not committed. Each installed wiki keeps:

- the original compressed XML dump;
- the compressed Wikimedia title/offset index;
- a title index containing page identity and stream offsets;
- a body-term index that does not store duplicate page bodies; and
- a manifest binding every index to its dump checksums.

Page reads seek to the indexed bzip2 stream and decompress only that page group.
Downloads are resumable, size-checked, and verified against Wikimedia SHA-1
metadata before publication. Response bodies are streamed directly to staging
files in bounded buffers rather than accumulated in memory. Job state and
partial files persist across server restarts; an interrupted job is requeued on
startup, and manually retrying a failed job resumes that job's existing files.
Large wikis such as `enwiki` are discovered and downloaded as ordered multipart
dump/index pairs.

Large sequential index scans use a parallel bzip2 decoder, and body indexing
also processes independent multistream chunks concurrently. Exact page reads
use a single-stream decoder to avoid parallel setup overhead.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
golangci-lint run ./...
govulncheck ./...
```

Tests generate small concatenated bzip2 fixtures and do not download a
production wiki.
