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

The listener is intentionally restricted to an explicit loopback IP. There is
no authentication layer.

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

Jobs progress through discovery, download, title indexing, title readiness, and
body indexing. A new generation replaces an old one only after its dump and
title index have been verified. If body indexing fails, title search and page
reads remain available, and a later update submission retries that stage.

## Storage

Runtime data is not committed. Each installed wiki keeps:

- the original compressed XML dump;
- the compressed Wikimedia title/offset index;
- a title index containing page identity and stream offsets;
- a body-term index that does not store duplicate page bodies; and
- a manifest binding every index to its dump checksums.

Page reads seek to the indexed bzip2 stream and decompress only that page group.
Downloads are resumable, size-checked, and verified against Wikimedia SHA-1
metadata before publication.

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
