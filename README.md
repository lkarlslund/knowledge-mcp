# Knowledge Dataset MCP

A local-first MCP server for downloading, indexing, searching, and reading large
reference datasets. Providers own discovery, variants, acquisition, raw-record
access, and on-demand conversion. The core owns background jobs, atomic dataset
generations, local indexes, ranking, MCP, and the dashboard.

Included providers:

- `wikimedia`: Wikimedia article multistream snapshots, with one `multistream` variant.
- `rfc`: the complete RFC Series from the RFC Editor, with a `text` variant.
- `kiwix`: the complete Kiwix OPDS catalog, grouped into datasets and archive flavours.

Provider URLs and local paths are not part of the agent contract. Search returns
a short, stable, opaque document `id`; `knowledge_read` accepts that ID together
with the dataset ID. Markdown is the default read format, including sparse links
such as `knowledge-read://read?dataset=rfc&id=9110` that tell an agent how to
follow related documents.

![Knowledge Dataset MCP dashboard in dark mode](docs/dashboard-dark.png)

## Build and run

Go 1.25 or newer is required.

```sh
go build -o wikipedia-multistream-mcp .
./wikipedia-multistream-mcp serve
```

The server listens on `http://127.0.0.1:8765`; its MCP endpoint is
`http://127.0.0.1:8765/mcp` and its dashboard is
`http://127.0.0.1:8765/`. Runtime data defaults to `./data`.

```sh
./wikipedia-multistream-mcp serve \
  --listen 127.0.0.1:9000 \
  --data-dir /srv/knowledge-data \
  --download-workers 3 \
  --index-workers 1 \
  --download-connections 3
```

Downloads and indexing use independent worker pools. One index job runs by
default to avoid competing Bleve segment merges; each job still uses the
configured internal decompression parallelism. Jobs submit and return
immediately; poll their IDs for status. Partial provider downloads are preserved
and resume after restarts. Updates are built in staging and atomically replace
the installed generation only after the title index is usable.

The listener intentionally requires an explicit loopback IP and has no
authentication layer.

Runtime profiles are available only on that loopback listener under
`/debug/pprof/`, for example `go tool pprof http://127.0.0.1:8765/debug/pprof/profile?seconds=30`.

## CLI

All non-server commands call the running MCP backend once.

```sh
# Discover datasets from all providers.
./wikipedia-multistream-mcp dataset available
./wikipedia-multistream-mcp dataset available rfc
./wikipedia-multistream-mcp dataset available devdocs

# Download the RFC text variant, then poll/control its jobs.
./wikipedia-multistream-mcp dataset download rfc --variant text
./wikipedia-multistream-mcp dataset status JOB_ID
./wikipedia-multistream-mcp dataset status --dataset rfc
./wikipedia-multistream-mcp dataset job JOB_ID --action pause
./wikipedia-multistream-mcp dataset job JOB_ID --action resume
./wikipedia-multistream-mcp dataset job JOB_ID --action cancel
./wikipedia-multistream-mcp dataset job JOB_ID --action retry

# Installed datasets, updates, search, and reads.
./wikipedia-multistream-mcp dataset list
./wikipedia-multistream-mcp dataset update rfc
./wikipedia-multistream-mcp search rfc "HTTP status codes"
./wikipedia-multistream-mcp search rfc "RFC 9110" --mode full_text
./wikipedia-multistream-mcp read rfc --id 9110
./wikipedia-multistream-mcp read rfc --id 9110 --format source

# Wikimedia remains a provider, not a special core concept.
./wikipedia-multistream-mcp dataset download dawiki --variant multistream
./wikipedia-multistream-mcp search dawiki "Copenhagen architecture"
./wikipedia-multistream-mcp read dawiki --id 12345
```

Set `KNOWLEDGE_DATASET_MCP_SERVER` or pass `--server` for a non-default
endpoint. The legacy `WIKIPEDIA_MULTISTREAM_MCP_SERVER` name remains accepted.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `knowledge_list_available` | Discover datasets, providers, and variants. |
| `knowledge_list_local` | List installed datasets and selection/search metadata. |
| `knowledge_status` | Inspect worker settings, provider health, and update scheduling. |
| `knowledge_download` | Submit a first-time background download. |
| `knowledge_update` | Submit an atomic background update or finish indexing. |
| `knowledge_job_status` | Poll one job by `job_id` or `dataset`. |
| `knowledge_job` | `pause`, `resume`, `cancel`, or `retry` a job. |
| `knowledge_search` | Search a local dataset and return stable opaque document IDs. |
| `knowledge_read` | Read by `dataset` plus `id`, or by exact title. |

`knowledge_read` returns Markdown by default. `format: "text"` requests a lossy
plain-text representation; `format: "source"` returns the provider-native raw
record. Large documents return `offset`, `returned_chars`, `total_chars`,
`truncated`, and `next_offset` for continuation.

Wikimedia reads preserve headings, lists, tables, links, infobox fields,
citations, redirects, outlines, and bounded references. RFC reads add lifecycle
status and structured relationships and convert RFC references into followable
Markdown links. Kiwix reads ZIM files natively and converts stored HTML,
including tables and internal links, to Markdown.

## Provider architecture

The core contract is in `internal/provider`. Implementations are isolated:

```text
internal/provider/
├── provider.go          discovery, acquisition, and common corpus boundary
├── kiwix/               OPDS catalog, native ZIM reader, HTML-to-Markdown
├── rfc/                 RFC Editor catalog, raw text, and Markdown
└── wikimedia/           Wikimedia discovery and multistream adapter
```

A provider supplies:

- a stable provider ID and ownership of dataset IDs;
- discovery metadata and one or more variants;
- current release metadata and a release fingerprint;
- resumable acquisition into a staging generation;
- resumable title/body record scans through the common corpus interface;
- provider-owned descriptions, scope, topics, cadence, and source features;
- stable identifiers, aliases, keywords, lifecycle status, and rank weight; and
- on-demand conversion to Markdown, text, or native source.

Providers never build or query search indexes. The provider-neutral
`internal/knowledgeindex` package consumes the common corpus, owns the single
Bleve schema and ranking pipeline, and checkpoints only at provider-declared
safe cursors. Title and body builds therefore resume after service restarts for
every provider without provider-specific index formats.

The store sees datasets only. Manifests persist `provider`, `dataset`, and
`variant`; jobs and MCP responses use the same vocabulary. Existing installations
using the old `data/wikis` directory and legacy `wiki` manifest/job fields are
migrated when the server starts. Local generations live at
`data/datasets/<provider>/<dataset>` so provider-owned data cannot collide.

The RFC provider uses the official RFC Editor XML catalog and canonical text
documents. RFC numbers are its stable document IDs. Existing raw RFC files are
hard-linked or copied into an update generation, while new documents are
downloaded in parallel. Individual partial files use HTTP ranges when resumed.

The Kiwix provider caches and searches the complete OPDS catalog, resolves
Metalink mirrors, resumes partial ZIM downloads with byte ranges, verifies
SHA-256, and supports uncompressed, zlib, bzip2, XZ, and Zstandard clusters.
Zstandard decoding uses Klaus Post's optimized Go implementation.

## Dashboard

The embedded Bootstrap/Alpine.js dashboard shows local datasets, provider and
variant metadata, document counts, storage attribution, upgrades, and live job
progress. The complete provider catalog is searchable in a modal, with language
and installed-state filters. Dataset variants are selected before download.
Updates arrive over a WebSocket; no CDN is required. The gear menu changes
download/index worker limits at runtime, controls per-index decompression
parallelism, configures periodic update checks and optional automatic updates,
and reports provider catalog health. Settings persist in `data/settings.json`.

## User service

```sh
mkdir -p ~/.local/lib/wikipedia-multistream-mcp \
  ~/.local/share/wikipedia-multistream-mcp/data \
  ~/.config/systemd/user
go build -o ~/.local/lib/wikipedia-multistream-mcp/wikipedia-multistream-mcp .
cp contrib/systemd/wikipedia-multistream-mcp.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now wikipedia-multistream-mcp.service
```

Inspect it with:

```sh
systemctl --user status wikipedia-multistream-mcp.service
journalctl --user -u wikipedia-multistream-mcp.service
```

An agent installing a release should download the matching binary and `.sha256`
asset from the latest GitHub release, verify it with `sha256sum -c`, install the
binary and included user unit at the paths above, start the service, verify
`http://127.0.0.1:8765/healthz`, and register the exact Streamable HTTP MCP URL
`http://127.0.0.1:8765/mcp`. Stdio-only clients can run:

```sh
~/.local/lib/wikipedia-multistream-mcp/wikipedia-multistream-mcp mcp stdio
```

Daily rolling releases provide Linux, macOS, and Windows binaries for AMD64 and
ARM64, with SHA-256 checksums.

## Storage and indexing

Runtime data is not committed. Each installed dataset has a provider-owned raw
area, provider metadata, compact title index, body-term index, and manifest.
Body text is not duplicated as stored fields in the search index. Wikimedia
page reads seek directly to indexed bzip2 stream boundaries; RFC reads open the
canonical local text document; Kiwix reads only the referenced ZIM cluster.
Title search becomes available before the body index finishes. Search supports
`auto`, `title`, and `full_text` modes, with BM25 body relevance plus generic
title, alias, keyword, exact-identifier, lifecycle, and provider rank signals.

## License

Knowledge Dataset MCP is released under the [MIT License](LICENSE).
