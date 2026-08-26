# Knowledge MCP

A local-first MCP server for downloading, indexing, searching, and reading large
reference datasets. Providers own discovery, variants, acquisition, raw-record
access, and on-demand conversion. The core owns background jobs, atomic dataset
generations, local indexes, ranking, MCP, and the dashboard.

Included providers:

- `wikimedia`: official monthly Wikimedia current-content XML exports.
- `rfc`: the complete RFC Series from the RFC Editor, with a `text` variant.
- `kiwix`: the complete Kiwix OPDS catalog, grouped into datasets and archive flavours.
- `ncbi`: the PubMed annual baseline plus ordered daily additions, revisions, and deletions.
- `eurlex`: official EU legal acts currently in force, in any of the 24 official EU languages.

Provider URLs and local paths are not part of the agent contract. Search returns
a temporary opaque `ref`; agents pass only that value to `knowledge_read`, without
retaining provider or dataset routing details. References use ordinary fast random
identifiers, persist across service restarts, and expire after seven inactive days.
Markdown is the default read format, with embedded links carrying another opaque
reference that an agent can follow with `knowledge_read`.

![Knowledge MCP dashboard in dark mode](docs/dashboard-dark.png)

## Build and run

Go 1.25 or newer is required.

```sh
go build -o knowledge-mcp .
./knowledge-mcp serve
```

The server listens on `http://127.0.0.1:8765`; its MCP endpoint is
`http://127.0.0.1:8765/mcp` and its dashboard is
`http://127.0.0.1:8765/`. Runtime data defaults to `./data`.

```sh
./knowledge-mcp serve \
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
the installed generation only after both its title index and shared-search
generation are ready.

The listener intentionally requires an explicit loopback IP and has no
authentication layer.

Runtime profiles are available only on that loopback listener under
`/debug/pprof/`, for example `go tool pprof http://127.0.0.1:8765/debug/pprof/profile?seconds=30`.

## CLI

All non-server commands call the running MCP backend once.

```sh
# Discover datasets from all providers.
./knowledge-mcp dataset available
./knowledge-mcp dataset available rfc
./knowledge-mcp dataset available devdocs
./knowledge-mcp dataset available pubmed

# Download the RFC text variant, then poll/control its jobs.
./knowledge-mcp dataset download rfc --variant text
./knowledge-mcp dataset download pubmed --variant baseline
./knowledge-mcp dataset download eurlex-in-force --variant en
./knowledge-mcp dataset status JOB_ID
./knowledge-mcp dataset status --dataset rfc
./knowledge-mcp dataset job JOB_ID --action pause
./knowledge-mcp dataset job JOB_ID --action resume
./knowledge-mcp dataset job JOB_ID --action cancel
./knowledge-mcp dataset job JOB_ID --action retry

# Installed datasets, updates, search, and reads.
./knowledge-mcp dataset list
./knowledge-mcp dataset update rfc
./knowledge-mcp search rfc "HTTP status codes"
./knowledge-mcp search rfc "RFC 9110" --mode full_text
./knowledge-mcp read rfc --id 9110

# Wikimedia remains a provider, not a special core concept.
./knowledge-mcp dataset download dawiki --variant content-current
./knowledge-mcp search dawiki "Copenhagen architecture"
./knowledge-mcp read dawiki --id 12345
```

Set `KNOWLEDGE_MCP_SERVER` or pass `--server` for a non-default endpoint.

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
| `knowledge_search` | Search one local dataset, or omit `dataset` to search the shared corpus of all ready datasets; supports publication-date bounds and newest/oldest sorting and returns opaque references. |
| `knowledge_read` | Read by opaque `ref`; `dataset` plus `id` or exact title remains supported for compatibility. |

`knowledge_read` always returns Markdown. Large documents return `offset`,
`returned_chars`, `total_chars`, `truncated`, and `next_offset` for continuation.

Wikimedia reads preserve headings, lists, tables, links, infobox fields,
citations, redirects, outlines, and bounded references. RFC reads add lifecycle
status and structured relationships and convert RFC references into followable
Markdown links. Kiwix reads ZIM files natively and converts stored HTML,
including tables and internal links, to Markdown.
PubMed reads preserve citation metadata, abstracts, publication types, identifiers,
MeSH terms, and available creation/publication/revision dates. EUR-Lex reads convert official XHTML and tables to Markdown and
turn CELEX cross-references into followable knowledge links.

## Provider architecture

The core contract is in `internal/provider`. Implementations are isolated:

```text
internal/provider/
├── provider.go          discovery, acquisition, and common corpus boundary
├── eurlex/              CELLAR discovery, EU legal XHTML, and Markdown
├── kiwix/               OPDS catalog, native ZIM reader, HTML-to-Markdown
├── ncbi/                PubMed baseline + daily updates, XML, and Markdown
├── rfc/                 RFC Editor catalog, raw text, and Markdown
└── wikimedia/           Current-content discovery, XML, and Markdown
```

A provider supplies:

- a stable provider ID and ownership of dataset IDs;
- discovery metadata and one or more variants;
- current release metadata and a release fingerprint;
- resumable acquisition into a staging generation;
- resumable title/body record scans through the common corpus interface;
- provider-owned descriptions, scope, topics, cadence, and source features;
- stable identifiers, aliases, keywords, lifecycle status, temporal metadata, and rank weight; and
- on-demand conversion to Markdown.

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

The NCBI provider applies the official annual PubMed baseline followed by daily
update files in numeric order, including replacement records and deletion
tombstones. Acquisition reuses unchanged compressed parts, resumes new parts with
HTTP ranges, and verifies NCBI's published MD5 for each completed part. The EUR-Lex provider discovers in-force sector 3 legal acts
through the Publications Office CELLAR endpoint and stores the selected official
language as resumable XHTML documents. Updates for both providers are built as
new staging generations and reuse unchanged local source files.

## Dashboard

The embedded Bootstrap/Alpine.js dashboard shows local datasets, provider and
variant metadata, document counts, storage attribution, upgrades, and live job
progress. The complete provider catalog is searchable in a modal, with language
and installed-state filters. Dataset variants are selected before download.
Updates arrive over a WebSocket; no CDN is required. The gear menu changes
download/index worker limits at runtime, controls per-index decompression
parallelism, configures periodic update checks and optional automatic updates,
and reports provider catalog health. Provider catalogs persist as atomic snapshots
under `data/catalogs`, refresh automatically when the configured update interval
expires, can be refreshed immediately from Settings, and remain usable while an
upstream catalog is temporarily unavailable. Settings persist in `data/settings.json`.

## User service

```sh
mkdir -p ~/.local/lib/knowledge-mcp \
  ~/.local/share/knowledge-mcp/data \
  ~/.config/systemd/user
go build -o ~/.local/lib/knowledge-mcp/knowledge-mcp .
cp contrib/systemd/knowledge-mcp.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now knowledge-mcp.service
```

Inspect it with:

```sh
systemctl --user status knowledge-mcp.service
journalctl --user -u knowledge-mcp.service
```

An agent installing a release should download the matching binary and `.sha256`
asset from the latest GitHub release, verify it with `sha256sum -c`, install the
binary and included user unit at the paths above, start the service, verify
`http://127.0.0.1:8765/healthz`, and register the exact Streamable HTTP MCP URL
`http://127.0.0.1:8765/mcp`. Stdio-only clients can run:

```sh
~/.local/lib/knowledge-mcp/knowledge-mcp mcp stdio
```

Daily rolling releases provide Linux, macOS, and Windows binaries for AMD64 and
ARM64, with SHA-256 checksums.

## Storage and indexing

Runtime data is not committed. Each installed dataset has a provider-owned raw
area, provider metadata, a compact lookup/title index, and a manifest. Searchable
documents from every provider feed one logical shared BM25 corpus, uniformly
striped across physical Bleve shards. Body text is indexed but not duplicated as
a stored field. Wikimedia reads open only the page-range bzip2 source file recorded
for the selected page;
RFC reads open the canonical local Markdown source; Kiwix reads only the referenced
ZIM cluster; PubMed reads one compressed baseline or update part; and EUR-Lex reads one local
XHTML document.

An initial install exposes title lookup while shared indexing continues. An
update builds a hidden shared generation and atomically publishes its provider
data, title lookup, and search generation when all are ready. Searches are always
filtered by the durable active-generation manifest, so retired or partially built
documents cannot leak into results. Removing a dataset first removes its generation
from that manifest; a managed background cleanup then reclaims its index terms.
Interrupted builds resume from provider-declared safe cursors. Search supports
`auto`, `title`, and `full_text` modes, with shared BM25 body relevance plus generic
title, alias, keyword, exact-identifier, lifecycle, and provider rank signals.
Providers also expose dates when their sources define them: RFC publication dates,
Wikimedia revision timestamps, PubMed creation/publication/revision dates, and
EUR-Lex document dates. `knowledge_search` accepts `published_after`,
`published_before`, and `sort` (`relevance`, `newest`, or `oldest`).

## License

Knowledge MCP is released under the [MIT License](LICENSE).
