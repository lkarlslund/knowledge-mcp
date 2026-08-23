package model

import "time"

const (
	StateQueued        = "queued"
	StateDiscovering   = "discovering"
	StateDownloading   = "downloading"
	StateVerifying     = "verifying"
	StateDownloaded    = "downloaded"
	StatePaused        = "paused"
	StateCanceled      = "canceled"
	StateTitleIndexing = "title_indexing"
	StateTitleReady    = "title_ready"
	StateBodyIndexing  = "body_indexing"
	StateReady         = "ready"
	StateUpToDate      = "up_to_date"
	StateFailed        = "failed"
)

type OnlineWiki struct {
	Name            string       `json:"name"`
	DisplayName     string       `json:"display_name,omitempty"`
	Project         string       `json:"project,omitempty"`
	ContentType     string       `json:"content_type,omitempty"`
	Language        WikiLanguage `json:"language,omitempty"`
	OnlineSourceURL string       `json:"online_source_url,omitempty"`
	DumpDate        string       `json:"dump_date"`
	Closed          bool         `json:"closed"`
	Available       bool         `json:"multistream_available"`
	DumpSize        int64        `json:"dump_size,omitempty"`
	IndexSize       int64        `json:"index_size,omitempty"`
	DumpSHA1        string       `json:"dump_sha1,omitempty"`
	IndexSHA1       string       `json:"index_sha1,omitempty"`
	Fingerprint     string       `json:"fingerprint,omitempty"`
	PartCount       int          `json:"part_count"`
	Installed       bool         `json:"installed"`
	UpdateAvailable bool         `json:"update_available"`
}

type AvailableResult struct {
	Wikis      []OnlineWiki `json:"wikis"`
	Offset     int          `json:"offset"`
	NextOffset int          `json:"next_offset,omitempty"`
	Total      int          `json:"total"`
}

type FileMetadata struct {
	URL  string `json:"url"`
	Size int64  `json:"size"`
	SHA1 string `json:"sha1"`
}

type DumpMetadata struct {
	Wiki        string     `json:"wiki"`
	DumpDate    string     `json:"dump_date"`
	Parts       []DumpPart `json:"parts"`
	Fingerprint string     `json:"fingerprint"`
}

type DumpPart struct {
	Key   string       `json:"key"`
	Dump  FileMetadata `json:"dump"`
	Index FileMetadata `json:"index"`
}

type WikiLanguage struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	LocalName string `json:"local_name"`
	Direction string `json:"direction,omitempty"`
}

type WikiSiteMetadata struct {
	Name              string       `json:"name"`
	Project           string       `json:"project"`
	ContentType       string       `json:"content_type"`
	Language          WikiLanguage `json:"language"`
	OnlineSourceURL   string       `json:"online_source_url"`
	ContentArticles   uint64       `json:"content_articles"`
	Closed            bool         `json:"closed"`
	License           string       `json:"license,omitempty"`
	LicenseURL        string       `json:"license_url,omitempty"`
	MetadataUpdatedAt time.Time    `json:"metadata_updated_at,omitempty"`
}

type Manifest struct {
	Wiki              string           `json:"wiki"`
	DumpDate          string           `json:"dump_date"`
	DumpSHA1          string           `json:"dump_sha1"`
	IndexSHA1         string           `json:"index_sha1"`
	Fingerprint       string           `json:"fingerprint,omitempty"`
	PartCount         int              `json:"part_count"`
	DumpSize          int64            `json:"dump_size"`
	IndexSize         int64            `json:"index_size"`
	PageCount         uint64           `json:"page_count"`
	TitleReady        bool             `json:"title_ready"`
	BodyReady         bool             `json:"body_ready"`
	TitleIndexVersion int              `json:"title_index_version,omitempty"`
	BodyIndexVersion  int              `json:"body_index_version,omitempty"`
	PublishedAt       time.Time        `json:"published_at"`
	Site              WikiSiteMetadata `json:"site"`
}

type LocalWiki struct {
	Manifest
	State                 string `json:"state"`
	SearchMode            string `json:"search_mode"`
	DiskBytes             int64  `json:"disk_bytes"`
	CompressedDumpBytes   int64  `json:"compressed_dump_bytes"`
	MultistreamIndexBytes int64  `json:"multistream_index_bytes"`
	TitleIndexBytes       int64  `json:"title_index_bytes"`
	BodyIndexBytes        int64  `json:"body_index_bytes"`
	OtherBytes            int64  `json:"other_bytes"`
	ActiveJob             string `json:"active_job_id,omitempty"`
}

type LocalWikiSummary struct {
	Wiki            string       `json:"wiki"`
	Name            string       `json:"name"`
	Project         string       `json:"project"`
	ContentType     string       `json:"content_type"`
	Language        WikiLanguage `json:"language"`
	OnlineSourceURL string       `json:"online_source_url"`
	ContentArticles uint64       `json:"content_articles"`
	IndexedPages    uint64       `json:"indexed_pages"`
	DumpDate        string       `json:"dump_date"`
	SearchMode      string       `json:"search_mode"`
	Closed          bool         `json:"closed"`
}

type Job struct {
	ID              string    `json:"id"`
	Wiki            string    `json:"wiki"`
	Kind            string    `json:"kind"`
	State           string    `json:"state"`
	Phase           string    `json:"phase"`
	Completed       int64     `json:"completed"`
	Total           int64     `json:"total"`
	Units           string    `json:"units,omitempty"`
	Rate            float64   `json:"rate,omitempty"`
	ProgressPercent float64   `json:"progress_percent,omitempty"`
	ProgressApprox  bool      `json:"progress_approximate,omitempty"`
	Message         string    `json:"message,omitempty"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	TitleAvailable  bool      `json:"title_available"`
	SourceJobID     string    `json:"source_job_id,omitempty"`
}

type SearchHit struct {
	PageID  uint64  `json:"page_id"`
	Title   string  `json:"title"`
	Score   float64 `json:"score"`
	Snippet string  `json:"snippet,omitempty"`
}

type SearchResult struct {
	Wiki       string      `json:"wiki"`
	SearchMode string      `json:"search_mode"`
	Total      uint64      `json:"total"`
	Offset     int         `json:"offset"`
	Hits       []SearchHit `json:"hits"`
}

type Page struct {
	Wiki            string          `json:"wiki"`
	PageID          uint64          `json:"page_id"`
	RevisionID      uint64          `json:"revision_id"`
	Title           string          `json:"title"`
	Timestamp       string          `json:"timestamp,omitempty"`
	PageURL         string          `json:"page_url,omitempty"`
	RequestedTitle  string          `json:"requested_title,omitempty"`
	RequestedPageID uint64          `json:"requested_page_id,omitempty"`
	Redirected      bool            `json:"redirected,omitempty"`
	RedirectChain   []RedirectHop   `json:"redirect_chain,omitempty"`
	Section         string          `json:"section,omitempty"`
	SectionFound    *bool           `json:"section_found,omitempty"`
	Format          string          `json:"format"`
	Content         string          `json:"content"`
	References      []PageReference `json:"references,omitempty"`
	Truncated       bool            `json:"truncated"`
	NextOffset      int             `json:"next_offset,omitempty"`
}

type RedirectHop struct {
	FromTitle  string `json:"from_title"`
	FromPageID uint64 `json:"from_page_id"`
	ToTitle    string `json:"to_title"`
	Fragment   string `json:"fragment,omitempty"`
}

type PageReference struct {
	ID      int    `json:"id"`
	Name    string `json:"name,omitempty"`
	Content string `json:"content"`
}
