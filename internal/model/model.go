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

type AvailableDataset struct {
	Provider             string         `json:"provider"`
	Variant              string         `json:"variant,omitempty"`
	Variants             []Variant      `json:"variants,omitempty"`
	ID                   string         `json:"id"`
	DisplayName          string         `json:"display_name,omitempty"`
	Description          string         `json:"description,omitempty"`
	Project              string         `json:"project,omitempty"`
	ContentType          string         `json:"content_type,omitempty"`
	Profile              DatasetProfile `json:"profile,omitempty"`
	Language             Language       `json:"language,omitempty"`
	OnlineSourceURL      string         `json:"online_source_url,omitempty"`
	ReleaseDate          string         `json:"release_date"`
	Closed               bool           `json:"closed"`
	Available            bool           `json:"available"`
	RawSize              int64          `json:"raw_size,omitempty"`
	ProviderMetadataSize int64          `json:"provider_metadata_size,omitempty"`
	RawHash              string         `json:"raw_hash,omitempty"`
	ProviderMetadataHash string         `json:"provider_metadata_hash,omitempty"`
	Fingerprint          string         `json:"fingerprint,omitempty"`
	PartCount            int            `json:"part_count"`
	Installed            bool           `json:"installed"`
	UpdateAvailable      bool           `json:"update_available"`
}

// Variant is a provider-defined representation of an installable collection.
// The collection name and variant ID together identify the raw source, while
// document IDs remain independent of either its URL or local storage path.
type Variant struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Format      string `json:"format,omitempty"`
}

type AvailableResult struct {
	Datasets   []AvailableDataset `json:"datasets"`
	Offset     int                `json:"offset"`
	NextOffset int                `json:"next_offset,omitempty"`
	Total      int                `json:"total"`
}

type FileMetadata struct {
	URL  string `json:"url"`
	Size int64  `json:"size"`
	SHA1 string `json:"sha1"`
}

type ReleaseMetadata struct {
	Dataset     string        `json:"dataset"`
	ReleaseDate string        `json:"release_date"`
	Parts       []ReleasePart `json:"parts"`
	Fingerprint string        `json:"fingerprint"`
}

type ReleasePart struct {
	Key              string       `json:"key"`
	Raw              FileMetadata `json:"raw"`
	ProviderMetadata FileMetadata `json:"provider_metadata"`
}

type Language struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	LocalName string `json:"local_name"`
	Direction string `json:"direction,omitempty"`
}

type DatasetMetadata struct {
	Name              string         `json:"name"`
	Description       string         `json:"description,omitempty"`
	Project           string         `json:"project"`
	ContentType       string         `json:"content_type"`
	Profile           DatasetProfile `json:"profile,omitempty"`
	Language          Language       `json:"language"`
	OnlineSourceURL   string         `json:"online_source_url"`
	SourceDocuments   uint64         `json:"source_documents"`
	Closed            bool           `json:"closed"`
	License           string         `json:"license,omitempty"`
	LicenseURL        string         `json:"license_url,omitempty"`
	MetadataUpdatedAt time.Time      `json:"metadata_updated_at,omitempty"`
}

// DatasetProfile is provider-owned selection metadata. It describes what the
// source contains rather than local implementation or index details.
type DatasetProfile struct {
	Topics          []string `json:"topics,omitempty"`
	GeographicScope []string `json:"geographic_scope,omitempty"`
	TimeCoverage    string   `json:"time_coverage,omitempty"`
	DocumentTypes   []string `json:"document_types,omitempty"`
	UpdateCadence   string   `json:"update_cadence,omitempty"`
	CoverageNotes   string   `json:"coverage_notes,omitempty"`
	SourceFeatures  []string `json:"source_features,omitempty"`
}

type Manifest struct {
	Provider             string          `json:"provider,omitempty"`
	Variant              string          `json:"variant,omitempty"`
	Dataset              string          `json:"dataset"`
	ReleaseDate          string          `json:"release_date"`
	RawHash              string          `json:"raw_hash"`
	ProviderMetadataHash string          `json:"provider_metadata_hash"`
	Fingerprint          string          `json:"fingerprint,omitempty"`
	PartCount            int             `json:"part_count"`
	RawSize              int64           `json:"raw_size"`
	ProviderMetadataSize int64           `json:"provider_metadata_size"`
	DocumentCount        uint64          `json:"document_count"`
	TitleReady           bool            `json:"title_ready"`
	BodyReady            bool            `json:"body_ready"`
	TitleIndexVersion    int             `json:"title_index_version,omitempty"`
	BodyIndexVersion     int             `json:"body_index_version,omitempty"`
	PublishedAt          time.Time       `json:"published_at"`
	Site                 DatasetMetadata `json:"site"`
}

type LocalDataset struct {
	Manifest
	State                 string `json:"state"`
	SearchMode            string `json:"search_mode"`
	DiskBytes             int64  `json:"disk_bytes"`
	RawBytes              int64  `json:"raw_bytes"`
	ProviderMetadataBytes int64  `json:"provider_metadata_bytes"`
	TitleIndexBytes       int64  `json:"title_index_bytes"`
	BodyIndexBytes        int64  `json:"body_index_bytes"`
	OtherBytes            int64  `json:"other_bytes"`
	ActiveJob             string `json:"active_job_id,omitempty"`
}

type LocalDatasetSummary struct {
	Provider         string         `json:"provider"`
	Variant          string         `json:"variant,omitempty"`
	Dataset          string         `json:"dataset"`
	Name             string         `json:"name"`
	Description      string         `json:"description,omitempty"`
	Project          string         `json:"project"`
	ContentType      string         `json:"content_type"`
	Profile          DatasetProfile `json:"profile,omitempty"`
	Language         Language       `json:"language"`
	OnlineSourceURL  string         `json:"online_source_url"`
	SourceDocuments  uint64         `json:"source_documents"`
	IndexedDocuments uint64         `json:"indexed_documents"`
	ReleaseDate      string         `json:"release_date"`
	SearchMode       string         `json:"search_mode"`
	Closed           bool           `json:"closed"`
}

type Job struct {
	ID              string    `json:"id"`
	Dataset         string    `json:"dataset"`
	Provider        string    `json:"provider"`
	Variant         string    `json:"variant,omitempty"`
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
	ID           string   `json:"id"`
	NumericID    uint64   `json:"-"`
	Title        string   `json:"title"`
	MatchedTitle string   `json:"matched_title,omitempty"`
	URL          string   `json:"url,omitempty"`
	Namespace    int      `json:"namespace"`
	Score        float64  `json:"score"`
	MatchMode    string   `json:"match_mode"`
	Snippet      string   `json:"snippet,omitempty"`
	Identifiers  []string `json:"identifiers,omitempty"`
	Status       string   `json:"status,omitempty"`
}

type SearchResult struct {
	Dataset              string      `json:"dataset"`
	Query                string      `json:"query"`
	SearchMode           string      `json:"search_mode"`
	Total                uint64      `json:"total"`
	Offset               int         `json:"offset"`
	NextOffset           int         `json:"next_offset,omitempty"`
	PrimaryFilterApplied bool        `json:"primary_filter_applied"`
	SnippetsAvailable    bool        `json:"snippets_available"`
	SnippetsComplete     bool        `json:"snippets_complete"`
	SnippetErrors        int         `json:"snippet_errors,omitempty"`
	Hits                 []SearchHit `json:"hits"`
}

type SearchOptions struct {
	Mode             string
	Offset           int
	Limit            int
	IncludeSecondary bool
	Snippets         bool
}

type Document struct {
	ID                  string                 `json:"id"`
	Dataset             string                 `json:"dataset"`
	NumericID           uint64                 `json:"-"`
	RevisionID          uint64                 `json:"revision_id"`
	Title               string                 `json:"title"`
	Timestamp           string                 `json:"timestamp,omitempty"`
	URL                 string                 `json:"url,omitempty"`
	RequestedTitle      string                 `json:"requested_title,omitempty"`
	RequestedNumericID  uint64                 `json:"-"`
	Redirected          bool                   `json:"redirected,omitempty"`
	RedirectChain       []RedirectHop          `json:"redirect_chain,omitempty"`
	Section             string                 `json:"section,omitempty"`
	SectionFound        *bool                  `json:"section_found,omitempty"`
	Format              string                 `json:"format"`
	Content             string                 `json:"content"`
	Offset              int                    `json:"offset"`
	ReturnedChars       int                    `json:"returned_chars"`
	TotalChars          int                    `json:"total_chars"`
	Sections            []DocumentSection      `json:"sections,omitempty"`
	OutlineTruncated    bool                   `json:"outline_truncated,omitempty"`
	References          []DocumentReference    `json:"references,omitempty"`
	Relationships       []DocumentRelationship `json:"relationships,omitempty"`
	ReferencesTruncated bool                   `json:"references_truncated,omitempty"`
	OmittedReferenceIDs []int                  `json:"omitted_reference_ids,omitempty"`
	Truncated           bool                   `json:"truncated"`
	NextOffset          int                    `json:"next_offset,omitempty"`
}

type DocumentRelationship struct {
	Type  string `json:"type"`
	ID    string `json:"id,omitempty"`
	Label string `json:"label"`
	URL   string `json:"url,omitempty"`
}

type RedirectHop struct {
	FromTitle     string `json:"from_title"`
	FromID        string `json:"from_id,omitempty"`
	FromNumericID uint64 `json:"-"`
	ToTitle       string `json:"to_title"`
	Fragment      string `json:"fragment,omitempty"`
}

type DocumentReference struct {
	ID            int    `json:"id"`
	Name          string `json:"name,omitempty"`
	Content       string `json:"content"`
	Truncated     bool   `json:"truncated,omitempty"`
	OriginalChars int    `json:"original_chars,omitempty"`
}

type DocumentSection struct {
	Heading string `json:"heading"`
	Anchor  string `json:"anchor"`
	Level   int    `json:"level"`
}

type ReadOptions struct {
	Format               string
	LinkDataset          string
	Section              string
	Offset               int
	MaxChars             int
	FollowRedirects      bool
	IncludeOutline       bool
	AlignBoundaries      bool
	ReferenceBudgetChars int
	ReferenceMaxChars    int
}
