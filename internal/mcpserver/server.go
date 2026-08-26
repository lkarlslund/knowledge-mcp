package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	jsonschema "github.com/google/jsonschema-go/jsonschema"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/dashboard"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var Version = "0.1.0"

type Service interface {
	ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error)
	ListLocalSummary() ([]model.LocalDatasetSummary, error)
	OperationalStatus() model.OperationalStatus
	Submit(string, string, string) (model.Job, error)
	Job(string, string) (model.Job, error)
	JobAction(string, string) (model.Job, error)
	Search(context.Context, string, string, model.SearchOptions) (model.SearchResult, error)
	Read(context.Context, string, string, string, model.ReadOptions) (model.Document, error)
	ReadReference(context.Context, string, model.ReadOptions) (model.Document, error)
}

type DashboardService interface {
	Service
	dashboard.Service
}

type listAvailableInput struct {
	Filter  string `json:"filter,omitempty" jsonschema:"case-insensitive substring filter across provider dataset metadata"`
	Offset  int    `json:"offset,omitempty" jsonschema:"zero-based catalog offset"`
	Limit   int    `json:"limit,omitempty" jsonschema:"number of datasets to return; defaults to 20 and is capped at 50"`
	Refresh bool   `json:"refresh,omitempty" jsonschema:"bypass the one-day online catalog cache"`
}

type emptyInput struct{}

type submitInput struct {
	Dataset string `json:"dataset" jsonschema:"installed or available dataset ID such as enwiki or rfc"`
	Variant string `json:"variant,omitempty" jsonschema:"provider-defined variant; omit to use the default"`
}

type jobStatusInput struct {
	JobID   string `json:"job_id,omitempty" jsonschema:"job identifier returned by knowledge_download or knowledge_update"`
	Dataset string `json:"dataset,omitempty" jsonschema:"dataset whose latest job should be returned when job_id is omitted"`
}

type jobInput struct {
	JobID  string `json:"job_id" jsonschema:"job identifier returned by a background operation"`
	Action string `json:"action" jsonschema:"one of pause, resume, cancel, or retry; use knowledge_job_status to inspect a job"`
}

type searchInput struct {
	Dataset          string `json:"dataset,omitempty" jsonschema:"optional installed dataset ID; omit to search all installed search-ready datasets"`
	Query            string `json:"query" jsonschema:"plain text search query"`
	Mode             string `json:"mode,omitempty" jsonschema:"auto (default), title, or full_text"`
	Offset           int    `json:"offset,omitempty" jsonschema:"zero-based result offset"`
	Limit            int    `json:"limit,omitempty" jsonschema:"result count; defaults to 10 and is capped at 50"`
	IncludeSecondary bool   `json:"include_secondary,omitempty" jsonschema:"include provider-defined secondary records; for Wikimedia this includes project, category, draft, talk, and other namespaces"`
	Snippets         *bool  `json:"snippets,omitempty" jsonschema:"include query-centered result passages; defaults to true"`
	PublishedAfter   string `json:"published_after,omitempty" jsonschema:"inclusive publication-date lower bound as YYYY, YYYY-MM, YYYY-MM-DD, or RFC3339; records without a publication date are excluded"`
	PublishedBefore  string `json:"published_before,omitempty" jsonschema:"inclusive publication-date upper bound as YYYY, YYYY-MM, YYYY-MM-DD, or RFC3339; records without a publication date are excluded"`
	Sort             string `json:"sort,omitempty" jsonschema:"relevance (default), newest, or oldest; date sorting uses provider-supplied publication dates"`
}

type readInput struct {
	Ref             string `json:"ref,omitempty" jsonschema:"opaque temporary document reference returned by knowledge_search or an embedded knowledge link"`
	Dataset         string `json:"dataset,omitempty" jsonschema:"installed dataset ID"`
	ID              string `json:"id,omitempty" jsonschema:"opaque document identifier returned by knowledge_search; mutually exclusive with title"`
	Title           string `json:"title,omitempty" jsonschema:"exact document title; mutually exclusive with id"`
	Section         string `json:"section,omitempty" jsonschema:"article section heading or anchor; offsets apply within the selected section"`
	Offset          int    `json:"offset,omitempty" jsonschema:"character offset into rendered content"`
	MaxChars        int    `json:"max_chars,omitempty" jsonschema:"maximum article-content characters; defaults to 50000 and is capped at 500000"`
	FollowRedirects *bool  `json:"follow_redirects,omitempty" jsonschema:"follow redirect chains and extract a targeted section; defaults to true"`
	IncludeOutline  *bool  `json:"include_outline,omitempty" jsonschema:"include up to 200 section headings; defaults to true on the first whole-article chunk and when a requested section is missing"`
}

func New(service Service) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "knowledge-dataset-mcp", Version: Version}, &mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{}})
	listAvailableSchema := mustSchemaFor[listAvailableInput]()
	setIntegerBounds(listAvailableSchema, "offset", 0, nil)
	setIntegerBounds(listAvailableSchema, "limit", 1, intPointer(50))
	jobStatusSchema := mustSchemaFor[jobStatusInput]()
	jobStatusSchema.AnyOf = []*jsonschema.Schema{{Required: []string{"job_id"}}, {Required: []string{"dataset"}}}
	jobSchema := mustSchemaFor[jobInput]()
	jobSchema.Properties["action"].Enum = []any{"pause", "resume", "cancel", "retry"}
	searchSchema := mustSchemaFor[searchInput]()
	searchSchema.Properties["mode"].Enum = []any{"auto", "title", "full_text"}
	setIntegerBounds(searchSchema, "offset", 0, nil)
	setIntegerBounds(searchSchema, "limit", 1, intPointer(50))
	readSchema := mustSchemaFor[readInput]()
	readSchema.OneOf = []*jsonschema.Schema{{Required: []string{"ref"}}, {Required: []string{"dataset", "title"}}, {Required: []string{"dataset", "id"}}}
	setIntegerBounds(readSchema, "offset", 0, nil)
	setIntegerBounds(readSchema, "max_chars", 1, intPointer(500_000))
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_list_available", Description: "Discover downloadable knowledge datasets with provider-defined descriptions, selection metadata, and variants.", InputSchema: listAvailableSchema, Annotations: readOnlyAnnotations(true)}, func(ctx context.Context, _ *mcp.CallToolRequest, in listAvailableInput) (*mcp.CallToolResult, model.AvailableResult, error) {
		out, err := service.ListAvailable(ctx, in.Filter, in.Offset, in.Limit, in.Refresh)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_list_local", Description: "List installed datasets with provider-defined descriptions plus variant, content scope, source, snapshot, and search metadata to help select the right dataset.", Annotations: readOnlyAnnotations(false)}, func(_ context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, []model.LocalDatasetSummary, error) {
		out, err := service.ListLocalSummary()
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_status", Description: "Inspect local worker settings, provider catalog health, and scheduled update-check timing.", Annotations: readOnlyAnnotations(false)}, func(_ context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, model.OperationalStatus, error) {
		return nil, service.OperationalStatus(), nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_download", Description: "Submit a background download for a dataset and return immediately with a job ID; poll knowledge_job_status.", Annotations: changingAnnotations(false, true)}, func(_ context.Context, _ *mcp.CallToolRequest, in submitInput) (*mcp.CallToolResult, model.Job, error) {
		out, err := service.Submit(in.Dataset, in.Variant, "download")
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_update", Description: "Submit a background update for an installed dataset and return immediately with a job ID.", Annotations: changingAnnotations(true, true)}, func(_ context.Context, _ *mcp.CallToolRequest, in submitInput) (*mcp.CallToolResult, model.Job, error) {
		out, err := service.Submit(in.Dataset, in.Variant, "update")
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_job_status", Description: "Poll one background job by job ID or dataset ID.", InputSchema: jobStatusSchema, Annotations: readOnlyAnnotations(false)}, func(_ context.Context, _ *mcp.CallToolRequest, in jobStatusInput) (*mcp.CallToolResult, model.Job, error) {
		if in.JobID == "" && in.Dataset == "" {
			return nil, model.Job{}, errors.New("provide job_id or dataset")
		}
		out, err := service.Job(in.JobID, in.Dataset)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_job", Description: "Control a background job using action pause, resume, cancel, or retry. Use knowledge_job_status to inspect progress.", InputSchema: jobSchema, Annotations: changingAnnotations(true, false)}, func(_ context.Context, _ *mcp.CallToolRequest, in jobInput) (*mcp.CallToolResult, model.Job, error) {
		if in.JobID == "" {
			return nil, model.Job{}, errors.New("provide job_id")
		}
		if in.Action != "pause" && in.Action != "resume" && in.Action != "cancel" && in.Action != "retry" {
			return nil, model.Job{}, errors.New("action must be pause, resume, cancel, or retry; use knowledge_job_status to inspect a job")
		}
		out, err := service.JobAction(in.JobID, in.Action)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_search", Description: "Search local indexes. Omit dataset to search all installed search-ready datasets concurrently, or provide it for exact scope. Exact identifiers such as RFC 9110 receive strong ranking. Follow results with knowledge_read using only ref.", InputSchema: searchSchema, Annotations: readOnlyAnnotations(false)}, func(ctx context.Context, _ *mcp.CallToolRequest, in searchInput) (*mcp.CallToolResult, model.SearchResult, error) {
		if strings.TrimSpace(in.Query) == "" {
			return nil, model.SearchResult{}, errors.New("provide a non-empty query")
		}
		snippets := in.Snippets == nil || *in.Snippets
		publishedAfter, err := parseSearchDate(in.PublishedAfter, false)
		if err != nil {
			return nil, model.SearchResult{}, fmt.Errorf("published_after: %w", err)
		}
		publishedBefore, err := parseSearchDate(in.PublishedBefore, true)
		if err != nil {
			return nil, model.SearchResult{}, fmt.Errorf("published_before: %w", err)
		}
		out, err := service.Search(ctx, in.Dataset, in.Query, model.SearchOptions{Mode: in.Mode, Offset: in.Offset, Limit: in.Limit, IncludeSecondary: in.IncludeSecondary, Snippets: snippets, PublishedAfter: publishedAfter, PublishedBefore: publishedBefore, Sort: in.Sort})
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "knowledge_read", Description: "Read a Markdown document using the opaque temporary ref returned by knowledge_search or an embedded link. Legacy dataset plus id/title arguments remain supported. Continue large documents with next_offset.", InputSchema: readSchema, Annotations: readOnlyAnnotations(false)}, func(ctx context.Context, _ *mcp.CallToolRequest, in readInput) (*mcp.CallToolResult, model.Document, error) {
		followRedirects := in.FollowRedirects == nil || *in.FollowRedirects
		maxChars := in.MaxChars
		if maxChars <= 0 {
			maxChars = 50_000
		} else if maxChars > 500_000 {
			maxChars = 500_000
		}
		includeOutline := in.IncludeOutline != nil && *in.IncludeOutline || in.IncludeOutline == nil && in.Offset == 0 && in.Section == ""
		options := model.ReadOptions{Format: "markdown", Section: in.Section, Offset: in.Offset, MaxChars: maxChars, FollowRedirects: followRedirects, IncludeOutline: includeOutline, AlignBoundaries: true, ReferenceBudgetChars: 10_000, ReferenceMaxChars: 4_000}
		var out model.Document
		var err error
		if in.Ref != "" {
			if in.Dataset != "" || in.Title != "" || in.ID != "" {
				return nil, model.Document{}, errors.New("provide ref alone, or dataset with exactly one of title or id")
			}
			out, err = service.ReadReference(ctx, in.Ref, options)
		} else {
			if in.Dataset == "" || (in.Title == "") == (in.ID == "") {
				return nil, model.Document{}, errors.New("provide ref alone, or dataset with exactly one of title or id")
			}
			out, err = service.Read(ctx, in.Dataset, in.Title, in.ID, options)
		}
		return nil, out, err
	})
	return server
}

func parseSearchDate(value string, upper bool) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	for _, candidate := range []struct {
		layout string
		span   time.Duration
	}{{time.RFC3339, 0}, {"2006-01-02", 24 * time.Hour}, {"2006-01", 0}, {"2006", 0}} {
		parsed, err := time.Parse(candidate.layout, value)
		if err != nil {
			continue
		}
		if upper {
			switch candidate.layout {
			case "2006":
				parsed = parsed.AddDate(1, 0, 0).Add(-time.Nanosecond)
			case "2006-01":
				parsed = parsed.AddDate(0, 1, 0).Add(-time.Nanosecond)
			case "2006-01-02":
				parsed = parsed.Add(candidate.span).Add(-time.Nanosecond)
			}
		}
		parsed = parsed.UTC()
		return &parsed, nil
	}
	return nil, errors.New("use YYYY, YYYY-MM, YYYY-MM-DD, or RFC3339")
}

func mustSchemaFor[T any]() *jsonschema.Schema {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		panic(err)
	}
	return schema
}

func setIntegerBounds(schema *jsonschema.Schema, property string, minimum int, maximum *int) {
	field := schema.Properties[property]
	minValue := float64(minimum)
	field.Minimum = &minValue
	if maximum != nil {
		maxValue := float64(*maximum)
		field.Maximum = &maxValue
	}
}

func intPointer(value int) *int { return &value }

func readOnlyAnnotations(openWorld bool) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &openWorld}
}

func changingAnnotations(destructive, openWorld bool) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{DestructiveHint: &destructive, OpenWorldHint: &openWorld}
}

func ServeHTTP(ctx context.Context, listen string, service DashboardService) error {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", listen)
	if err != nil {
		return err
	}
	handler := httpHandler(service)
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	registerPProf(mux)
	mux.Handle("/", dashboard.Handler(service))
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 0, IdleTimeout: 2 * time.Minute}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve MCP HTTP: %w", err)
	}
	return nil
}

func registerPProf(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}

func httpHandler(service Service) http.Handler {
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return New(service) }, &mcp.StreamableHTTPOptions{
		Stateless:                    true,
		JSONResponse:                 true,
		MaxRequestBodyBytes:          1 << 20,
		PropagateRequestCancellation: true,
	})
	return http.NewCrossOriginProtection().Handler(handler)
}

func ServeStdio(ctx context.Context, service Service) error {
	return New(service).Run(ctx, &mcp.StdioTransport{})
}
