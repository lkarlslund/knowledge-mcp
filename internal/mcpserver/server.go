package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/dashboard"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var Version = "0.1.0"

type Service interface {
	ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error)
	ListLocalSummary() ([]model.LocalWikiSummary, error)
	Submit(string, string) (model.Job, error)
	Job(string, string) (model.Job, error)
	JobAction(string, string) (model.Job, error)
	Search(context.Context, string, string, int, int) (model.SearchResult, error)
	Read(context.Context, string, string, uint64, string, int, int) (model.Page, error)
}

type DashboardService interface {
	Service
	dashboard.Service
}

type listAvailableInput struct {
	Filter  string `json:"filter,omitempty" jsonschema:"case-insensitive substring filter for Wikimedia database names"`
	Offset  int    `json:"offset,omitempty" jsonschema:"zero-based catalog offset"`
	Limit   int    `json:"limit,omitempty" jsonschema:"number of wikis to return; defaults to 20 and is capped at 50"`
	Refresh bool   `json:"refresh,omitempty" jsonschema:"bypass the one-hour online catalog cache"`
}

type emptyInput struct{}

type submitInput struct {
	Wiki string `json:"wiki" jsonschema:"Wikimedia database name such as enwiki or dawiki"`
}

type jobStatusInput struct {
	JobID string `json:"job_id,omitempty" jsonschema:"job identifier returned by wiki_download or wiki_update"`
	Wiki  string `json:"wiki,omitempty" jsonschema:"wiki whose latest job should be returned when job_id is omitted"`
}

type jobInput struct {
	JobID  string `json:"job_id" jsonschema:"job identifier returned by a background operation"`
	Action string `json:"action" jsonschema:"one of status, pause, resume, cancel, or retry"`
}

type searchInput struct {
	Wiki   string `json:"wiki" jsonschema:"installed Wikimedia database name"`
	Query  string `json:"query" jsonschema:"plain text search query"`
	Offset int    `json:"offset,omitempty" jsonschema:"zero-based result offset"`
	Limit  int    `json:"limit,omitempty" jsonschema:"result count; defaults to 10 and is capped at 50"`
}

type readInput struct {
	Wiki     string `json:"wiki" jsonschema:"installed Wikimedia database name"`
	Title    string `json:"title,omitempty" jsonschema:"exact page title; mutually exclusive with page_id"`
	PageID   uint64 `json:"page_id,omitempty" jsonschema:"numeric page identifier; mutually exclusive with title"`
	Format   string `json:"format,omitempty" jsonschema:"text (default) or wikitext"`
	Offset   int    `json:"offset,omitempty" jsonschema:"character offset into rendered content"`
	MaxChars int    `json:"max_chars,omitempty" jsonschema:"maximum characters; defaults to 20000 and is capped at 100000"`
}

func New(service Service) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "wikipedia-multistream-mcp", Version: Version}, &mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{}})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_list_available", Description: "List and filter Wikimedia wikis with completed online multistream article dumps."}, func(ctx context.Context, _ *mcp.CallToolRequest, in listAvailableInput) (*mcp.CallToolResult, model.AvailableResult, error) {
		out, err := service.ListAvailable(ctx, in.Filter, in.Offset, in.Limit, in.Refresh)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_list_local", Description: "List local wikis with human-readable project, language, content scope, online source, content size, snapshot date, and search capability metadata."}, func(_ context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, []model.LocalWikiSummary, error) {
		out, err := service.ListLocalSummary()
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_download", Description: "Submit a background download for a new wiki and return immediately with a job ID."}, func(_ context.Context, _ *mcp.CallToolRequest, in submitInput) (*mcp.CallToolResult, model.Job, error) {
		out, err := service.Submit(in.Wiki, "download")
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_update", Description: "Submit a background update for an installed wiki and return immediately with a job ID."}, func(_ context.Context, _ *mcp.CallToolRequest, in submitInput) (*mcp.CallToolResult, model.Job, error) {
		out, err := service.Submit(in.Wiki, "update")
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_job_status", Description: "Poll one download/update job by job ID or wiki name."}, func(_ context.Context, _ *mcp.CallToolRequest, in jobStatusInput) (*mcp.CallToolResult, model.Job, error) {
		if in.JobID == "" && in.Wiki == "" {
			return nil, model.Job{}, errors.New("provide job_id or wiki")
		}
		out, err := service.Job(in.JobID, in.Wiki)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_job", Description: "Inspect or control a background job using action status, pause, resume, cancel, or retry."}, func(_ context.Context, _ *mcp.CallToolRequest, in jobInput) (*mcp.CallToolResult, model.Job, error) {
		if in.JobID == "" {
			return nil, model.Job{}, errors.New("provide job_id")
		}
		out, err := service.JobAction(in.JobID, in.Action)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_search", Description: "Search an installed wiki. Uses title search as soon as published and full-text search when body indexing finishes."}, func(ctx context.Context, _ *mcp.CallToolRequest, in searchInput) (*mcp.CallToolResult, model.SearchResult, error) {
		out, err := service.Search(ctx, in.Wiki, in.Query, in.Offset, in.Limit)
		return nil, out, err
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wiki_read", Description: "Read an installed wiki page by exact title or page ID as plain text or raw wikitext."}, func(ctx context.Context, _ *mcp.CallToolRequest, in readInput) (*mcp.CallToolResult, model.Page, error) {
		out, err := service.Read(ctx, in.Wiki, in.Title, in.PageID, in.Format, in.Offset, in.MaxChars)
		return nil, out, err
	})
	return server
}

func ServeHTTP(ctx context.Context, listen string, service DashboardService) error {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", listen)
	if err != nil {
		return err
	}
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return New(service) }, &mcp.StreamableHTTPOptions{JSONResponse: true})
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
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

func ServeStdio(ctx context.Context, service Service) error {
	return New(service).Run(ctx, &mcp.StdioTransport{})
}
