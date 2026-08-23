package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/mcpserver"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Client struct {
	session *mcp.ClientSession
}

func Connect(ctx context.Context, endpoint string) (*Client, error) {
	transport := &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: &http.Client{Timeout: 2 * time.Minute}, DisableStandaloneSSE: true}
	client := mcp.NewClient(&mcp.Implementation{Name: "wikipedia-multistream-cli", Version: mcpserver.Version}, nil)
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", endpoint, err)
	}
	return &Client{session: session}, nil
}

func (c *Client) Close() error { return c.session.Close() }

func (c *Client) Call(ctx context.Context, name string, input, output any) error {
	result, err := c.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: input})
	if err != nil {
		return err
	}
	if result.IsError {
		if len(result.Content) > 0 {
			return fmt.Errorf("tool %s failed: %v", name, result.Content[0])
		}
		return fmt.Errorf("tool %s failed", name)
	}
	if result.StructuredContent == nil {
		return errors.New("tool returned no structured content")
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, output)
}

func (c *Client) ListAvailable(ctx context.Context, filter string, offset, limit int, refresh bool) (model.AvailableResult, error) {
	var out model.AvailableResult
	err := c.Call(ctx, "wiki_list_available", map[string]any{"filter": filter, "offset": offset, "limit": limit, "refresh": refresh}, &out)
	return out, err
}

func (c *Client) ListLocal() ([]model.LocalWikiSummary, error) {
	return c.ListLocalSummary()
}

func (c *Client) ListLocalSummary() ([]model.LocalWikiSummary, error) {
	var out []model.LocalWikiSummary
	err := c.Call(context.Background(), "wiki_list_local", map[string]any{}, &out)
	return out, err
}

func (c *Client) Submit(wiki, kind string) (model.Job, error) {
	var out model.Job
	err := c.Call(context.Background(), "wiki_"+kind, map[string]any{"wiki": wiki}, &out)
	return out, err
}

func (c *Client) Job(id, wiki string) (model.Job, error) {
	var out model.Job
	err := c.Call(context.Background(), "wiki_job_status", map[string]any{"job_id": id, "wiki": wiki}, &out)
	return out, err
}

func (c *Client) JobAction(id, action string) (model.Job, error) {
	var out model.Job
	err := c.Call(context.Background(), "wiki_job", map[string]any{"job_id": id, "action": action}, &out)
	return out, err
}

func (c *Client) Search(ctx context.Context, wiki, query string, options model.SearchOptions) (model.SearchResult, error) {
	var out model.SearchResult
	err := c.Call(ctx, "wiki_search", map[string]any{"wiki": wiki, "query": query, "offset": options.Offset, "limit": options.Limit, "include_non_articles": options.IncludeNonArticles, "snippets": options.Snippets}, &out)
	return out, err
}

func (c *Client) Read(ctx context.Context, wiki, title string, pageID uint64, options model.ReadOptions) (model.Page, error) {
	var out model.Page
	arguments := map[string]any{"wiki": wiki, "format": options.Format, "section": options.Section, "offset": options.Offset, "max_chars": options.MaxChars, "follow_redirects": options.FollowRedirects, "include_outline": options.IncludeOutline}
	if title != "" {
		arguments["title"] = title
	} else {
		arguments["page_id"] = pageID
	}
	err := c.Call(ctx, "wiki_read", arguments, &out)
	return out, err
}
