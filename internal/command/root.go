package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/mcpclient"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/mcpserver"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/store"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikimedia"
	"github.com/spf13/cobra"
)

const defaultEndpoint = "http://127.0.0.1:8765/mcp"

type options struct {
	server string
}

func Execute() error {
	opts := &options{}
	root := &cobra.Command{
		Use:           "wikipedia-multistream-mcp",
		Short:         "Search local Wikimedia multistream dumps through MCP",
		Version:       mcpserver.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	defaultServer := os.Getenv("WIKIPEDIA_MULTISTREAM_MCP_SERVER")
	if defaultServer == "" {
		defaultServer = defaultEndpoint
	}
	root.PersistentFlags().StringVar(&opts.server, "server", defaultServer, "running MCP endpoint")
	root.AddCommand(newServeCommand(), newMCPCommand(opts), newWikiCommand(opts), newSearchCommand(opts), newReadCommand(opts))
	return root.Execute()
}

func newServeCommand() *cobra.Command {
	var listen, dataDir string
	var downloadWorkers, indexWorkers, downloadConnections int
	command := &cobra.Command{
		Use:   "serve",
		Short: "Run the persistent Streamable HTTP MCP backend",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireLoopback(listen); err != nil {
				return err
			}
			backend, err := store.Open(dataDir, wikimedia.NewClient(downloadConnections), store.Options{DownloadWorkers: downloadWorkers, IndexWorkers: indexWorkers})
			if err != nil {
				return err
			}
			defer backend.Close()
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			fmt.Fprintf(os.Stderr, "serving MCP at http://%s/mcp using %s\n", listen, dataDir)
			return mcpserver.ServeHTTP(ctx, listen, backend)
		},
	}
	command.Flags().StringVar(&listen, "listen", "127.0.0.1:8765", "loopback listen address")
	command.Flags().StringVar(&dataDir, "data-dir", "./data", "runtime data directory")
	command.Flags().IntVar(&downloadWorkers, "download-workers", 3, "concurrent download/update jobs")
	command.Flags().IntVar(&indexWorkers, "index-workers", 2, "concurrent indexing jobs")
	command.Flags().IntVar(&downloadConnections, "download-connections", 3, "parallel HTTP ranges shared by downloads")
	return command
}

func newMCPCommand(opts *options) *cobra.Command {
	mcpCommand := &cobra.Command{Use: "mcp", Short: "MCP transport helpers"}
	mcpCommand.AddCommand(&cobra.Command{
		Use:   "stdio",
		Short: "Bridge stdio MCP clients to the running backend",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := mcpclient.Connect(cmd.Context(), opts.server)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			return mcpserver.ServeStdio(cmd.Context(), client)
		},
	})
	return mcpCommand
}

func newWikiCommand(opts *options) *cobra.Command {
	wiki := &cobra.Command{Use: "wiki", Short: "Discover and manage local wikis"}
	var filter string
	var offset, limit int
	var refresh bool
	available := &cobra.Command{
		Use:   "available [filter]",
		Short: "List online multistream dumps",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				filter = args[0]
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.ListAvailable(cmd.Context(), filter, offset, limit, refresh)
				return printResult(result, err)
			})
		},
	}
	available.Flags().IntVar(&offset, "offset", 0, "catalog offset")
	available.Flags().IntVar(&limit, "limit", 20, "maximum results (up to 50)")
	available.Flags().BoolVar(&refresh, "refresh", false, "refresh online catalog metadata")
	list := &cobra.Command{
		Use:   "list",
		Short: "List local wikis",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.ListLocal()
				return printResult(result, err)
			})
		},
	}
	submit := func(kind string) *cobra.Command {
		return &cobra.Command{
			Use:   kind + " WIKI",
			Short: "Submit a background wiki " + kind + " and return its job ID",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
					result, err := client.Submit(args[0], kind)
					return printResult(result, err)
				})
			},
		}
	}
	var statusWiki string
	status := &cobra.Command{
		Use:   "status [job-id]",
		Short: "Poll one job status snapshot",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := ""
			if len(args) == 1 {
				id = args[0]
			}
			if id == "" && statusWiki == "" {
				return errors.New("provide a job ID or --wiki")
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Job(id, statusWiki)
				return printResult(result, err)
			})
		},
	}
	status.Flags().StringVar(&statusWiki, "wiki", "", "return the latest job for this wiki")
	var jobAction string
	job := &cobra.Command{
		Use:   "job JOB_ID",
		Short: "Inspect or control a background job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.JobAction(args[0], jobAction)
				return printResult(result, err)
			})
		},
	}
	job.Flags().StringVar(&jobAction, "action", "status", "status, pause, resume, cancel, or retry")
	wiki.AddCommand(available, list, submit("download"), submit("update"), status, job)
	return wiki
}

func newSearchCommand(opts *options) *cobra.Command {
	var offset, limit int
	command := &cobra.Command{
		Use:   "search WIKI QUERY",
		Short: "Search titles or full text in a local wiki",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Search(cmd.Context(), args[0], args[1], offset, limit)
				return printResult(result, err)
			})
		},
	}
	command.Flags().IntVar(&offset, "offset", 0, "result offset")
	command.Flags().IntVar(&limit, "limit", 10, "maximum results (up to 50)")
	return command
}

func newReadCommand(opts *options) *cobra.Command {
	var pageID uint64
	var format string
	var offset, maxChars int
	command := &cobra.Command{
		Use:   "read WIKI [TITLE]",
		Short: "Read a local wiki page",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			title := ""
			if len(args) == 2 {
				title = args[1]
			}
			if title == "" && pageID == 0 || title != "" && pageID != 0 {
				return errors.New("provide exactly one TITLE or --page-id")
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Read(cmd.Context(), args[0], title, pageID, format, offset, maxChars)
				return printResult(result, err)
			})
		},
	}
	command.Flags().Uint64Var(&pageID, "page-id", 0, "read by numeric page ID")
	command.Flags().StringVar(&format, "format", "text", "text or wikitext")
	command.Flags().IntVar(&offset, "offset", 0, "character offset")
	command.Flags().IntVar(&maxChars, "max-chars", 20_000, "maximum returned characters")
	return command
}

func withClient(ctx context.Context, endpoint string, fn func(*mcpclient.Client) error) error {
	client, err := mcpclient.Connect(ctx, endpoint)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	return fn(client)
}

func printResult(value any, err error) error {
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func requireLoopback(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("invalid listen port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("serve only accepts an explicit loopback IP address")
	}
	return nil
}
