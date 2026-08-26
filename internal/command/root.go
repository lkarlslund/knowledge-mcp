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
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/mcpclient"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/mcpserver"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	eurlexprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/eurlex"
	kiwixprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/kiwix"
	ncbiprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/ncbi"
	rfcprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/rfc"
	wikimediaprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/wikimedia"
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
		Short:         "Search locally indexed knowledge datasets through MCP",
		Version:       mcpserver.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	defaultServer := os.Getenv("KNOWLEDGE_DATASET_MCP_SERVER")
	if defaultServer == "" {
		defaultServer = os.Getenv("WIKIPEDIA_MULTISTREAM_MCP_SERVER")
	}
	if defaultServer == "" {
		defaultServer = defaultEndpoint
	}
	root.PersistentFlags().StringVar(&opts.server, "server", defaultServer, "running MCP endpoint")
	root.AddCommand(newServeCommand(), newMCPCommand(opts), newDatasetCommand(opts), newSearchCommand(opts), newReadCommand(opts))
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
			providers, err := provider.NewRegistry(wikimediaprovider.New(wikimedia.NewClient(downloadConnections)), rfcprovider.New(), kiwixprovider.New(), ncbiprovider.New(), eurlexprovider.New())
			if err != nil {
				return err
			}
			backend, err := store.Open(dataDir, providers, store.Options{DownloadWorkers: downloadWorkers, IndexWorkers: indexWorkers})
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			fmt.Fprintf(os.Stderr, "serving MCP at http://%s/mcp using %s\n", listen, dataDir)
			serveErr := mcpserver.ServeHTTP(ctx, listen, backend)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if closeErr := backend.CloseContext(shutdownCtx); closeErr != nil && !errors.Is(closeErr, context.DeadlineExceeded) {
				return closeErr
			} else if errors.Is(closeErr, context.DeadlineExceeded) {
				fmt.Fprintln(os.Stderr, "index workers are still finishing an atomic batch; exiting with the last durable checkpoint")
			}
			return serveErr
		},
	}
	command.Flags().StringVar(&listen, "listen", "127.0.0.1:8765", "loopback listen address")
	command.Flags().StringVar(&dataDir, "data-dir", "./data", "runtime data directory")
	command.Flags().IntVar(&downloadWorkers, "download-workers", 3, "concurrent download/update jobs")
	command.Flags().IntVar(&indexWorkers, "index-workers", 1, "concurrent indexing jobs")
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

func newDatasetCommand(opts *options) *cobra.Command {
	datasets := &cobra.Command{Use: "dataset", Short: "Discover and manage knowledge datasets"}
	var filter string
	var offset, limit int
	var refresh bool
	available := &cobra.Command{
		Use:   "available [filter]",
		Short: "List datasets offered by all providers",
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
		Short: "List local datasets",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.ListLocal()
				return printResult(result, err)
			})
		},
	}
	submit := func(kind string) *cobra.Command {
		var variant string
		command := &cobra.Command{
			Use:   kind + " DATASET",
			Short: "Submit a background dataset " + kind + " and return its job ID",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
					result, err := client.Submit(args[0], variant, kind)
					return printResult(result, err)
				})
			},
		}
		command.Flags().StringVar(&variant, "variant", "", "provider-defined dataset variant")
		return command
	}
	var statusDataset string
	status := &cobra.Command{
		Use:   "status [job-id]",
		Short: "Poll one job status snapshot",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := ""
			if len(args) == 1 {
				id = args[0]
			}
			if id == "" && statusDataset == "" {
				return errors.New("provide a job ID or --dataset")
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Job(id, statusDataset)
				return printResult(result, err)
			})
		},
	}
	status.Flags().StringVar(&statusDataset, "dataset", "", "return the latest job for this dataset")
	var jobAction string
	job := &cobra.Command{
		Use:   "job JOB_ID",
		Short: "Control a background job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if jobAction != "pause" && jobAction != "resume" && jobAction != "cancel" && jobAction != "retry" {
				return errors.New("--action must be pause, resume, cancel, or retry")
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.JobAction(args[0], jobAction)
				return printResult(result, err)
			})
		},
	}
	job.Flags().StringVar(&jobAction, "action", "", "pause, resume, cancel, or retry")
	datasets.AddCommand(available, list, submit("download"), submit("update"), status, job)
	return datasets
}

func newSearchCommand(opts *options) *cobra.Command {
	var offset, limit int
	var mode string
	var includeSecondary, noSnippets bool
	command := &cobra.Command{
		Use:   "search [DATASET] QUERY",
		Short: "Search one dataset or all local datasets",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dataset, query := "", args[0]
			if len(args) == 2 {
				dataset, query = args[0], args[1]
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Search(cmd.Context(), dataset, query, model.SearchOptions{Mode: mode, Offset: offset, Limit: limit, IncludeSecondary: includeSecondary, Snippets: !noSnippets})
				return printResult(result, err)
			})
		},
	}
	command.Flags().IntVar(&offset, "offset", 0, "result offset")
	command.Flags().IntVar(&limit, "limit", 10, "maximum results (up to 50)")
	command.Flags().StringVar(&mode, "mode", "auto", "auto, title, or full_text")
	command.Flags().BoolVar(&includeSecondary, "include-secondary", false, "include provider-defined secondary records")
	command.Flags().BoolVar(&noSnippets, "no-snippets", false, "omit query-centered result snippets")
	return command
}

func newReadCommand(opts *options) *cobra.Command {
	var id, ref string
	var section string
	var offset, maxChars int
	var followRedirects, outline bool
	command := &cobra.Command{
		Use:   "read [DATASET] [TITLE]",
		Short: "Read a document from a local dataset",
		Args:  cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if maxChars < 1 || maxChars > 500_000 {
				return errors.New("max-chars must be between 1 and 500000 for MCP reads")
			}
			if ref != "" {
				if len(args) != 0 || id != "" {
					return errors.New("provide --ref alone")
				}
				return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
					result, err := client.ReadReference(cmd.Context(), ref, model.ReadOptions{Section: section, Offset: offset, MaxChars: maxChars, FollowRedirects: followRedirects, IncludeOutline: outline})
					return printResult(result, err)
				})
			}
			if len(args) == 0 {
				return errors.New("provide --ref, or DATASET with TITLE or --id")
			}
			title := ""
			if len(args) == 2 {
				title = args[1]
			}
			if (title == "") == (id == "") {
				return errors.New("provide exactly one TITLE or --id")
			}
			return withClient(cmd.Context(), opts.server, func(client *mcpclient.Client) error {
				result, err := client.Read(cmd.Context(), args[0], title, id, model.ReadOptions{Section: section, Offset: offset, MaxChars: maxChars, FollowRedirects: followRedirects, IncludeOutline: outline})
				return printResult(result, err)
			})
		},
	}
	command.Flags().StringVar(&ref, "ref", "", "temporary opaque reference returned by search or an embedded link")
	command.Flags().StringVar(&id, "id", "", "opaque document ID returned by search")
	command.Flags().StringVar(&section, "section", "", "read one article section by heading or anchor")
	command.Flags().IntVar(&offset, "offset", 0, "character offset")
	command.Flags().IntVar(&maxChars, "max-chars", 50_000, "maximum returned characters (up to 500000)")
	command.Flags().BoolVar(&followRedirects, "follow-redirects", true, "follow redirects and extract targeted sections")
	command.Flags().BoolVar(&outline, "outline", false, "include the article section outline")
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
