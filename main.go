package main

import (
	"fmt"
	"os"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/command"
)

func main() {
	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
