// Package knowledgeindex owns provider-neutral index layout and schema
// versions. Provider adapters feed documents into these local indexes; callers
// never need to know a provider's raw filenames or locators.
package knowledgeindex

import "github.com/blevesearch/bleve/v2"

const (
	TitleDirectory        = "titles.bleve"
	BodyDirectory         = "bodies.bleve"
	TitleVersion          = 6
	BodyVersion           = 9
	DefaultReadCharacters = 100_000
)

// DocumentCount reads the authoritative number of indexed documents without
// requiring provider raw data to be scanned again.
func DocumentCount(path string) (uint64, error) {
	index, err := bleve.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = index.Close() }()
	return index.DocCount()
}
