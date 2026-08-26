package store

import (
	"reflect"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

func TestDatasetLanguages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dataset model.AvailableDataset
		want    []model.Language
	}{
		{
			name:    "named language",
			dataset: model.AvailableDataset{Language: model.Language{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"}},
			want:    []model.Language{{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"}},
		},
		{
			name:    "provider languages",
			dataset: model.AvailableDataset{Language: model.Language{Code: "mul"}, Languages: []model.Language{{Code: "en", Name: "English"}, {Code: "fr", Name: "French"}}},
			want:    []model.Language{{Code: "en", Name: "English"}, {Code: "fr", Name: "French"}},
		},
		{name: "empty", want: []model.Language{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := datasetLanguages(test.dataset); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("datasetLanguages(%#v) = %#v, want %#v", test.dataset, got, test.want)
			}
		})
	}
}

func TestDatasetHasLanguage(t *testing.T) {
	t.Parallel()
	dataset := model.AvailableDataset{Languages: []model.Language{{Code: "en"}, {Code: "fr"}, {Code: "de"}}}
	if !datasetHasLanguage(dataset, "fr") {
		t.Fatal("multilingual dataset should match a component language")
	}
	if datasetHasLanguage(dataset, "eng") {
		t.Fatal("language matching should use exact catalog codes")
	}
}
