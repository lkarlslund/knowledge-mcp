package store

import (
	"reflect"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

func TestDatasetLanguages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		language model.Language
		want     []model.Language
	}{
		{
			name:     "named language",
			language: model.Language{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"},
			want:     []model.Language{{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"}},
		},
		{
			name:     "multilingual and duplicate",
			language: model.Language{Code: "eng, fra,eng", Name: "eng, fra,eng"},
			want:     []model.Language{{Code: "eng", Name: "eng", LocalName: "eng"}, {Code: "fra", Name: "fra", LocalName: "fra"}},
		},
		{name: "empty", want: []model.Language{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := datasetLanguages(test.language); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("datasetLanguages(%#v) = %#v, want %#v", test.language, got, test.want)
			}
		})
	}
}

func TestDatasetHasLanguage(t *testing.T) {
	t.Parallel()
	language := model.Language{Code: "eng,fra,deu"}
	if !datasetHasLanguage(language, "fra") {
		t.Fatal("multilingual dataset should match a component language")
	}
	if datasetHasLanguage(language, "en") {
		t.Fatal("language matching should use exact catalog codes")
	}
}
