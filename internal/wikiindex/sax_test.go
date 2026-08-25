package wikiindex

import (
	"bytes"
	"encoding/xml"
	"reflect"
	"testing"
)

func TestSelectedSAXDecoderMatchesEncodingXML(t *testing.T) {
	t.Parallel()
	input := []byte(`<page><title>Ignored &amp; skipped</title><ns>0</ns><id>1</id><revision><id>10</id><timestamp>2026-01-01T00:00:00Z</timestamp><text>A large body that should not be materialized.</text></revision></page>
<page><title>Mette &amp; Greenland</title><ns>0</ns><id>2</id><revision><id>20</id><timestamp>2026-08-25T12:34:56Z</timestamp><text xml:space="preserve">Lead &lt;ref&gt;source&lt;/ref&gt;&#10;Second line.</text></revision></page>
<page><title>Alias</title><ns>0</ns><id>3</id><redirect title="Mette &amp; Greenland"/><revision><id>30</id><text><![CDATA[#REDIRECT [[Mette & Greenland]]]]></text></revision></page></mediawiki>`)
	wanted := map[uint64]struct{}{2: {}, 3: {}}

	want, err := decodeSelectedXMLPages(xml.NewDecoder(bytes.NewReader(input)), wanted)
	if err != nil {
		t.Fatalf("encoding/xml decoder: %v", err)
	}
	got, err := decodeSelectedXMLPagesSAX(bytes.NewReader(input), wanted)
	if err != nil {
		t.Fatalf("gosax decoder: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gosax pages differ\n got: %#v\nwant: %#v", got, want)
	}
	wantAll, err := decodeXMLPages(xml.NewDecoder(bytes.NewReader(input)))
	if err != nil {
		t.Fatalf("encoding/xml full decoder: %v", err)
	}
	gotAll, err := decodeSelectedXMLPagesSAX(bytes.NewReader(input), nil)
	if err != nil {
		t.Fatalf("gosax full decoder: %v", err)
	}
	if !reflect.DeepEqual(gotAll, wantAll) {
		t.Fatalf("gosax full pages differ\n got: %#v\nwant: %#v", gotAll, wantAll)
	}
}
