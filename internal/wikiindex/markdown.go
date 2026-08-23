package wikiindex

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

const referencesMarker = "\x00WIKIPEDIA_MCP_REFERENCES\x00"

var (
	referencesTagRE = regexp.MustCompile(`(?is)<references\b[^>]*(?:/>|>.*?</references\s*>)`)
	refNameRE       = regexp.MustCompile(`(?i)\bname\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s/>]+))`)
)

type markdownReference struct {
	name    string
	content string
}

// MarkdownReference is a rendered reference used by Markdown footnote markers.
type MarkdownReference struct {
	ID      int
	Name    string
	Content string
}

// MarkdownDocument contains rendered Markdown and its reference definitions.
type MarkdownDocument struct {
	Content    string
	References []MarkdownReference
}

type markdownTableCell struct {
	value  string
	header bool
}

type markdownRenderer struct {
	baseURL    string
	references []markdownReference
	namedRefs  map[string]int
}

// Markdown converts source wikitext into local, LLM-friendly Markdown. It is a
// syntax conversion rather than a MediaWiki render: templates are preserved as
// readable values but cannot be expanded without the source wiki's template
// and Lua execution environment.
func Markdown(source, baseURL string) string {
	return RenderMarkdown(source, baseURL).Content
}

// RenderMarkdown converts wikitext and also exposes references separately so
// callers can retain footnote definitions when returning a paginated excerpt.
func RenderMarkdown(source, baseURL string) MarkdownDocument {
	renderer := &markdownRenderer{baseURL: strings.TrimRight(baseURL, "/"), namedRefs: make(map[string]int)}
	source = strings.ReplaceAll(source, "\r\n", "\n")
	source = strings.ReplaceAll(source, "\r", "\n")
	source = referencesTagRE.ReplaceAllString(source, referencesMarker)
	source = renderer.extractReferences(source)
	source = renderer.renderTemplates(source)
	result := renderer.renderBlocks(source)
	referenceBlock, references := renderer.renderReferences()
	if strings.Contains(result, referencesMarker) {
		result = strings.ReplaceAll(result, referencesMarker, referenceBlock)
	} else if referenceBlock != "" {
		result += "\n\n## References\n\n" + referenceBlock
	}
	return MarkdownDocument{Content: cleanMarkdown(result), References: references}
}

// PageURL returns the canonical page URL derived from site metadata.
func PageURL(baseURL, title string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" || title == "" {
		return ""
	}
	return wikiLinkURL(baseURL, title)
}

func (r *markdownRenderer) extractReferences(source string) string {
	var out strings.Builder
	for offset := 0; offset < len(source); {
		start := indexASCIIFold(source, offset, "<ref")
		if start < 0 {
			out.WriteString(source[offset:])
			break
		}
		out.WriteString(source[offset:start])
		tagEndOffset := strings.IndexByte(source[start:], '>')
		if tagEndOffset < 0 {
			out.WriteString(source[start:])
			break
		}
		tagEnd := start + tagEndOffset + 1
		tag := source[start:tagEnd]
		if !isRefTag(tag) {
			out.WriteByte(source[start])
			offset = start + 1
			continue
		}

		name := referenceName(tag)
		content := ""
		next := tagEnd
		if !strings.HasSuffix(strings.TrimSpace(tag[:len(tag)-1]), "/") {
			closing := indexASCIIFold(source, tagEnd, "</ref")
			if closing >= 0 {
				content = source[tagEnd:closing]
				closingEnd := strings.IndexByte(source[closing:], '>')
				if closingEnd >= 0 {
					next = closing + closingEnd + 1
				}
			}
		}
		index := r.referenceIndex(name, content)
		if index >= 0 {
			fmt.Fprintf(&out, "[^%d]", index+1)
		}
		offset = next
	}
	return out.String()
}

func referenceName(tag string) string {
	match := refNameRE.FindStringSubmatch(tag)
	if len(match) == 0 {
		return ""
	}
	for _, value := range match[1:] {
		if value != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (r *markdownRenderer) referenceIndex(name, content string) int {
	if name != "" {
		if index, ok := r.namedRefs[name]; ok {
			if r.references[index].content == "" && strings.TrimSpace(content) != "" {
				r.references[index].content = content
			}
			return index
		}
	}
	if name == "" && strings.TrimSpace(content) == "" {
		return -1
	}
	index := len(r.references)
	r.references = append(r.references, markdownReference{name: name, content: content})
	if name != "" {
		r.namedRefs[name] = index
	}
	return index
}

func (r *markdownRenderer) renderReferences() (string, []MarkdownReference) {
	if len(r.references) == 0 {
		return "", nil
	}
	lines := make([]string, 0, len(r.references))
	references := make([]MarkdownReference, 0, len(r.references))
	for index, reference := range r.references {
		content := strings.TrimSpace(r.renderBlocks(r.renderTemplates(reference.content)))
		if content == "" {
			content = "Named reference `" + escapeMarkdown(reference.name) + "`"
		}
		content = strings.ReplaceAll(content, "\n", "\n    ")
		lines = append(lines, fmt.Sprintf("[^%d]: %s", index+1, content))
		references = append(references, MarkdownReference{ID: index + 1, Name: reference.name, Content: content})
	}
	return strings.Join(lines, "\n"), references
}

func (r *markdownRenderer) renderTemplates(source string) string {
	var out strings.Builder
	for offset := 0; offset < len(source); {
		start := strings.Index(source[offset:], "{{")
		if start < 0 {
			out.WriteString(source[offset:])
			break
		}
		start += offset
		out.WriteString(source[offset:start])
		end := skipMarkup(source, start, "{{", "}}")
		if end <= start || end > len(source) {
			out.WriteString(source[start:])
			break
		}
		out.WriteString(r.renderTemplate(source[start+2 : end-2]))
		offset = end
	}
	return out.String()
}

func (r *markdownRenderer) renderTemplate(source string) string {
	parts := splitTopLevel(source, '|')
	if len(parts) == 0 {
		return ""
	}
	name := strings.TrimSpace(parts[0])
	normalized := strings.ToLower(strings.ReplaceAll(name, "_", " "))
	if normalized == "short description" || normalized == "about" || normalized == "for" || strings.HasPrefix(normalized, "redirect") || strings.HasPrefix(normalized, "use ") || strings.HasPrefix(normalized, "pp-") {
		return ""
	}
	if normalized == "reflist" || normalized == "references" || normalized == "notelist" {
		return referencesMarker
	}
	parameters := parseTemplateParameters(parts[1:])
	if strings.HasPrefix(normalized, "cite ") || normalized == "citation" {
		return r.renderCitation(parameters)
	}
	if strings.HasPrefix(normalized, "infobox") {
		return r.renderInfobox(name, parameters)
	}
	if normalized == "citation needed" || normalized == "cn" || normalized == "fact" {
		return "*[citation needed]*"
	}
	if normalized == "efn" || normalized == "refn" || normalized == "note" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		if len(values) > 0 {
			return " *(Note: " + strings.Join(values, "; ") + ")*"
		}
		return ""
	}
	if normalized == "sfn" || normalized == "sfnp" || normalized == "harv" || normalized == "harvnb" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		if len(values) > 0 {
			return "[" + strings.Join(values, ", ") + "]"
		}
		return ""
	}
	if normalized == "main" || normalized == "see also" || normalized == "further" {
		links := make([]string, 0, len(parameters))
		for _, parameter := range parameters {
			if parameter.key == "" && strings.TrimSpace(parameter.value) != "" {
				title := strings.TrimSpace(r.renderFragment(parameter.value))
				links = append(links, r.internalLink(title, title))
			}
		}
		if len(links) > 0 {
			label := map[string]string{"main": "Main", "see also": "See also", "further": "Further"}[normalized]
			return "**" + label + ":** " + strings.Join(links, ", ")
		}
	}
	if normalized == "lang" || normalized == "langx" || normalized == "native name" || normalized == "ipa" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		if len(values) > 1 {
			return strings.Join(values[1:], " ")
		}
		if len(values) == 1 {
			return values[0]
		}
		return ""
	}
	if strings.HasPrefix(normalized, "ipac-") {
		value := strings.Join(positionalTemplateValues(parameters, r.renderFragment), "")
		value = strings.ReplaceAll(value, "_", " ")
		if value != "" {
			return "/" + value + "/"
		}
		return ""
	}
	if normalized == "respell" {
		value := strings.Join(positionalTemplateValues(parameters, r.renderFragment), "-")
		return strings.ReplaceAll(value, "_", " ")
	}
	if normalized == "url" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		if len(values) > 0 {
			return "<" + values[0] + ">"
		}
		return ""
	}
	if normalized == "flag" || normalized == "flagdeco" || normalized == "flagicon" || normalized == "formatnum" || normalized == "nobold" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		if len(values) > 0 {
			return values[0]
		}
		return ""
	}
	if normalized == "nowrap" || normalized == "small" || normalized == "large" || normalized == "center" || normalized == "quote" || normalized == "blockquote" {
		for index := len(parameters) - 1; index >= 0; index-- {
			if parameters[index].key == "" && strings.TrimSpace(parameters[index].value) != "" {
				return r.renderFragment(parameters[index].value)
			}
		}
	}
	if normalized == "convert" {
		values := positionalTemplateValues(parameters, r.renderFragment)
		return strings.Join(values, " ")
	}

	values := make([]string, 0, len(parameters))
	for _, parameter := range parameters {
		value := strings.TrimSpace(r.renderFragment(parameter.value))
		if value == "" {
			continue
		}
		if parameter.key != "" {
			values = append(values, escapeMarkdown(parameter.key)+": "+value)
		} else {
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return ""
	}
	return "**" + escapeMarkdown(name) + ":** " + strings.Join(values, "; ")
}

type templateParameter struct {
	key   string
	value string
}

func parseTemplateParameters(parts []string) []templateParameter {
	parameters := make([]templateParameter, 0, len(parts))
	for _, part := range parts {
		key, value, ok := templateAssignment(part)
		if ok {
			parameters = append(parameters, templateParameter{key: strings.TrimSpace(key), value: strings.TrimSpace(value)})
		} else {
			parameters = append(parameters, templateParameter{value: strings.TrimSpace(part)})
		}
	}
	return parameters
}

func templateAssignment(value string) (string, string, bool) {
	index := indexTopLevel(value, '=')
	if index < 0 {
		return "", "", false
	}
	key := strings.TrimSpace(value[:index])
	if key == "" {
		return "", "", false
	}
	for _, character := range key {
		if !unicode.IsLetter(character) && !unicode.IsDigit(character) && character != ' ' && character != '_' && character != '-' {
			return "", "", false
		}
	}
	return key, value[index+1:], true
}

func positionalTemplateValues(parameters []templateParameter, render func(string) string) []string {
	values := make([]string, 0, len(parameters))
	for _, parameter := range parameters {
		if parameter.key == "" {
			if value := strings.TrimSpace(render(parameter.value)); value != "" {
				values = append(values, value)
			}
		}
	}
	return values
}

func (r *markdownRenderer) renderCitation(parameters []templateParameter) string {
	values := make(map[string]string)
	var positional []string
	for _, parameter := range parameters {
		value := strings.TrimSpace(r.renderFragment(parameter.value))
		if parameter.key == "" {
			if value != "" {
				positional = append(positional, value)
			}
			continue
		}
		values[strings.ToLower(strings.ReplaceAll(parameter.key, "_", "-"))] = value
	}
	title, address := firstValue(values, "title", "chapter", "article"), firstValue(values, "url", "chapter-url")
	primary := title
	if address != "" {
		if primary == "" {
			primary = "Source"
		}
		primary = "[" + escapeMarkdown(primary) + "](" + address + ")"
	}
	if primary == "" && len(positional) > 0 {
		primary = strings.Join(positional, "; ")
	}
	author := firstValue(values, "author", "author1")
	if author == "" {
		first, last := firstValue(values, "first", "first1"), firstValue(values, "last", "last1")
		author = strings.TrimSpace(first + " " + last)
	}
	metadata := []string{author, firstValue(values, "work", "website", "journal", "publisher"), firstValue(values, "date", "year")}
	for _, identifier := range []string{"doi", "isbn", "access-date"} {
		if value := values[identifier]; value != "" {
			metadata = append(metadata, strings.ToUpper(identifier)+" "+value)
		}
	}
	for _, value := range metadata {
		if value != "" {
			if primary != "" {
				primary += ". "
			}
			primary += value
		}
	}
	return primary
}

func firstValue(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if values[key] != "" {
			return values[key]
		}
	}
	return ""
}

func (r *markdownRenderer) renderInfobox(name string, parameters []templateParameter) string {
	rows := make([]string, 0, len(parameters)+2)
	rows = append(rows, "| "+escapeTableCell(strings.TrimSpace(name))+" | |", "|---|---|")
	for _, parameter := range parameters {
		value := strings.TrimSpace(r.renderFragment(parameter.value))
		if value == "" {
			continue
		}
		key := parameter.key
		if key == "" {
			key = "Value"
		}
		rows = append(rows, "| **"+escapeTableCell(key)+"** | "+escapeTableCell(value)+" |")
	}
	if len(rows) == 2 {
		return ""
	}
	return "\n\n" + strings.Join(rows, "\n") + "\n\n"
}

func (r *markdownRenderer) renderFragment(source string) string {
	return r.renderInline(r.renderTemplates(source))
}

func (r *markdownRenderer) renderBlocks(source string) string {
	lines := strings.Split(source, "\n")
	var out strings.Builder
	for index := 0; index < len(lines); {
		trimmed := strings.TrimSpace(lines[index])
		if strings.HasPrefix(trimmed, "{|") {
			end := index + 1
			for end < len(lines) && !strings.HasPrefix(strings.TrimSpace(lines[end]), "|}") {
				end++
			}
			if end < len(lines) {
				end++
			}
			out.WriteString(r.renderTable(lines[index:end]))
			out.WriteString("\n\n")
			index = end
			continue
		}
		if level, heading, ok := wikiHeading(lines[index]); ok {
			out.WriteString(strings.Repeat("#", level))
			out.WriteByte(' ')
			out.WriteString(strings.TrimSpace(r.renderInline(heading)))
			out.WriteString("\n\n")
			index++
			continue
		}
		if strings.HasPrefix(lines[index], " ") && strings.TrimSpace(lines[index]) != "" {
			out.WriteString("```text\n")
			for index < len(lines) && strings.HasPrefix(lines[index], " ") {
				out.WriteString(strings.TrimPrefix(lines[index], " "))
				out.WriteByte('\n')
				index++
			}
			out.WriteString("```\n\n")
			continue
		}
		if prefix, body, ok := wikiListItem(lines[index]); ok {
			out.WriteString(prefix)
			out.WriteString(strings.TrimSpace(r.renderInline(body)))
			out.WriteByte('\n')
			index++
			continue
		}
		if strings.HasPrefix(trimmed, "----") {
			out.WriteString("---\n\n")
			index++
			continue
		}
		out.WriteString(r.renderInline(lines[index]))
		out.WriteByte('\n')
		index++
	}
	return out.String()
}

func wikiHeading(line string) (int, string, bool) {
	trimmed := strings.TrimSpace(line)
	start := 0
	for start < len(trimmed) && trimmed[start] == '=' {
		start++
	}
	end := len(trimmed)
	for end > start && trimmed[end-1] == '=' {
		end--
	}
	if start < 2 || start > 6 || len(trimmed)-end != start {
		return 0, "", false
	}
	return start, trimmed[start:end], true
}

func wikiListItem(line string) (string, string, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	depth := 0
	for depth < len(trimmed) && strings.ContainsRune("*#;:", rune(trimmed[depth])) {
		depth++
	}
	if depth == 0 {
		return "", "", false
	}
	if depth >= len(trimmed) || !unicode.IsSpace(rune(trimmed[depth])) {
		return "", "", false
	}
	indent := strings.Repeat("  ", depth-1)
	switch trimmed[depth-1] {
	case '#':
		return indent + "1. ", trimmed[depth:], true
	case ':':
		return strings.Repeat("> ", depth), trimmed[depth:], true
	case ';':
		return indent + "- **", strings.TrimSpace(trimmed[depth:]) + "**", true
	default:
		return indent + "- ", trimmed[depth:], true
	}
}

func (r *markdownRenderer) renderTable(lines []string) string {
	var caption string
	var rows [][]markdownTableCell
	var current []markdownTableCell
	commit := func() {
		if len(current) > 0 {
			rows = append(rows, current)
			current = nil
		}
	}
	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "|}"):
			commit()
		case strings.HasPrefix(trimmed, "|-"):
			commit()
		case strings.HasPrefix(trimmed, "|+"):
			caption = strings.TrimSpace(r.renderFragment(strings.TrimSpace(trimmed[2:])))
		case strings.HasPrefix(trimmed, "!"):
			for _, value := range splitTopLevelString(trimmed[1:], "!!") {
				current = append(current, markdownTableCell{value: tableCellValue(value), header: true})
			}
		case strings.HasPrefix(trimmed, "|"):
			for _, value := range splitTopLevelString(trimmed[1:], "||") {
				current = append(current, markdownTableCell{value: tableCellValue(value)})
			}
		case trimmed != "" && len(current) > 0:
			current[len(current)-1].value += " " + trimmed
		}
	}
	commit()
	if len(rows) == 0 {
		return ""
	}
	columns := 0
	for _, row := range rows {
		columns = max(columns, len(row))
	}
	header := rows[0]
	if !rowHasHeader(header) {
		header = make([]markdownTableCell, columns)
		for index := range header {
			header[index] = markdownTableCell{value: fmt.Sprintf("Column %d", index+1), header: true}
		}
	} else {
		rows = rows[1:]
	}
	var out strings.Builder
	if caption != "" {
		out.WriteString("**")
		out.WriteString(caption)
		out.WriteString("**\n\n")
	}
	writeTableRow(&out, header, columns, r)
	out.WriteByte('|')
	for range columns {
		out.WriteString("---|")
	}
	out.WriteByte('\n')
	for _, row := range rows {
		writeTableRow(&out, row, columns, r)
	}
	return strings.TrimSpace(out.String())
}

func tableCellValue(value string) string {
	value = strings.TrimSpace(value)
	if separator := indexTopLevel(value, '|'); separator >= 0 && strings.Contains(value[:separator], "=") {
		value = strings.TrimSpace(value[separator+1:])
	}
	return value
}

func rowHasHeader(row []markdownTableCell) bool {
	for _, cell := range row {
		if cell.header {
			return true
		}
	}
	return false
}

func writeTableRow(out *strings.Builder, row []markdownTableCell, columns int, renderer *markdownRenderer) {
	out.WriteByte('|')
	for index := 0; index < columns; index++ {
		value := ""
		header := false
		if index < len(row) {
			value = escapeTableCell(strings.TrimSpace(renderer.renderFragment(row[index].value)))
			header = row[index].header
		}
		if header {
			value = "**" + value + "**"
		}
		out.WriteByte(' ')
		out.WriteString(value)
		out.WriteString(" |")
	}
	out.WriteByte('\n')
}

func (r *markdownRenderer) renderInline(source string) string {
	var out strings.Builder
	for offset := 0; offset < len(source); {
		switch {
		case strings.HasPrefix(source[offset:], "<!--"):
			end := strings.Index(source[offset+4:], "-->")
			if end < 0 {
				return out.String()
			}
			offset += 4 + end + 3
		case strings.HasPrefix(source[offset:], "[["):
			end := strings.Index(source[offset+2:], "]]")
			if end < 0 {
				out.WriteString(source[offset:])
				return out.String()
			}
			value := source[offset+2 : offset+2+end]
			parts := splitTopLevel(value, '|')
			target := strings.TrimSpace(parts[0])
			label := internalLinkLabel(target, parts[1:])
			out.WriteString(r.internalLink(target, r.renderFragment(label)))
			offset += 2 + end + 2
		case source[offset] == '[':
			end := strings.IndexByte(source[offset+1:], ']')
			if end < 0 {
				out.WriteByte(source[offset])
				offset++
				continue
			}
			closing := offset + 1 + end
			value := strings.TrimSpace(source[offset+1 : closing])
			if strings.HasPrefix(value, "^") {
				out.WriteString(source[offset : closing+1])
				offset = closing + 1
				continue
			}
			if closing+1 < len(source) && source[closing+1] == '(' {
				markdownEnd := strings.IndexByte(source[closing+2:], ')')
				if markdownEnd >= 0 {
					markdownEnd += closing + 2
					out.WriteString(source[offset : markdownEnd+1])
					offset = markdownEnd + 1
					continue
				}
			}
			space := strings.IndexAny(value, " \t\r\n")
			if space > 0 && isExternalURL(value[:space]) {
				out.WriteString("[")
				out.WriteString(escapeMarkdown(strings.TrimSpace(r.renderFragment(value[space+1:]))))
				out.WriteString("](")
				out.WriteString(value[:space])
				out.WriteByte(')')
			} else if isExternalURL(value) {
				out.WriteByte('<')
				out.WriteString(value)
				out.WriteByte('>')
			} else {
				out.WriteString(value)
			}
			offset = closing + 1
		case strings.HasPrefix(source[offset:], "'''''"):
			offset = r.renderDelimited(&out, source, offset, "'''''", "***")
		case strings.HasPrefix(source[offset:], "'''"):
			offset = r.renderDelimited(&out, source, offset, "'''", "**")
		case strings.HasPrefix(source[offset:], "''"):
			offset = r.renderDelimited(&out, source, offset, "''", "*")
		case source[offset] == '<':
			value, next := r.renderHTML(source, offset)
			out.WriteString(value)
			offset = next
		case hasASCIIPrefixFold(source[offset:], "__TOC__"):
			offset += len("__TOC__")
		default:
			out.WriteByte(source[offset])
			offset++
		}
	}
	return html.UnescapeString(out.String())
}

func internalLinkLabel(target string, options []string) string {
	label := target
	isMedia := hasASCIIPrefixFold(target, "file:") || hasASCIIPrefixFold(target, "image:")
	for index := len(options) - 1; index >= 0; index-- {
		candidate := strings.TrimSpace(options[index])
		if candidate == "" || isMedia && isMediaOption(candidate) {
			continue
		}
		label = candidate
		break
	}
	if isMedia {
		label = strings.TrimSpace(label)
		if label == target {
			if separator := strings.IndexByte(target, ':'); separator >= 0 {
				label = target[separator+1:]
			}
		}
		return "Image: " + label
	}
	return label
}

func isMediaOption(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if strings.HasSuffix(normalized, "px") || strings.HasPrefix(normalized, "alt=") || strings.HasPrefix(normalized, "link=") || strings.HasPrefix(normalized, "upright") {
		return true
	}
	switch normalized {
	case "thumb", "thumbnail", "frameless", "frame", "border", "left", "right", "center", "none", "baseline", "sub", "super", "top", "text-top", "middle", "bottom", "text-bottom":
		return true
	default:
		return false
	}
}

func (r *markdownRenderer) renderDelimited(out *strings.Builder, source string, offset int, delimiter, markdown string) int {
	end := strings.Index(source[offset+len(delimiter):], delimiter)
	if end < 0 {
		out.WriteString(delimiter)
		return offset + len(delimiter)
	}
	innerStart := offset + len(delimiter)
	out.WriteString(markdown)
	out.WriteString(r.renderInline(source[innerStart : innerStart+end]))
	out.WriteString(markdown)
	return innerStart + end + len(delimiter)
}

func (r *markdownRenderer) renderHTML(source string, offset int) (string, int) {
	tagEndOffset := strings.IndexByte(source[offset:], '>')
	if tagEndOffset < 0 {
		return source[offset:], len(source)
	}
	tagEnd := offset + tagEndOffset + 1
	tag := source[offset:tagEnd]
	name := htmlTagName(tag)
	if name == "br" {
		return "  \n", tagEnd
	}
	if strings.HasPrefix(tag, "</") || strings.HasSuffix(strings.TrimSpace(tag[:len(tag)-1]), "/") {
		return "", tagEnd
	}
	for _, special := range []string{"code", "math", "pre", "syntaxhighlight", "source", "nowiki"} {
		if name != special {
			continue
		}
		closing := indexASCIIFold(source, tagEnd, "</"+special)
		if closing < 0 {
			return "", tagEnd
		}
		closingEndOffset := strings.IndexByte(source[closing:], '>')
		if closingEndOffset < 0 {
			return "", len(source)
		}
		content := html.UnescapeString(source[tagEnd:closing])
		next := closing + closingEndOffset + 1
		switch special {
		case "code", "nowiki":
			return "`" + strings.ReplaceAll(content, "`", "\\`") + "`", next
		case "math":
			return "$" + content + "$", next
		default:
			return "\n\n```\n" + strings.TrimSpace(content) + "\n```\n\n", next
		}
	}
	return "", tagEnd
}

func htmlTagName(tag string) string {
	start := 1
	if start < len(tag) && tag[start] == '/' {
		start++
	}
	end := start
	for end < len(tag) && (unicode.IsLetter(rune(tag[end])) || unicode.IsDigit(rune(tag[end]))) {
		end++
	}
	return strings.ToLower(tag[start:end])
}

func (r *markdownRenderer) internalLink(target, label string) string {
	target = strings.TrimSpace(target)
	label = strings.TrimSpace(label)
	if target == "" {
		return label
	}
	return "[" + escapeMarkdown(label) + "](" + wikiLinkURL(r.baseURL, target) + ")"
}

func wikiLinkURL(baseURL, target string) string {
	fragment := ""
	if index := strings.IndexByte(target, '#'); index >= 0 {
		fragment = target[index+1:]
		target = target[:index]
	}
	target = strings.ReplaceAll(strings.TrimSpace(target), " ", "_")
	address := "wiki:" + url.PathEscape(target)
	if baseURL != "" {
		address = strings.TrimRight(baseURL, "/") + "/wiki/" + url.PathEscape(target)
	}
	if fragment != "" {
		address += "#" + url.PathEscape(strings.ReplaceAll(fragment, " ", "_"))
	}
	return address
}

func splitTopLevel(source string, separator byte) []string {
	var parts []string
	start := 0
	curly, square := 0, 0
	for index := 0; index < len(source); index++ {
		switch {
		case strings.HasPrefix(source[index:], "{{"):
			curly++
			index++
		case curly > 0 && strings.HasPrefix(source[index:], "}}"):
			curly--
			index++
		case strings.HasPrefix(source[index:], "[["):
			square++
			index++
		case square > 0 && strings.HasPrefix(source[index:], "]]"):
			square--
			index++
		case source[index] == separator && curly == 0 && square == 0:
			parts = append(parts, source[start:index])
			start = index + 1
		}
	}
	return append(parts, source[start:])
}

func splitTopLevelString(source, separator string) []string {
	if separator == "" {
		return []string{source}
	}
	var parts []string
	start := 0
	curly, square := 0, 0
	for index := 0; index < len(source); index++ {
		switch {
		case strings.HasPrefix(source[index:], "{{"):
			curly++
			index++
		case curly > 0 && strings.HasPrefix(source[index:], "}}"):
			curly--
			index++
		case strings.HasPrefix(source[index:], "[["):
			square++
			index++
		case square > 0 && strings.HasPrefix(source[index:], "]]"):
			square--
			index++
		case curly == 0 && square == 0 && strings.HasPrefix(source[index:], separator):
			parts = append(parts, source[start:index])
			start = index + len(separator)
			index += len(separator) - 1
		}
	}
	return append(parts, source[start:])
}

func indexTopLevel(source string, separator byte) int {
	parts := splitTopLevel(source, separator)
	if len(parts) < 2 {
		return -1
	}
	return len(parts[0])
}

func escapeMarkdown(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "[", "\\[")
	value = strings.ReplaceAll(value, "]", "\\]")
	return value
}

func escapeTableCell(value string) string {
	value = strings.ReplaceAll(value, "\n", "<br>")
	return strings.ReplaceAll(value, "|", "\\|")
}

func cleanMarkdown(value string) string {
	value = html.UnescapeString(value)
	for strings.Contains(value, "\n\n\n") {
		value = strings.ReplaceAll(value, "\n\n\n", "\n\n")
	}
	lines := strings.Split(value, "\n")
	for index, line := range lines {
		if !strings.HasSuffix(line, "  ") {
			lines[index] = strings.TrimRightFunc(line, unicode.IsSpace)
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
