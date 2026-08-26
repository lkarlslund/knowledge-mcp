package kiwix

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

var markdownSpace = regexp.MustCompile(`[ \t]+`)

func htmlMarkdown(source []byte, dataset string, namespace byte, entryPath string) string {
	document, err := html.Parse(bytes.NewReader(source))
	if err != nil {
		return strings.TrimSpace(string(source))
	}
	var output bytes.Buffer
	renderHTMLNode(&output, document, dataset, namespace, entryPath, 0)
	result := strings.ReplaceAll(output.String(), "\u00a0", " ")
	for strings.Contains(result, "\n\n\n") {
		result = strings.ReplaceAll(result, "\n\n\n", "\n\n")
	}
	return strings.TrimSpace(result)
}

func renderHTMLNode(output *bytes.Buffer, node *html.Node, dataset string, namespace byte, entryPath string, listDepth int) {
	if node.Type == html.TextNode {
		text := markdownSpace.ReplaceAllString(node.Data, " ")
		output.WriteString(text)
		return
	}
	if node.Type == html.ElementNode && (node.Data == "script" || node.Data == "style" || node.Data == "noscript" || node.Data == "svg") {
		return
	}
	if node.Type != html.ElementNode {
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			renderHTMLNode(output, child, dataset, namespace, entryPath, listDepth)
		}
		return
	}
	name := strings.ToLower(node.Data)
	switch name {
	case "h1", "h2", "h3", "h4", "h5", "h6":
		level := int(name[1] - '0')
		ensureBlankLine(output)
		output.WriteString(strings.Repeat("#", level) + " ")
	case "p", "div", "section", "article", "header", "footer", "blockquote":
		ensureBlankLine(output)
	case "table":
		renderHTMLTable(output, node)
		ensureBlankLine(output)
		return
	case "br":
		output.WriteString("  \n")
	case "strong", "b":
		output.WriteString("**")
	case "em", "i":
		output.WriteString("*")
	case "code":
		output.WriteByte('`')
	case "pre":
		output.WriteString("```\n")
	case "ul", "ol":
		ensureNewline(output)
		listDepth++
	case "li":
		ensureNewline(output)
		output.WriteString(strings.Repeat("  ", max(0, listDepth-1)) + "- ")
	case "a":
		label := strings.TrimSpace(nodeText(node))
		href := attribute(node, "href")
		if label != "" {
			output.WriteString("[")
			output.WriteString(escapeMarkdown(label))
			output.WriteString("](")
			output.WriteString(markdownLink(dataset, namespace, entryPath, href))
			output.WriteString(")")
		}
		return
	case "img":
		if alt := strings.TrimSpace(attribute(node, "alt")); alt != "" {
			output.WriteString("[Image: " + escapeMarkdown(alt) + "]")
		}
		return
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		renderHTMLNode(output, child, dataset, namespace, entryPath, listDepth)
	}
	switch name {
	case "strong", "b":
		output.WriteString("**")
	case "em", "i":
		output.WriteString("*")
	case "code":
		output.WriteByte('`')
	case "pre":
		output.WriteString("\n```\n")
	case "p", "div", "section", "article", "header", "footer", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol":
		ensureBlankLine(output)
	case "li":
		ensureNewline(output)
	}
}

func renderHTMLTable(output *bytes.Buffer, table *html.Node) {
	var rows [][]string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && strings.EqualFold(node.Data, "tr") {
			var cells []string
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				if child.Type == html.ElementNode && (strings.EqualFold(child.Data, "th") || strings.EqualFold(child.Data, "td")) {
					value := strings.ReplaceAll(nodeText(child), "|", "\\|")
					cells = append(cells, value)
				}
			}
			if len(cells) > 0 {
				rows = append(rows, cells)
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(table)
	if len(rows) == 0 {
		return
	}
	columns := 0
	for _, row := range rows {
		columns = max(columns, len(row))
	}
	writeRow := func(row []string) {
		output.WriteString("| ")
		for column := range columns {
			if column < len(row) {
				output.WriteString(row[column])
			}
			output.WriteString(" | ")
		}
		output.WriteByte('\n')
	}
	writeRow(rows[0])
	separator := make([]string, columns)
	for index := range separator {
		separator[index] = "---"
	}
	writeRow(separator)
	for _, row := range rows[1:] {
		writeRow(row)
	}
}

func markdownLink(dataset string, namespace byte, entryPath, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") {
		return href
	}
	parsed, err := url.Parse(href)
	if err != nil || parsed.IsAbs() || strings.HasPrefix(href, "//") || strings.HasPrefix(href, "mailto:") {
		return href
	}
	target := parsed.Path
	if !strings.HasPrefix(target, "/") {
		target = path.Join(path.Dir(entryPath), target)
	}
	target = strings.TrimPrefix(path.Clean(target), "/")
	if target == "." || target == "" {
		return href
	}
	id := zimDocumentID(namespace, target)
	result := fmt.Sprintf("knowledge-read://read?dataset=%s&id=%s", url.QueryEscape(dataset), id)
	if parsed.Fragment != "" {
		result += "&section=" + url.QueryEscape(parsed.Fragment)
	}
	return result
}

func attribute(node *html.Node, name string) string {
	for _, attribute := range node.Attr {
		if strings.EqualFold(attribute.Key, name) {
			return attribute.Val
		}
	}
	return ""
}

func nodeText(node *html.Node) string {
	var output strings.Builder
	var walk func(*html.Node)
	walk = func(current *html.Node) {
		if current.Type == html.TextNode {
			output.WriteString(current.Data)
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return strings.Join(strings.Fields(output.String()), " ")
}

func nodeTextFromHTML(source []byte) string {
	document, err := html.Parse(bytes.NewReader(source))
	if err != nil {
		return string(source)
	}
	return nodeText(document)
}

func escapeMarkdown(value string) string {
	return strings.NewReplacer("\\", "\\\\", "[", "\\[", "]", "\\]").Replace(value)
}

func ensureNewline(output *bytes.Buffer) {
	if output.Len() > 0 && output.Bytes()[output.Len()-1] != '\n' {
		output.WriteByte('\n')
	}
}

func ensureBlankLine(output *bytes.Buffer) {
	if output.Len() == 0 {
		return
	}
	data := output.Bytes()
	if len(data) >= 2 && data[len(data)-2] == '\n' && data[len(data)-1] == '\n' {
		return
	}
	if data[len(data)-1] == '\n' {
		output.WriteByte('\n')
	} else {
		output.WriteString("\n\n")
	}
}
