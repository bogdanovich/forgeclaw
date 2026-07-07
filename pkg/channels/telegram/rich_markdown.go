package telegram

import (
	"fmt"
	"html"
	"regexp"
	"strings"
)

var (
	reRichMarkdownMultiBacktickCode  = regexp.MustCompile("`{2,}([^`\n]+)`{2,}")
	reRichMarkdownSingleBacktickCode = regexp.MustCompile("`[^`\n]+`")
	reRichMarkdownHTMLLink           = regexp.MustCompile(
		`(?is)<a\s+[^>]*href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>`,
	)
	reRichMarkdownBlockquote = regexp.MustCompile(
		`(?is)<blockquote[^>]*>(.*?)</blockquote>`,
	)
	reRichMarkdownSingleTilde = regexp.MustCompile(`(^|[^~])~([^~\n]+)~([^~]|$)`)
	reRichMarkdownHeading     = regexp.MustCompile(`(?m)^(#{1,6})[ \t]+(.+?)[ \t]*$`)
)

func markdownToTelegramRichMarkdown(text string) string {
	if text == "" {
		return ""
	}

	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	text = normalizeRichMarkdownHeadings(text)

	code := protectRichMarkdownCode(text)
	text = code.text

	text = reRichMarkdownHTMLLink.ReplaceAllStringFunc(text, func(match string) string {
		parts := reRichMarkdownHTMLLink.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		label := strings.TrimSpace(stripKnownRichMarkdownHTMLTags(parts[2]))
		if label == "" {
			label = parts[1]
		}
		return fmt.Sprintf(
			"[%s](%s)",
			escapeRichMarkdownLinkLabel(label),
			escapeRichMarkdownLinkURL(html.UnescapeString(parts[1])),
		)
	})
	text = reRichMarkdownBlockquote.ReplaceAllStringFunc(text, func(match string) string {
		parts := reRichMarkdownBlockquote.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		inner := strings.TrimSpace(stripKnownRichMarkdownHTMLTags(parts[1]))
		if inner == "" {
			return ""
		}
		lines := strings.Split(inner, "\n")
		for i, line := range lines {
			lines[i] = "> " + strings.TrimSpace(line)
		}
		return strings.Join(lines, "\n")
	})

	text = replaceKnownRichMarkdownHTMLTags(text)
	text = reRichMarkdownSingleTilde.ReplaceAllString(text, "$1~~$2~~$3")

	text = code.restore(text)
	return strings.TrimSpace(text)
}

func normalizeRichMarkdownHeadings(text string) string {
	return reRichMarkdownHeading.ReplaceAllStringFunc(text, func(match string) string {
		parts := reRichMarkdownHeading.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		level := len(parts[1])
		title := strings.TrimSpace(parts[2])
		return strings.Repeat("#", level) + " " + title
	})
}

type richMarkdownCodePlaceholders struct {
	text  string
	items []string
}

func protectRichMarkdownCode(text string) richMarkdownCodePlaceholders {
	items := make([]string, 0)
	text = protectRichMarkdownFencedCode(text, &items)
	text = reRichMarkdownMultiBacktickCode.ReplaceAllStringFunc(text, func(match string) string {
		items = append(items, normalizeRichMarkdownInlineCode(match))
		return fmt.Sprintf("\x00TGCODE%d\x00", len(items)-1)
	})
	text = reRichMarkdownSingleBacktickCode.ReplaceAllStringFunc(text, func(match string) string {
		items = append(items, normalizeRichMarkdownInlineCode(match))
		return fmt.Sprintf("\x00TGCODE%d\x00", len(items)-1)
	})
	return richMarkdownCodePlaceholders{text: text, items: items}
}

func protectRichMarkdownFencedCode(text string, items *[]string) string {
	var b strings.Builder
	unchangedStart := 0
	lineStart := 0
	for lineStart < len(text) {
		lineEnd, nextLineStart := richMarkdownLineBounds(text, lineStart)
		openLen, ok := richMarkdownOpeningFenceLength(text[lineStart:lineEnd])
		if !ok {
			lineStart = nextLineStart
			continue
		}

		searchStart := nextLineStart
		found := false
		for searchStart < len(text) {
			closeLineEnd, closeNextLineStart := richMarkdownLineBounds(text, searchStart)
			if isRichMarkdownClosingFenceLine(text[searchStart:closeLineEnd], openLen) {
				b.WriteString(text[unchangedStart:lineStart])
				block := text[lineStart:closeNextLineStart]
				*items = append(*items, block)
				b.WriteString(fmt.Sprintf("\x00TGCODE%d\x00", len(*items)-1))
				unchangedStart = closeNextLineStart
				lineStart = closeNextLineStart
				found = true
				break
			}
			searchStart = closeNextLineStart
		}
		if !found {
			lineStart = nextLineStart
		}
	}
	b.WriteString(text[unchangedStart:])
	return b.String()
}

func richMarkdownLineBounds(text string, start int) (lineEnd int, nextLineStart int) {
	if idx := strings.IndexByte(text[start:], '\n'); idx >= 0 {
		lineEnd = start + idx
		return lineEnd, lineEnd + 1
	}
	return len(text), len(text)
}

func richMarkdownOpeningFenceLength(line string) (int, bool) {
	line = strings.TrimLeft(line, " \t")
	count := countLeadingBackticks(line)
	return count, count >= 3
}

func isRichMarkdownClosingFenceLine(line string, openLen int) bool {
	line = strings.TrimSpace(line)
	count := countLeadingBackticks(line)
	if count < openLen {
		return false
	}
	return strings.TrimSpace(line[count:]) == ""
}

func countLeadingBackticks(text string) int {
	count := 0
	for count < len(text) && text[count] == '`' {
		count++
	}
	return count
}

func normalizeRichMarkdownInlineCode(match string) string {
	start := 0
	for start < len(match) && match[start] == '`' {
		start++
	}
	end := len(match)
	for end > start && match[end-1] == '`' {
		end--
	}
	content := strings.TrimSpace(match[start:end])
	if content == "" {
		return match
	}
	if !strings.Contains(content, "`") {
		return "`" + content + "`"
	}
	return "```\n" + content + "\n```"
}

func (p richMarkdownCodePlaceholders) restore(text string) string {
	for i, item := range p.items {
		text = strings.ReplaceAll(text, fmt.Sprintf("\x00TGCODE%d\x00", i), item)
	}
	return text
}

func replaceKnownRichMarkdownHTMLTags(text string) string {
	text = regexp.MustCompile(`(?is)<\s*h([1-6])(?:\s+[^<>]*)?>`).
		ReplaceAllStringFunc(text, func(match string) string {
			parts := regexp.MustCompile(`(?is)<\s*h([1-6])`).FindStringSubmatch(match)
			if len(parts) != 2 {
				return match
			}
			return strings.Repeat("#", int(parts[1][0]-'0')) + " "
		})

	replacements := []struct {
		pattern string
		value   string
	}{
		{`(?is)<\s*br\s*/?\s*>`, "\n"},
		{`(?is)</\s*(p|div)\s*>`, "\n\n"},
		{`(?is)<\s*li(?:\s+[^<>]*)?>`, "- "},
		{`(?is)</\s*li\s*>`, "\n"},
		{`(?is)</\s*h[1-6]\s*>`, "\n"},
		{`(?is)</?\s*(p|div|span|ul|ol)(?:\s+[^<>]*)?>`, ""},
	}
	for _, repl := range replacements {
		text = regexp.MustCompile(repl.pattern).ReplaceAllString(text, repl.value)
	}
	return html.UnescapeString(text)
}

func stripKnownRichMarkdownHTMLTags(text string) string {
	text = html.UnescapeString(text)
	replacements := []struct {
		pattern string
		value   string
	}{
		{`(?is)<\s*br\s*/?\s*>`, "\n"},
		{`(?is)</\s*(p|div|li|h[1-6])\s*>`, "\n"},
		{`(?is)<\s*li(?:\s+[^<>]*)?>`, "- "},
		{`(?is)<[^>]*>`, ""},
	}
	for _, repl := range replacements {
		text = regexp.MustCompile(repl.pattern).ReplaceAllString(text, repl.value)
	}
	return strings.TrimSpace(text)
}

func escapeRichMarkdownLinkLabel(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, `[`, `\[`)
	text = strings.ReplaceAll(text, `]`, `\]`)
	return text
}

func escapeRichMarkdownLinkURL(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, `)`, `\)`)
	return text
}
