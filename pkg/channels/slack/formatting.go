package slack

import (
	"regexp"
	"strings"
)

const slackMaxTextBlockLength = 3000

var (
	slackBoldRe       = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	slackStrikeRe     = regexp.MustCompile(`~~([^~]+)~~`)
	slackHeaderRe     = regexp.MustCompile(`(?m)^#{1,6}\s+(.+)$`)
	slackBulletRe     = regexp.MustCompile(`(?m)^- (.+)$`)
	slackCodeBlockRe  = regexp.MustCompile("(?s)```.*?```")
	slackInlineCodeRe = regexp.MustCompile("`[^`]+`")
	slackItalicRe     = regexp.MustCompile(`(?:^|[^*])\*([^*\n]+)\*(?:[^*]|$)`)
)

func formatSlackMessage(text string) string {
	if text == "" {
		return ""
	}

	// Protect code blocks.
	var codeBlocks []string
	text = slackCodeBlockRe.ReplaceAllStringFunc(text, func(match string) string {
		codeBlocks = append(codeBlocks, match)
		return "\x00CODEBLOCK\x00"
	})

	// Protect inline code.
	var inlineCode []string
	text = slackInlineCodeRe.ReplaceAllStringFunc(text, func(match string) string {
		inlineCode = append(inlineCode, match)
		return "\x00INLINE\x00"
	})

	// Convert Markdown italic *text* to Slack _text_ before bold conversion.
	text = slackItalicRe.ReplaceAllStringFunc(text, func(match string) string {
		firstAsterisk := strings.Index(match, "*")
		lastAsterisk := strings.LastIndex(match, "*")
		if firstAsterisk == lastAsterisk {
			return match
		}
		content := match[firstAsterisk+1 : lastAsterisk]
		return match[:firstAsterisk] + "_" + content + "_" + match[lastAsterisk+1:]
	})

	// Convert common Markdown constructs to Slack mrkdwn.
	text = slackBoldRe.ReplaceAllString(text, "*$1*")
	text = slackStrikeRe.ReplaceAllString(text, "~$1~")
	text = replaceSlackMarkdownLinks(text)
	text = slackHeaderRe.ReplaceAllString(text, "*$1*")
	text = slackBulletRe.ReplaceAllString(text, "• $1")

	// Restore inline code and code blocks.
	for _, code := range inlineCode {
		text = strings.Replace(text, "\x00INLINE\x00", code, 1)
	}
	for _, block := range codeBlocks {
		text = strings.Replace(text, "\x00CODEBLOCK\x00", block, 1)
	}

	return text
}

func splitSlackText(text string, maxLen int) []string {
	runes := []rune(text)
	if len(runes) <= maxLen {
		return []string{text}
	}

	var chunks []string
	for len(runes) > 0 {
		if len(runes) <= maxLen {
			chunks = append(chunks, string(runes))
			break
		}
		splitAt := maxLen
		for i := maxLen; i > maxLen/2; i-- {
			if runes[i] == '\n' {
				splitAt = i
				break
			}
		}
		chunk := strings.TrimSpace(string(runes[:splitAt]))
		if chunk != "" {
			chunks = append(chunks, chunk)
		}
		runes = runes[splitAt:]
	}
	return chunks
}

func replaceSlackMarkdownLinks(text string) string {
	var out strings.Builder
	out.Grow(len(text))

	for i := 0; i < len(text); {
		if text[i] != '[' {
			out.WriteByte(text[i])
			i++
			continue
		}

		labelEnd := strings.IndexByte(text[i+1:], ']')
		if labelEnd < 0 {
			out.WriteByte(text[i])
			i++
			continue
		}
		labelEnd += i + 1
		if labelEnd+1 >= len(text) || text[labelEnd+1] != '(' {
			out.WriteByte(text[i])
			i++
			continue
		}

		urlStart := labelEnd + 2
		urlEnd, ok := findMarkdownLinkDestinationEnd(text, urlStart)
		if !ok {
			out.WriteByte(text[i])
			i++
			continue
		}

		label := text[i+1 : labelEnd]
		url := text[urlStart:urlEnd]
		out.WriteByte('<')
		out.WriteString(url)
		out.WriteByte('|')
		out.WriteString(label)
		out.WriteByte('>')
		i = urlEnd + 1
	}

	return out.String()
}

func findMarkdownLinkDestinationEnd(text string, start int) (int, bool) {
	depth := 0
	for i := start; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return i, true
			}
			depth--
		}
	}
	return 0, false
}
