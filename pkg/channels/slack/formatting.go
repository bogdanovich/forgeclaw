package slack

import (
	"regexp"
	"strings"
)

const slackMaxTextBlockLength = 3000

var (
	slackBoldRe       = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	slackStrikeRe     = regexp.MustCompile(`~~([^~]+)~~`)
	slackLinkRe       = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
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
	text = slackLinkRe.ReplaceAllString(text, "<$2|$1>")
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
