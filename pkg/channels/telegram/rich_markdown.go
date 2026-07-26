package telegram

import "strings"

func stripTelegramRichOnlyMarkdown(text string) string {
	text = strings.ReplaceAll(text, "<sub>", "")
	return strings.ReplaceAll(text, "</sub>", "")
}

func markdownToTelegramRichMarkdown(text string) string {
	if text == "" {
		return ""
	}

	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return text
}
