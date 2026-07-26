package telegram

import "strings"

const (
	telegramRichFooterPrefix = "\n\n---\n<sub>"
	telegramRichFooterSuffix = "</sub>"
)

func unwrapTelegramRichFooter(text string) string {
	if !strings.HasSuffix(text, telegramRichFooterSuffix) {
		return text
	}
	start := strings.LastIndex(text, telegramRichFooterPrefix)
	if start < 0 {
		return text
	}
	footerStart := start + len(telegramRichFooterPrefix)
	footer := text[footerStart : len(text)-len(telegramRichFooterSuffix)]
	if strings.ContainsAny(footer, "\r\n") ||
		(!strings.HasPrefix(footer, "model: ") && !strings.HasPrefix(footer, "tokens: ")) {
		return text
	}
	return text[:start] + "\n\n---\n" + footer
}

func markdownToTelegramRichMarkdown(text string) string {
	if text == "" {
		return ""
	}

	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return text
}
