package telegram

import "strings"

func escapeHTML(text string) string {
	text = strings.ReplaceAll(text, "&", "&amp;")
	text = strings.ReplaceAll(text, "<", "&lt;")
	text = strings.ReplaceAll(text, ">", "&gt;")
	return text
}

func escapeHTMLAttr(text string) string {
	text = escapeHTML(text)
	text = strings.ReplaceAll(text, `"`, "&quot;")
	return text
}
