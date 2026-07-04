package telegram

import (
	"strconv"
	"strings"

	"github.com/mymmrac/telego"
)

type telegramRichBlockKind int

const (
	telegramRichBlockParagraph telegramRichBlockKind = iota
	telegramRichBlockHeading
	telegramRichBlockBulletList
	telegramRichBlockNumberedList
	telegramRichBlockCode
	telegramRichBlockQuote
	telegramRichBlockDivider
)

type telegramRichInlineKind int

const (
	telegramRichInlineText telegramRichInlineKind = iota
	telegramRichInlineBold
	telegramRichInlineItalic
	telegramRichInlineUnderline
	telegramRichInlineStrike
	telegramRichInlineCode
	telegramRichInlineLink
)

type telegramRichMessage struct {
	Blocks              []telegramRichBlock
	SkipEntityDetection bool
	IsRTL               bool
}

type telegramRichBlock struct {
	Kind     telegramRichBlockKind
	Level    int
	Inlines  []telegramRichInline
	Items    []telegramRichListItem
	Code     string
	Language string
}

type telegramRichListItem struct {
	Inlines []telegramRichInline
}

type telegramRichInline struct {
	Kind     telegramRichInlineKind
	Text     string
	URL      string
	Children []telegramRichInline
}

func renderTelegramRichMessage(message telegramRichMessage) telego.InputRichMessage {
	return telego.InputRichMessage{
		HTML:                renderTelegramRichHTML(message),
		IsRtl:               message.IsRTL,
		SkipEntityDetection: message.SkipEntityDetection,
	}
}

func renderTelegramOutboundRichMessage(content string) telego.InputRichMessage {
	return telego.InputRichMessage{
		HTML: parseContent(content, false),
	}
}

func renderTelegramRichHTML(message telegramRichMessage) string {
	var b strings.Builder
	for _, block := range message.Blocks {
		renderTelegramRichBlock(&b, block)
	}
	return strings.TrimSpace(b.String())
}

func renderTelegramRichBlock(b *strings.Builder, block telegramRichBlock) {
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}

	switch block.Kind {
	case telegramRichBlockHeading:
		level := block.Level
		if level < 1 {
			level = 1
		}
		if level > 6 {
			level = 6
		}
		b.WriteString("<b>")
		b.WriteString(strings.Repeat("#", level))
		b.WriteByte(' ')
		b.WriteString(renderTelegramRichInlines(block.Inlines))
		b.WriteString("</b>")
	case telegramRichBlockParagraph:
		b.WriteString(renderTelegramRichInlines(block.Inlines))
	case telegramRichBlockBulletList:
		for i, item := range block.Items {
			if i > 0 {
				b.WriteByte('\n')
			}
			b.WriteString("• ")
			b.WriteString(renderTelegramRichInlines(item.Inlines))
		}
	case telegramRichBlockNumberedList:
		for i, item := range block.Items {
			if i > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(". ")
			b.WriteString(renderTelegramRichInlines(item.Inlines))
		}
	case telegramRichBlockCode:
		b.WriteString(renderTelegramRichCodeBlock(block.Code, block.Language))
	case telegramRichBlockQuote:
		renderTelegramRichQuote(b, renderTelegramRichInlines(block.Inlines))
	case telegramRichBlockDivider:
		b.WriteString("-----")
	}
}

func renderTelegramRichQuote(b *strings.Builder, text string) {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("&gt; ")
		b.WriteString(line)
	}
}

func renderTelegramRichInlines(inlines []telegramRichInline) string {
	var b strings.Builder
	for _, inline := range inlines {
		b.WriteString(renderTelegramRichInline(inline))
	}
	return b.String()
}

func renderTelegramRichInline(inline telegramRichInline) string {
	content := inline.Text
	if len(inline.Children) > 0 {
		content = renderTelegramRichInlines(inline.Children)
	} else if inline.Kind != telegramRichInlineCode {
		content = escapeHTML(content)
	}

	switch inline.Kind {
	case telegramRichInlineBold:
		return "<b>" + content + "</b>"
	case telegramRichInlineItalic:
		return "<i>" + content + "</i>"
	case telegramRichInlineUnderline:
		return "<u>" + content + "</u>"
	case telegramRichInlineStrike:
		return "<s>" + content + "</s>"
	case telegramRichInlineCode:
		return renderTelegramRichInlineCode(inline.Text)
	case telegramRichInlineLink:
		if !telegramRichLinkURLAllowed(inline.URL) {
			return content
		}
		return `<a href="` + escapeHTMLAttr(inline.URL) + `">` + content + "</a>"
	default:
		return content
	}
}

func renderTelegramRichInlineCode(text string) string {
	return "<code>" + escapeHTML(text) + "</code>"
}

func renderTelegramRichCodeBlock(code, language string) string {
	var b strings.Builder
	b.WriteString("<pre><code>")
	b.WriteString(escapeHTML(code))
	b.WriteString("</code></pre>")
	return b.String()
}

func telegramRichLinkURLAllowed(raw string) bool {
	if strings.ContainsAny(raw, " \t\r\n") {
		return false
	}
	return strings.HasPrefix(raw, "https://") ||
		strings.HasPrefix(raw, "http://") ||
		strings.HasPrefix(raw, "mailto:") ||
		strings.HasPrefix(raw, "tel:") ||
		strings.HasPrefix(raw, "tg://")
}
