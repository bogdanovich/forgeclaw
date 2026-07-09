package telegram

import (
	"testing"

	"github.com/mymmrac/telego"
	"github.com/stretchr/testify/assert"
)

func TestRenderTelegramRichHTML_ReportShape(t *testing.T) {
	msg := telegramRichMessage{
		Blocks: []telegramRichBlock{
			{
				Kind:  telegramRichBlockHeading,
				Level: 2,
				Inlines: []telegramRichInline{
					{Kind: telegramRichInlineText, Text: "Review Summary"},
				},
			},
			{
				Kind: telegramRichBlockParagraph,
				Inlines: []telegramRichInline{
					{Kind: telegramRichInlineBold, Text: "Status"},
					{Kind: telegramRichInlineText, Text: ": ready"},
				},
			},
			{
				Kind: telegramRichBlockBulletList,
				Items: []telegramRichListItem{
					{
						Inlines: []telegramRichInline{
							{Kind: telegramRichInlineText, Text: "tests pass"},
						},
					},
					{
						Inlines: []telegramRichInline{
							{Kind: telegramRichInlineItalic, Text: "lint pending"},
						},
					},
				},
			},
			{
				Kind: telegramRichBlockNumberedList,
				Items: []telegramRichListItem{
					{Inlines: []telegramRichInline{{Kind: telegramRichInlineText, Text: "merge"}}},
					{Inlines: []telegramRichInline{{Kind: telegramRichInlineText, Text: "deploy"}}},
				},
			},
			{
				Kind: telegramRichBlockQuote,
				Inlines: []telegramRichInline{
					{Kind: telegramRichInlineText, Text: "Keep PRs small."},
				},
			},
			{Kind: telegramRichBlockDivider},
		},
	}

	got := renderTelegramRichHTML(msg)

	want := "<b>## Review Summary</b>\n\n<b>Status</b>: ready\n\n" +
		"• tests pass\n• <i>lint pending</i>\n\n" +
		"1. merge\n2. deploy\n\n" +
		"&gt; Keep PRs small.\n\n-----"
	assert.Equal(t, want, got)
}

func TestRenderTelegramRichHTML_EscapesUserText(t *testing.T) {
	msg := telegramRichMessage{
		Blocks: []telegramRichBlock{{
			Kind: telegramRichBlockParagraph,
			Inlines: []telegramRichInline{
				{
					Kind: telegramRichInlineText,
					Text: "literal *bold* _italic_ [x](https://bad) #1 <tag> & ok",
				},
			},
		}},
	}

	got := renderTelegramRichHTML(msg)

	assert.Equal(t, `literal *bold* _italic_ [x](https://bad) #1 &lt;tag&gt; &amp; ok`, got)
}

func TestRenderTelegramRichHTML_Links(t *testing.T) {
	msg := telegramRichMessage{
		Blocks: []telegramRichBlock{{
			Kind: telegramRichBlockParagraph,
			Inlines: []telegramRichInline{
				{Kind: telegramRichInlineLink, Text: "safe link", URL: "https://example.com/a_(b)"},
				{Kind: telegramRichInlineText, Text: " "},
				{Kind: telegramRichInlineLink, Text: "bad link", URL: "javascript:alert(1)"},
			},
		}},
	}

	got := renderTelegramRichHTML(msg)

	assert.Equal(t, `<a href="https://example.com/a_(b)">safe link</a> bad link`, got)
}

func TestRenderTelegramRichHTML_CodeBlocksPreserveContent(t *testing.T) {
	msg := telegramRichMessage{
		Blocks: []telegramRichBlock{{
			Kind:     telegramRichBlockCode,
			Language: "go+html",
			Code:     "fmt.Println(```)\n",
		}},
	}

	got := renderTelegramRichHTML(msg)

	assert.Equal(t, `<pre><code>fmt.Println(`+"```"+`)
</code></pre>`, got)
}

func TestRenderTelegramRichMessage_OutputUsesHTMLRichMessage(t *testing.T) {
	msg := telegramRichMessage{
		SkipEntityDetection: true,
		IsRTL:               true,
		Blocks: []telegramRichBlock{{
			Kind: telegramRichBlockParagraph,
			Inlines: []telegramRichInline{
				{Kind: telegramRichInlineUnderline, Text: "under"},
				{Kind: telegramRichInlineText, Text: " "},
				{Kind: telegramRichInlineStrike, Text: "gone"},
				{Kind: telegramRichInlineText, Text: " "},
				{Kind: telegramRichInlineCode, Text: "x := `y`"},
			},
		}},
	}

	got := renderTelegramRichMessage(msg)

	assert.Equal(t, telego.InputRichMessage{
		HTML:                "<u>under</u> <s>gone</s> <code>x := `y`</code>",
		IsRtl:               true,
		SkipEntityDetection: true,
	}, got)
}

func TestRenderTelegramOutboundRichMessage_UsesTelegramRichMarkdown(t *testing.T) {
	content := "Hello **world**\n\n> quote\n\n- one\n- two\n\n~gone~"
	got := renderTelegramOutboundRichMessage(content)

	assert.Equal(t, telego.InputRichMessage{
		Markdown: "Hello **world**\n\n> quote\n\n- one\n- two\n\n~gone~",
	}, got)
}
