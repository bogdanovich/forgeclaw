package telegram

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkdownToTelegramRichMarkdown(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "keeps common markdown blocks",
			in:   "# Title\n\n## Section\n\n### Subsection\n\n> quote\n\n- one\n- two\n\n| A | B |\n|---|---|\n| x | y |",
			want: "# Title\n\n## Section\n\n### Subsection\n\n> quote\n\n- one\n- two\n\n| A | B |\n|---|---|\n| x | y |",
		},
		{
			name: "keeps all rich markdown headings",
			in:   "#### H4\n##### H5\n###### H6",
			want: "#### H4\n##### H5\n###### H6",
		},
		{
			name: "preserves rich inline tags and converts html links",
			in:   `<b>bold</b> <i>ital</i> <s>gone</s> <code>x := 1</code> <a href="https://example.com">site</a>`,
			want: `<b>bold</b> <i>ital</i> <s>gone</s> <code>x := 1</code> [site](https://example.com)`,
		},
		{
			name: "escapes generated rich markdown links",
			in:   `<a href="https://example.com/search?q=a)&amp;ok=1">docs [beta]</a>`,
			want: `[docs \[beta\]](https://example.com/search?q=a\)&ok=1)`,
		},
		{
			name: "converts html blockquote",
			in:   "Intro\n<blockquote>first\nsecond</blockquote>",
			want: "Intro\n> first\n> second",
		},
		{
			name: "preserves rich markdown image and media blocks",
			in:   "![diagram](https://example.com/a.png)",
			want: "![diagram](https://example.com/a.png)",
		},
		{
			name: "converts single tilde strike",
			in:   "before ~gone~ after",
			want: "before ~~gone~~ after",
		},
		{
			name: "does not rewrite code",
			in:   "```html\n<b>raw</b>\n```\n\n`~raw~`",
			want: "```html\n<b>raw</b>\n```\n\n`~raw~`",
		},
		{
			name: "normalizes double backtick inline code",
			in:   "``double backticks``",
			want: "`double backticks`",
		},
		{
			name: "preserves markdown markers inside inline code",
			in:   "`code with **bold** inside`\n`code with *italic* inside`",
			want: "`code with **bold** inside`\n`code with *italic* inside`",
		},
		{
			name: "preserves telegram rich markdown features",
			in:   "==marked text== ||spoiler|| $x^2 + y^2$\n\n---\n\n- [ ] task\n- [x] done\n\n<u>underlined</u> <sup>sup</sup> <sub>sub</sub> <tg-spoiler>hidden</tg-spoiler>\n\n<details open><summary>Summary with **bold**</summary>\n\n### Details heading\n- item\n\n</details>\n\n<tg-collage>\n\n![](https://telegram.org/example/photo.jpg)\n\n</tg-collage>",
			want: "==marked text== ||spoiler|| $x^2 + y^2$\n\n---\n\n- [ ] task\n- [x] done\n\n<u>underlined</u> <sup>sup</sup> <sub>sub</sub> <tg-spoiler>hidden</tg-spoiler>\n\n<details open><summary>Summary with **bold**</summary>\n\n### Details heading\n- item\n\n</details>\n\n<tg-collage>\n\n![](https://telegram.org/example/photo.jpg)\n\n</tg-collage>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, markdownToTelegramRichMarkdown(tc.in))
		})
	}
}
