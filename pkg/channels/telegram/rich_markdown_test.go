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
			name: "normalizes line endings and trims outer whitespace",
			in:   " \r\n# Title\r\n\r\n- one\r\n ",
			want: "# Title\n\n- one",
		},
		{
			name: "preserves common markdown blocks",
			in:   "# Title\n\n## Section\n\n> quote\n\n- one\n- two\n\n| A | B |\n|---|---|\n| x | y |",
			want: "# Title\n\n## Section\n\n> quote\n\n- one\n- two\n\n| A | B |\n|---|---|\n| x | y |",
		},
		{
			name: "preserves code blocks verbatim",
			in:   "````markdown\nbefore\n```\n<b>still code</b>\n````\n\n`~raw~`",
			want: "````markdown\nbefore\n```\n<b>still code</b>\n````\n\n`~raw~`",
		},
		{
			name: "preserves telegram rich markdown features",
			in:   "<!-- separator -->\n==marked text== ||spoiler|| $x^2 + y^2$\n\n---\n\n- [ ] task\n- [x] done\n\n<u>underlined</u> <sup>sup</sup> <sub>sub</sub> <tg-spoiler>hidden</tg-spoiler>\n\n<details open><summary>Summary with **bold**</summary>\n\n### Details heading\n- item\n\n</details>\n\n<tg-collage>\n\n![](https://telegram.org/example/photo.jpg)\n\n</tg-collage>",
			want: "<!-- separator -->\n==marked text== ||spoiler|| $x^2 + y^2$\n\n---\n\n- [ ] task\n- [x] done\n\n<u>underlined</u> <sup>sup</sup> <sub>sub</sub> <tg-spoiler>hidden</tg-spoiler>\n\n<details open><summary>Summary with **bold**</summary>\n\n### Details heading\n- item\n\n</details>\n\n<tg-collage>\n\n![](https://telegram.org/example/photo.jpg)\n\n</tg-collage>",
		},
		{
			name: "preserves html-looking content instead of translating it",
			in:   `<a href="https://example.com"><code>x</code> <b>bold</b></a>` + "\n<blockquote><b>hi</b></blockquote>",
			want: `<a href="https://example.com"><code>x</code> <b>bold</b></a>` + "\n<blockquote><b>hi</b></blockquote>",
		},
		{
			name: "does not normalize single tilde strike",
			in:   "before ~gone~ after",
			want: "before ~gone~ after",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, markdownToTelegramRichMarkdown(tc.in))
		})
	}
}
