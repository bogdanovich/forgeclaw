package slack

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	slacksdk "github.com/slack-go/slack"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

func TestParseSlackChatID(t *testing.T) {
	tests := []struct {
		name       string
		chatID     string
		wantChanID string
		wantThread string
	}{
		{
			name:       "channel only",
			chatID:     "C123456",
			wantChanID: "C123456",
			wantThread: "",
		},
		{
			name:       "channel with thread",
			chatID:     "C123456/1234567890.123456",
			wantChanID: "C123456",
			wantThread: "1234567890.123456",
		},
		{
			name:       "DM channel",
			chatID:     "D987654",
			wantChanID: "D987654",
			wantThread: "",
		},
		{
			name:       "empty string",
			chatID:     "",
			wantChanID: "",
			wantThread: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chanID, threadTS := parseSlackChatID(tt.chatID)
			if chanID != tt.wantChanID {
				t.Errorf("parseSlackChatID(%q) channelID = %q, want %q", tt.chatID, chanID, tt.wantChanID)
			}
			if threadTS != tt.wantThread {
				t.Errorf("parseSlackChatID(%q) threadTS = %q, want %q", tt.chatID, threadTS, tt.wantThread)
			}
		})
	}
}

func TestResolveSlackOutboundTarget_PrefersContextTopicID(t *testing.T) {
	deliveryChatID, channelID, threadTS := resolveSlackOutboundTarget("C123456", &bus.InboundContext{
		Channel: "slack",
		ChatID:  "C123456",
		TopicID: "1234567890.123456",
	})

	if deliveryChatID != "C123456/1234567890.123456" {
		t.Fatalf("deliveryChatID = %q, want %q", deliveryChatID, "C123456/1234567890.123456")
	}
	if channelID != "C123456" {
		t.Fatalf("channelID = %q, want %q", channelID, "C123456")
	}
	if threadTS != "1234567890.123456" {
		t.Fatalf("threadTS = %q, want %q", threadTS, "1234567890.123456")
	}
}

func TestStripBotMention(t *testing.T) {
	ch := &SlackChannel{botUserID: "U12345BOT"}

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "mention at start",
			input: "<@U12345BOT> hello there",
			want:  "hello there",
		},
		{
			name:  "mention in middle",
			input: "hey <@U12345BOT> can you help",
			want:  "hey  can you help",
		},
		{
			name:  "no mention",
			input: "hello world",
			want:  "hello world",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "only mention",
			input: "<@U12345BOT>",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ch.stripBotMention(tt.input)
			if got != tt.want {
				t.Errorf("stripBotMention(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSlackChannelInboundDedupByChannelAndTimestamp(t *testing.T) {
	ch := &SlackChannel{}
	if ch.markInboundEventHandled("C123456", "1780730899.262309") {
		t.Fatal("first inbound event should not be treated as duplicate")
	}
	if !ch.markInboundEventHandled("C123456", "1780730899.262309") {
		t.Fatal("second inbound event with same channel/timestamp should be deduplicated")
	}
	if ch.markInboundEventHandled("C123456", "1780730900.000001") {
		t.Fatal("different timestamp should not be treated as duplicate")
	}
	if ch.markInboundEventHandled("C999999", "1780730899.262309") {
		t.Fatal("different channel should not be treated as duplicate")
	}
}

func TestNewSlackChannel(t *testing.T) {
	msgBus := bus.NewMessageBus()
	bc := &config.Channel{Type: "slack", Enabled: true}

	t.Run("missing bot token", func(t *testing.T) {
		cfg := &config.SlackSettings{}
		cfg.AppToken = *config.NewSecureString("xapp-test")
		_, err := NewSlackChannel(bc, cfg, msgBus)
		if err == nil {
			t.Error("expected error for missing bot_token, got nil")
		}
	})

	t.Run("missing app token", func(t *testing.T) {
		cfg := &config.SlackSettings{}
		cfg.BotToken = *config.NewSecureString("xoxb-test")
		_, err := NewSlackChannel(bc, cfg, msgBus)
		if err == nil {
			t.Error("expected error for missing app_token, got nil")
		}
	})

	t.Run("valid config", func(t *testing.T) {
		cfg := &config.SlackSettings{}
		cfg.BotToken = *config.NewSecureString("xoxb-test")
		cfg.AppToken = *config.NewSecureString("xapp-test")
		bc := &config.Channel{Type: "slack", Enabled: true, AllowFrom: []string{"U123"}}
		ch, err := NewSlackChannel(bc, cfg, msgBus)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ch.Name() != "slack" {
			t.Errorf("Name() = %q, want %q", ch.Name(), "slack")
		}
		if ch.IsRunning() {
			t.Error("new channel should not be running")
		}
	})
}

func TestSlackChannelIsAllowed(t *testing.T) {
	msgBus := bus.NewMessageBus()

	t.Run("empty allowlist allows all", func(t *testing.T) {
		bc := &config.Channel{Type: config.ChannelSlack, Enabled: true, AllowFrom: []string{}}
		cfg := &config.SlackSettings{}
		cfg.BotToken = *config.NewSecureString("xoxb-test")
		cfg.AppToken = *config.NewSecureString("xapp-test")
		ch, _ := NewSlackChannel(bc, cfg, msgBus)
		if !ch.IsAllowed("U_ANYONE") {
			t.Error("empty allowlist should allow all users")
		}
	})

	t.Run("allowlist restricts users", func(t *testing.T) {
		bc := &config.Channel{Type: config.ChannelSlack, Enabled: true, AllowFrom: []string{"U_ALLOWED"}}
		cfg := &config.SlackSettings{}
		cfg.BotToken = *config.NewSecureString("xoxb-test")
		cfg.AppToken = *config.NewSecureString("xapp-test")
		ch, _ := NewSlackChannel(bc, cfg, msgBus)
		if !ch.IsAllowed("U_ALLOWED") {
			t.Error("allowed user should pass allowlist check")
		}
		if ch.IsAllowed("U_BLOCKED") {
			t.Error("non-allowed user should be blocked")
		}
	})
}

func TestSlackChannelShouldProcessChannel(t *testing.T) {
	ch := &SlackChannel{
		config: &config.SlackSettings{
			AllowedChannelIDs: []string{"C_REVIEW"},
			IgnoredChannelIDs: []string{"C_IGNORE"},
		},
	}

	if !ch.shouldProcessChannel("C_REVIEW") {
		t.Fatal("allowed channel should be processed")
	}
	if ch.shouldProcessChannel("C_OTHER") {
		t.Fatal("channel outside allowed_channel_ids should be ignored")
	}
	if ch.shouldProcessChannel("C_IGNORE") {
		t.Fatal("ignored_channel_ids should override processing")
	}
}

func TestSendMedia_SendsCaptionFallbackAfterUploads(t *testing.T) {
	ch := &SlackChannel{
		BaseChannel: channels.NewBaseChannel("slack", nil, nil, nil),
	}
	ch.SetRunning(true)

	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "report.txt")
	if err := os.WriteFile(localPath, []byte("attachment body"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:    "report.txt",
		ContentType: "text/plain",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	var uploaded []slackUploadRecord
	var posted []string
	ch.uploadFileFn = func(ctx context.Context, params slacksdk.UploadFileParameters) error {
		uploaded = append(uploaded, slackUploadRecord{
			Channel: params.Channel,
			Thread:  params.ThreadTimestamp,
			File:    params.File,
			Size:    params.FileSize,
			Name:    params.Filename,
			Title:   params.Title,
		})
		return nil
	}
	ch.postTextFn = func(ctx context.Context, channelID, threadTS, text string) error {
		posted = append(posted, channelID+"|"+threadTS+"|"+text)
		return nil
	}

	_, err = ch.SendMedia(context.Background(), bus.OutboundMediaMessage{
		ChatID: "C123456/1234567890.123456",
		Parts: []bus.MediaPart{{
			Ref:         ref,
			Type:        "file",
			Filename:    "report.txt",
			ContentType: "text/plain",
			Caption:     "shared caption",
		}},
	})
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}
	if len(uploaded) != 1 {
		t.Fatalf("uploads = %v, want 1 upload", uploaded)
	}
	if uploaded[0].Title != "shared caption" {
		t.Fatalf("upload title = %q, want shared caption", uploaded[0].Title)
	}
	if uploaded[0].Size != len([]byte("attachment body")) {
		t.Fatalf("upload size = %d, want %d", uploaded[0].Size, len([]byte("attachment body")))
	}
	if len(posted) != 1 || posted[0] != "C123456|1234567890.123456|shared caption" {
		t.Fatalf("posted = %v, want fallback text in same thread", posted)
	}
}

func TestSlackChannelSend_ToolFeedbackUpdatesTrackedMessage(t *testing.T) {
	msgBus := bus.NewMessageBus()
	cfg := &config.SlackSettings{}
	cfg.BotToken = *config.NewSecureString("xoxb-test")
	cfg.AppToken = *config.NewSecureString("xapp-test")
	bc := &config.Channel{Type: "slack", Enabled: true}

	ch, err := NewSlackChannel(bc, cfg, msgBus)
	if err != nil {
		t.Fatalf("NewSlackChannel() error = %v", err)
	}
	ch.SetRunning(true)

	var posted []string
	var edited []string
	ch.postMessageFn = func(_ context.Context, channelID, threadTS, text string) (string, error) {
		posted = append(posted, channelID+"|"+threadTS+"|"+text)
		return "msg-1", nil
	}
	ch.editMessageFn = func(_ context.Context, channelID, messageID, text string) error {
		edited = append(edited, channelID+"|"+messageID+"|"+text)
		return nil
	}

	toolFeedback := bus.OutboundMessage{
		ChatID:  "C123456",
		Content: "Working...\n• tool: `read_file` — `README.md`",
		Context: bus.InboundContext{Raw: map[string]string{
			"message_kind": "tool_feedback",
		}},
	}

	msgIDs, err := ch.Send(context.Background(), toolFeedback)
	if err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if !reflect.DeepEqual(msgIDs, []string{"msg-1"}) {
		t.Fatalf("first Send() ids = %v, want [msg-1]", msgIDs)
	}
	if len(posted) != 1 {
		t.Fatalf("posted = %v, want 1 message", posted)
	}
	if len(edited) != 0 {
		t.Fatalf("edited = %v, want no edits on first send", edited)
	}

	toolFeedback.Content = "Media working...\n• tool: `delegate`"
	msgIDs, err = ch.Send(context.Background(), toolFeedback)
	if err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	if !reflect.DeepEqual(msgIDs, []string{"msg-1"}) {
		t.Fatalf("second Send() ids = %v, want [msg-1]", msgIDs)
	}
	if len(posted) != 1 {
		t.Fatalf("posted after update = %v, want still 1 message", posted)
	}
	if !reflect.DeepEqual(edited, []string{"C123456|msg-1|Media working...\n• tool: `read_file` — `README.md`\n• tool: `delegate`"}) {
		t.Fatalf("edited = %v, want merged tracked message edit", edited)
	}
}

func TestSlackChannelFinalizeToolFeedbackMessage_EditsTrackedMessage(t *testing.T) {
	msgBus := bus.NewMessageBus()
	cfg := &config.SlackSettings{}
	cfg.BotToken = *config.NewSecureString("xoxb-test")
	cfg.AppToken = *config.NewSecureString("xapp-test")
	bc := &config.Channel{Type: "slack", Enabled: true}

	ch, err := NewSlackChannel(bc, cfg, msgBus)
	if err != nil {
		t.Fatalf("NewSlackChannel() error = %v", err)
	}

	ch.RecordToolFeedbackMessage("C123456/1234567890.123456#session:s1", "msg-progress", "Working...")

	var edited []string
	ch.editMessageFn = func(_ context.Context, channelID, messageID, text string) error {
		edited = append(edited, channelID+"|"+messageID+"|"+text)
		return nil
	}

	msgIDs, handled := ch.FinalizeToolFeedbackMessage(context.Background(), bus.OutboundMessage{
		ChatID:     "C123456",
		Content:    "Final answer",
		SessionKey: "s1",
		Context:    bus.InboundContext{ChatID: "C123456", TopicID: "1234567890.123456"},
	})
	if !handled {
		t.Fatal("FinalizeToolFeedbackMessage() handled = false, want true")
	}
	if !reflect.DeepEqual(msgIDs, []string{"msg-progress"}) {
		t.Fatalf("FinalizeToolFeedbackMessage() ids = %v, want [msg-progress]", msgIDs)
	}
	if !reflect.DeepEqual(edited, []string{"C123456|msg-progress|Final answer"}) {
		t.Fatalf("edited = %v, want final edit on tracked message", edited)
	}
}

func TestFormatSlackMessage_ConvertsMarkdownToMrkdwn(t *testing.T) {
	input := "Here’s a concise summary:\n\n## Main idea\n**Bold** and *italic* with ~~strike~~.\n- first item\n- second item\n[OpenAI](https://openai.com)\n`inline`"
	want := "Here’s a concise summary:\n\n*Main idea*\n*Bold* and _italic_ with ~strike~.\n• first item\n• second item\n<https://openai.com|OpenAI>\n`inline`"
	if got := formatSlackMessage(input); got != want {
		t.Fatalf("formatSlackMessage() = %q, want %q", got, want)
	}
}

func TestFormatSlackMessage_BeautifiesPlainSectionHeadings(t *testing.T) {
	input := "Summary:\n\nMain points:\n• one\n• two\n\nBig takeaway:\nDone.\n\nIn one sentence:\nShort."
	want := "📝 *Summary:*\n\n💡 *Main points:*\n• one\n• two\n\n🔑 *Big takeaway:*\nDone.\n\n📌 *In one sentence:*\nShort."
	if got := formatSlackMessage(input); got != want {
		t.Fatalf("formatSlackMessage() = %q, want %q", got, want)
	}
}

func TestSlackChannelSend_FormatsFinalMessageForSlack(t *testing.T) {
	msgBus := bus.NewMessageBus()
	cfg := &config.SlackSettings{}
	cfg.BotToken = *config.NewSecureString("xoxb-test")
	cfg.AppToken = *config.NewSecureString("xapp-test")
	bc := &config.Channel{Type: "slack", Enabled: true}

	ch, err := NewSlackChannel(bc, cfg, msgBus)
	if err != nil {
		t.Fatalf("NewSlackChannel() error = %v", err)
	}
	ch.SetRunning(true)

	var posted []string
	ch.postMessageFn = func(_ context.Context, channelID, threadTS, text string) (string, error) {
		posted = append(posted, channelID+"|"+threadTS+"|"+formatSlackMessage(text))
		return "msg-1", nil
	}

	msgIDs, err := ch.Send(context.Background(), bus.OutboundMessage{
		ChatID:  "C123456",
		Content: "## Main idea\n**Bold**\n- item\n[link](https://example.com)",
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if !reflect.DeepEqual(msgIDs, []string{"msg-1"}) {
		t.Fatalf("Send() ids = %v, want [msg-1]", msgIDs)
	}
	want := []string{"C123456||*Main idea*\n*Bold*\n• item\n<https://example.com|link>"}
	if !reflect.DeepEqual(posted, want) {
		t.Fatalf("posted = %v, want %v", posted, want)
	}
}

type slackUploadRecord struct {
	Channel string
	Thread  string
	File    string
	Size    int
	Name    string
	Title   string
}
