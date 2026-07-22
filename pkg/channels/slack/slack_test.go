package slack

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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
				t.Errorf(
					"parseSlackChatID(%q) channelID = %q, want %q",
					tt.chatID,
					chanID,
					tt.wantChanID,
				)
			}
			if threadTS != tt.wantThread {
				t.Errorf(
					"parseSlackChatID(%q) threadTS = %q, want %q",
					tt.chatID,
					threadTS,
					tt.wantThread,
				)
			}
		})
	}
}

func TestResolveSlackOutboundTarget_PrefersContextTopicID(t *testing.T) {
	deliveryChatID, channelID, threadTS := resolveSlackOutboundTarget(
		"C123456",
		&bus.InboundContext{
			Channel: "slack",
			ChatID:  "C123456",
			TopicID: "1234567890.123456",
		},
	)

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

func TestSlackToolFeedbackChatKey_FallsBackToReplyToMessageID(t *testing.T) {
	got := slackToolFeedbackChatKey("C123456", &bus.InboundContext{
		Channel:          "slack",
		ChatID:           "C123456",
		ReplyToMessageID: "1234567890.123456",
	})
	if got != "C123456/1234567890.123456" {
		t.Fatalf("slackToolFeedbackChatKey() = %q, want %q", got, "C123456/1234567890.123456")
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
	if ch.markInboundEventHandled("message", "C123456", "1780730899.262309") {
		t.Fatal("first inbound event should not be treated as duplicate")
	}
	if !ch.markInboundEventHandled("message", "C123456", "1780730899.262309") {
		t.Fatal("second inbound event with same channel/timestamp should be deduplicated")
	}
	if ch.markInboundEventHandled("message", "C123456", "1780730900.000001") {
		t.Fatal("different timestamp should not be treated as duplicate")
	}
	if ch.markInboundEventHandled("message", "C999999", "1780730899.262309") {
		t.Fatal("different channel should not be treated as duplicate")
	}
}

func TestSlackChannelInboundDedupDistinguishesEventKind(t *testing.T) {
	ch := &SlackChannel{}
	if ch.markInboundEventHandled("message", "C123456", "1780730899.262309") {
		t.Fatal("first message event should not be treated as duplicate")
	}
	if ch.markInboundEventHandled("app_mention", "C123456", "1780730899.262309") {
		t.Fatal("app_mention should not be deduplicated against message event")
	}
	if !ch.markInboundEventHandled("app_mention", "C123456", "1780730899.262309") {
		t.Fatal("second app_mention event with same channel/timestamp should be deduplicated")
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

func TestSlackSocketModeLoopReconnectsAfterUnexpectedExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runCalls := make(chan int, 2)
	var calls int
	ch := &SlackChannel{
		BaseChannel: channels.NewBaseChannel(
			"slack",
			&config.SlackSettings{},
			bus.NewMessageBus(),
			nil,
		),
		ctx: ctx,
		runSocketModeFn: func(ctx context.Context) error {
			calls++
			runCalls <- calls
			if calls == 1 {
				return errors.New("socket failed")
			}
			<-ctx.Done()
			return ctx.Err()
		},
		reconnectDelay: func(int) time.Duration {
			return 10 * time.Millisecond
		},
	}
	ch.SetRunning(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.runSocketModeLoop()
	}()

	select {
	case got := <-runCalls:
		if got != 1 {
			t.Fatalf("first run call = %d, want 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first socket run")
	}
	waitForSlackRunning(t, ch, false)

	select {
	case got := <-runCalls:
		if got != 2 {
			t.Fatalf("second run call = %d, want 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reconnect")
	}
	waitForSlackRunning(t, ch, true)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("socket loop did not exit after context cancel")
	}
}

func waitForSlackRunning(t *testing.T, ch *SlackChannel, want bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ch.IsRunning() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Slack running = %v, want %v", ch.IsRunning(), want)
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
		bc := &config.Channel{
			Type:      config.ChannelSlack,
			Enabled:   true,
			AllowFrom: []string{"U_ALLOWED"},
		}
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

func TestSlackChannelSend_ToolFeedbackUsesTransportSend(t *testing.T) {
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
		posted = append(posted, channelID+"|"+threadTS+"|"+text)
		return fmt.Sprintf("msg-%d", len(posted)), nil
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
	toolFeedback.Content = "Media working...\n• tool: `delegate`"
	msgIDs, err = ch.Send(context.Background(), toolFeedback)
	if err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	if !reflect.DeepEqual(msgIDs, []string{"msg-2"}) {
		t.Fatalf("second Send() ids = %v, want [msg-2]", msgIDs)
	}
	if len(posted) != 2 {
		t.Fatalf("posted after second send = %v, want 2 messages", posted)
	}
}

func TestFormatSlackMessage_ConvertsMarkdownToMrkdwn(t *testing.T) {
	input := "Here’s a concise summary:\n\n## Main idea\n**Bold** and *italic* with ~~strike~~.\n- first item\n- second item\n[OpenAI](https://openai.com)\n`inline`"
	want := "Here’s a concise summary:\n\n*Main idea*\n*Bold* and _italic_ with ~strike~.\n• first item\n• second item\n<https://openai.com|OpenAI>\n`inline`"
	if got := formatSlackMessage(input); got != want {
		t.Fatalf("formatSlackMessage() = %q, want %q", got, want)
	}
}

func TestFormatSlackMessage_PreservesLinksWithParentheses(t *testing.T) {
	input := "[docs](https://en.wikipedia.org/wiki/Function_(mathematics))"
	want := "<https://en.wikipedia.org/wiki/Function_(mathematics)|docs>"
	if got := formatSlackMessage(input); got != want {
		t.Fatalf("formatSlackMessage() = %q, want %q", got, want)
	}
}

func TestSlackBlocks_DropsBlocksForLongFencedCode(t *testing.T) {
	ch := &SlackChannel{}
	longCode := "```\n" + strings.Repeat("x", slackMaxTextBlockLength+32) + "\n```"
	if blocks := ch.slackBlocks(longCode); len(blocks) != 0 {
		t.Fatalf("slackBlocks() = %d block(s), want none for long fenced code", len(blocks))
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
