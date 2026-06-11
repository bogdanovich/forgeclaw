package telegram

import (
	"context"
	"testing"
	"time"

	"github.com/mymmrac/telego"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestHandleMessage_TopicFilterSuppressesWrongForumTopic(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{MentionOnly: false}),
		),
		bot:     newTestTelegramBot(t, "testbot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
		tgCfg: &config.TelegramSettings{
			AllowedTopicIDs: []string{"3565"},
		},
	}

	msg := &telego.Message{
		Text:            "/help",
		MessageID:       42,
		MessageThreadID: 6,
		Chat: telego.Chat{
			ID:      -1003942574786,
			Type:    "supergroup",
			IsForum: true,
		},
		From: &telego.User{
			ID:        2490846,
			FirstName: "Anton",
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		t.Fatalf("expected topic-filtered message to be ignored, got inbound content %q", inbound.Content)
	case <-ctx.Done():
	}
}
