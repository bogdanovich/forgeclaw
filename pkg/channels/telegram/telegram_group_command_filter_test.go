package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mymmrac/telego"
	ta "github.com/mymmrac/telego/telegoapi"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

type getMeCaller struct {
	username string
}

func (c getMeCaller) Call(_ context.Context, url string, _ *ta.RequestData) (*ta.Response, error) {
	if strings.HasSuffix(url, "/getMe") {
		result := fmt.Sprintf(`{"id":1,"is_bot":true,"first_name":"bot","username":%q}`, c.username)
		return &ta.Response{Ok: true, Result: []byte(result)}, nil
	}
	return &ta.Response{Ok: true, Result: []byte("true")}, nil
}

type flakyGetMeCaller struct {
	mu       sync.Mutex
	calls    int
	username string
	firstErr error
	botID    int64
}

func (c *flakyGetMeCaller) Call(_ context.Context, url string, _ *ta.RequestData) (*ta.Response, error) {
	if !strings.HasSuffix(url, "/getMe") {
		return &ta.Response{Ok: true, Result: []byte("true")}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == 1 && c.firstErr != nil {
		return nil, c.firstErr
	}

	result := fmt.Sprintf(`{"id":%d,"is_bot":true,"first_name":"bot","username":%q}`, c.botID, c.username)
	return &ta.Response{Ok: true, Result: []byte(result)}, nil
}

func newTestTelegramBot(t *testing.T, username string) *telego.Bot {
	t.Helper()

	token := "123456:" + strings.Repeat("a", 35)
	bot, err := telego.NewBot(token,
		telego.WithAPICaller(getMeCaller{username: username}),
		telego.WithDiscardLogger(),
	)
	if err != nil {
		t.Fatalf("NewBot error: %v", err)
	}
	return bot
}

func newFlakyTestTelegramBot(t *testing.T, caller *flakyGetMeCaller) *telego.Bot {
	t.Helper()

	token := "123456:" + strings.Repeat("a", 35)
	bot, err := telego.NewBot(token,
		telego.WithAPICaller(caller),
		telego.WithDiscardLogger(),
	)
	if err != nil {
		t.Fatalf("NewBot error: %v", err)
	}
	return bot
}

func newGroupMentionOnlyChannel(t *testing.T, botUsername string) (*TelegramChannel, *bus.MessageBus) {
	t.Helper()

	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel("telegram", nil, messageBus, nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{MentionOnly: true}),
		),
		bot:     newTestTelegramBot(t, botUsername),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}
	return ch, messageBus
}

func TestHandleMessage_GroupMentionOnly_BotCommandEntity(t *testing.T) {
	tests := []struct {
		name          string
		text          string
		wantForwarded bool
		wantContent   string
	}{
		{
			name:          "command with bot username",
			text:          "/new@testbot",
			wantForwarded: true,
			wantContent:   "/new",
		},
		{
			name:          "bare command",
			text:          "/new",
			wantForwarded: true,
			wantContent:   "/new",
		},
		{
			name:          "command for another bot",
			text:          "/new@otherbot",
			wantForwarded: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch, messageBus := newGroupMentionOnlyChannel(t, "testbot")

			msg := &telego.Message{
				Text: tc.text,
				Entities: []telego.MessageEntity{{
					Type:   telego.EntityTypeBotCommand,
					Offset: 0,
					Length: len([]rune(tc.text)),
				}},
				MessageID: 42,
				Chat: telego.Chat{
					ID:   123,
					Type: "group",
				},
				From: &telego.User{
					ID:        7,
					FirstName: "Alice",
				},
			}

			if err := ch.handleMessage(context.Background(), msg); err != nil {
				t.Fatalf("handleMessage error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			select {
			case <-ctx.Done():
				if tc.wantForwarded {
					t.Fatal("timeout waiting for message to be forwarded")
					return
				}
			case inbound, ok := <-messageBus.InboundChan():
				if tc.wantForwarded {
					if !ok {
						t.Fatal("expected inbound message to be forwarded")
					}
					if inbound.Content != tc.wantContent {
						t.Fatalf("content=%q want=%q", inbound.Content, tc.wantContent)
					}
					return
				}
			}
		})
	}
}

func TestIsBotMentioned_MentionEntityUnaffected(t *testing.T) {
	ch, _ := newGroupMentionOnlyChannel(t, "testbot")

	msg := &telego.Message{
		Text: "@testbot hello",
		Entities: []telego.MessageEntity{{
			Type:   telego.EntityTypeMention,
			Offset: 0,
			Length: len("@testbot"),
		}},
	}

	if !ch.isBotMentioned(msg) {
		t.Fatal("expected mention entity to be treated as bot mention")
	}
}

func TestIsBotMentioned_RecoversFromPoisonedTelegoCache(t *testing.T) {
	caller := &flakyGetMeCaller{
		username: "FamilyJournalBot",
		firstErr: errors.New("temporary getMe failure"),
		botID:    8642110061,
	}
	bot := newFlakyTestTelegramBot(t, caller)

	if got := bot.Username(); got != "" {
		t.Fatalf("Username()=%q want empty after first getMe failure", got)
	}

	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel("telegram", nil, bus.NewMessageBus(), nil),
		bot:         bot,
		chatIDs:     make(map[string]int64),
		ctx:         context.Background(),
	}

	msg := &telego.Message{
		Text: "@FamilyJournalBot ответь",
		Entities: []telego.MessageEntity{{
			Type:   telego.EntityTypeMention,
			Offset: 0,
			Length: len("@FamilyJournalBot"),
		}},
	}

	if !ch.isBotMentioned(msg) {
		t.Fatal("expected bot mention to recover via direct getMe lookup")
	}
}

func TestIsReplyToNonBotMessage_TreatsOwnBotReplyAsBotAfterRecovery(t *testing.T) {
	caller := &flakyGetMeCaller{
		username: "FamilyJournalBot",
		firstErr: errors.New("temporary getMe failure"),
		botID:    8642110061,
	}
	bot := newFlakyTestTelegramBot(t, caller)

	if got := bot.Username(); got != "" {
		t.Fatalf("Username()=%q want empty after first getMe failure", got)
	}

	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel("telegram", nil, bus.NewMessageBus(), nil),
		bot:         bot,
		chatIDs:     make(map[string]int64),
		ctx:         context.Background(),
	}

	msg := &telego.Message{
		ReplyToMessage: &telego.Message{
			From: &telego.User{
				ID:        8642110061,
				IsBot:     true,
				Username:  "FamilyJournalBot",
				FirstName: "Family",
			},
		},
	}

	if ch.isReplyToNonBotMessage(msg) {
		t.Fatal("expected reply to own bot message not to be treated as non-bot reply")
	}
}

func TestHandleMessage_GroupTopicIgnoresHumanMentionWithoutBotMention(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly: false,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	text := "@AntonBogdanovich help"
	msg := &telego.Message{
		Text: text,
		Entities: []telego.MessageEntity{{
			Type:   telego.EntityTypeMention,
			Offset: 0,
			Length: len("@AntonBogdanovich"),
		}},
		MessageID:       43,
		MessageThreadID: 1771,
		Chat: telego.Chat{
			ID:      -1002133645926,
			Type:    "supergroup",
			IsForum: true,
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		t.Fatalf("expected human-only mention to be ignored, got inbound content %q", inbound.Content)
	case <-ctx.Done():
	}

	ctxObserved, cancelObserved := context.WithTimeout(context.Background(), time.Second)
	defer cancelObserved()
	select {
	case observed := <-messageBus.ObservedChan():
		if observed.Reason == "" {
			t.Fatal("expected observed reason")
		}
		if observed.Content != text {
			t.Fatalf("observed content=%q want %q", observed.Content, text)
		}
	case <-ctxObserved.Done():
		t.Fatal("timeout waiting for observed human-only mention")
	}
}

func TestHandleMessage_GroupTopicAllowsBotMentionWithHumanMention(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly: false,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	text := "@KityaBot помоги @AntonBogdanovich"
	msg := &telego.Message{
		Text: text,
		Entities: []telego.MessageEntity{
			{
				Type:   telego.EntityTypeMention,
				Offset: 0,
				Length: len("@KityaBot"),
			},
			{
				Type:   telego.EntityTypeMention,
				Offset: len("@KityaBot помоги "),
				Length: len("@AntonBogdanovich"),
			},
		},
		MessageID:       44,
		MessageThreadID: 1771,
		Chat: telego.Chat{
			ID:      -1002133645926,
			Type:    "supergroup",
			IsForum: true,
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		if inbound.Content != "помоги @AntonBogdanovich" {
			t.Fatalf("content=%q want %q", inbound.Content, "помоги @AntonBogdanovich")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
	}
}

func TestHandleMessage_GroupTopicCanDisableNonBotMentionGuard(t *testing.T) {
	ignoreNonBotMentions := false
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly:          false,
				IgnoreNonBotMentions: &ignoreNonBotMentions,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	text := "@AntonBogdanovich help"
	msg := &telego.Message{
		Text: text,
		Entities: []telego.MessageEntity{{
			Type:   telego.EntityTypeMention,
			Offset: 0,
			Length: len("@AntonBogdanovich"),
		}},
		MessageID: 45,
		Chat: telego.Chat{
			ID:   -1002133645926,
			Type: "supergroup",
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		if inbound.Content != text {
			t.Fatalf("content=%q want %q", inbound.Content, text)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
	}
}

func TestHandleMessage_GroupTopicIgnoresReplyToHumanWithoutBotMention(t *testing.T) {
	ignoreNonBotReplies := true
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly:         false,
				IgnoreNonBotReplies: &ignoreNonBotReplies,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	msg := &telego.Message{
		Text:      "Тебе понравился рецепт?",
		MessageID: 46,
		Chat: telego.Chat{
			ID:   -1002133645926,
			Type: "supergroup",
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
		ReplyToMessage: &telego.Message{
			MessageID: 45,
			Text:      "Ок это отдельно другой рецепт",
			From: &telego.User{
				ID:        123,
				FirstName: "Anton",
				Username:  "AntonBogdanovich",
			},
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		t.Fatalf("expected reply to human to be ignored, got inbound content %q", inbound.Content)
	case <-ctx.Done():
	}

	ctxObserved, cancelObserved := context.WithTimeout(context.Background(), time.Second)
	defer cancelObserved()
	select {
	case observed := <-messageBus.ObservedChan():
		if observed.Reason == "" {
			t.Fatal("expected observed reason")
		}
		if !strings.Contains(
			observed.Content,
			"[quoted user message from AntonBogdanovich]: Ок это отдельно другой рецепт",
		) {
			t.Fatalf("observed content should include quoted human context, got %q", observed.Content)
		}
		if !strings.Contains(observed.Content, "Тебе понравился рецепт?") {
			t.Fatalf("observed content should include reply text, got %q", observed.Content)
		}
	case <-ctxObserved.Done():
		t.Fatal("timeout waiting for observed reply")
	}
}

func TestHandleMessage_GroupTopicAllowsReplyToBotWithReplyGuard(t *testing.T) {
	ignoreNonBotReplies := true
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly:         false,
				IgnoreNonBotReplies: &ignoreNonBotReplies,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	msg := &telego.Message{
		Text:      "а подробнее?",
		MessageID: 47,
		Chat: telego.Chat{
			ID:   -1002133645926,
			Type: "supergroup",
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
		ReplyToMessage: &telego.Message{
			MessageID: 46,
			Text:      "Вот рецепт",
			From: &telego.User{
				ID:        1,
				IsBot:     true,
				FirstName: "Kogotok",
				Username:  "KityaBot",
			},
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		if !strings.Contains(inbound.Content, "а подробнее?") {
			t.Fatalf("content=%q should include user reply text", inbound.Content)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
	}
}

func TestHandleMessage_GroupTopicAllowsImplicitReplyToForumTopicRoot(t *testing.T) {
	ignoreNonBotReplies := true
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly:         false,
				IgnoreNonBotReplies: &ignoreNonBotReplies,
				Topics: map[string]config.GroupTriggerConfig{
					"1771": {
						MentionOnly:         false,
						IgnoreNonBotReplies: &ignoreNonBotReplies,
					},
				},
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	msg := &telego.Message{
		Text:            "тест",
		MessageID:       49,
		MessageThreadID: 1771,
		Chat: telego.Chat{
			ID:      -1002133645926,
			Type:    "supergroup",
			IsForum: true,
		},
		From: &telego.User{
			ID:        2490846,
			FirstName: "Anton",
			Username:  "AntonBogdanovich",
		},
		ReplyToMessage: &telego.Message{
			MessageID:       1771,
			MessageThreadID: 1771,
			ForumTopicCreated: &telego.ForumTopicCreated{
				Name: "Коготок",
			},
			From: &telego.User{
				ID:        2490846,
				FirstName: "Anton",
				Username:  "AntonBogdanovich",
			},
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		if !strings.Contains(inbound.Content, "тест") {
			t.Fatalf("content=%q should include user text", inbound.Content)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
	}

	select {
	case observed := <-messageBus.ObservedChan():
		t.Fatalf("topic-root reply should not be observed/suppressed, got %+v", observed)
	default:
	}
}

func TestHandleMessage_GroupTopicAllowsBotMentionInReplyToHuman(t *testing.T) {
	ignoreNonBotReplies := true
	messageBus := bus.NewMessageBus()
	ch := &TelegramChannel{
		BaseChannel: channels.NewBaseChannel(
			"telegram",
			nil,
			messageBus,
			nil,
			channels.WithGroupTrigger(config.GroupTriggerConfig{
				MentionOnly:         false,
				IgnoreNonBotReplies: &ignoreNonBotReplies,
			}),
		),
		bot:     newTestTelegramBot(t, "KityaBot"),
		chatIDs: make(map[string]int64),
		ctx:     context.Background(),
	}

	text := "@KityaBot ответь на это"
	msg := &telego.Message{
		Text: text,
		Entities: []telego.MessageEntity{{
			Type:   telego.EntityTypeMention,
			Offset: 0,
			Length: len("@KityaBot"),
		}},
		MessageID: 48,
		Chat: telego.Chat{
			ID:   -1002133645926,
			Type: "supergroup",
		},
		From: &telego.User{
			ID:        866438409,
			FirstName: "Anna",
			Username:  "mintmeow",
		},
		ReplyToMessage: &telego.Message{
			MessageID: 47,
			Text:      "human context",
			From: &telego.User{
				ID:        123,
				FirstName: "Anton",
				Username:  "AntonBogdanovich",
			},
		},
	}

	if err := ch.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case inbound := <-messageBus.InboundChan():
		if !strings.Contains(inbound.Content, "ответь на это") {
			t.Fatalf("content=%q should include stripped bot-mention content", inbound.Content)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
	}
}
