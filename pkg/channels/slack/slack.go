package slack

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const (
	slackSocketReconnectInitialDelay = time.Second
	slackSocketReconnectMaxDelay     = 30 * time.Second
)

type SlackChannel struct {
	*channels.BaseChannel
	bc              *config.Channel
	config          *config.SlackSettings
	api             *slack.Client
	socketClient    *socketmode.Client
	botUserID       string
	teamID          string
	ctx             context.Context
	cancel          context.CancelFunc
	pendingAcks     sync.Map
	inboundDedup    *channels.DedupStore
	progress        *channels.ToolFeedbackAnimator
	postMessageFn   func(context.Context, string, string, string) (string, error)
	editMessageFn   func(context.Context, string, string, string) error
	deleteMessageFn func(context.Context, string, string) error
	uploadFileFn    func(context.Context, slack.UploadFileParameters) error
	postTextFn      func(context.Context, string, string, string) error
	runSocketModeFn func(context.Context) error
	reconnectDelay  func(int) time.Duration
}

type slackMessageRef struct {
	ChannelID string
	Timestamp string
}

func NewSlackChannel(
	bc *config.Channel,
	cfg *config.SlackSettings,
	messageBus *bus.MessageBus,
) (*SlackChannel, error) {
	if cfg.BotToken.String() == "" || cfg.AppToken.String() == "" {
		return nil, fmt.Errorf("slack bot_token and app_token are required")
	}

	api := slack.New(
		cfg.BotToken.String(),
		slack.OptionAppLevelToken(cfg.AppToken.String()),
	)

	socketClient := socketmode.New(api)

	base := channels.NewBaseChannel("slack", cfg, messageBus, bc.AllowFrom,
		channels.WithMaxMessageLength(40000),
		channels.WithGroupTrigger(bc.GroupTrigger),
		channels.WithReasoningChannelID(bc.ReasoningChannelID),
	)

	ch := &SlackChannel{
		BaseChannel:  base,
		bc:           bc,
		config:       cfg,
		api:          api,
		socketClient: socketClient,
		inboundDedup: channels.NewDedupStore(2*time.Minute, 0),
	}
	ch.postMessageFn = func(ctx context.Context, channelID, threadTS, text string) (string, error) {
		opts := ch.slackMessageOptions(text, threadTS)
		if threadTS != "" {
			opts = append(opts, slack.MsgOptionTS(threadTS))
		}
		_, ts, err := api.PostMessageContext(ctx, channelID, opts...)
		return ts, err
	}
	ch.editMessageFn = func(ctx context.Context, channelID, messageID, text string) error {
		opts := ch.slackMessageOptions(text, "")
		_, _, _, err := api.UpdateMessageContext(
			ctx,
			channelID,
			messageID,
			opts...,
		)
		return err
	}
	ch.deleteMessageFn = func(ctx context.Context, channelID, messageID string) error {
		_, _, err := api.DeleteMessageContext(ctx, channelID, messageID)
		return err
	}
	ch.uploadFileFn = func(ctx context.Context, params slack.UploadFileParameters) error {
		_, err := api.UploadFileContext(ctx, params)
		return err
	}
	ch.postTextFn = func(ctx context.Context, channelID, threadTS, text string) error {
		opts := ch.slackMessageOptions(text, threadTS)
		if threadTS != "" {
			opts = append(opts, slack.MsgOptionTS(threadTS))
		}
		_, _, err := api.PostMessageContext(ctx, channelID, opts...)
		return err
	}
	ch.progress = channels.NewToolFeedbackAnimator(func(ctx context.Context, chatID, messageID, content string) error {
		return ch.EditMessage(ctx, slackToolFeedbackDeliveryChatKey(chatID), messageID, content)
	}, func(ctx context.Context, chatID, messageID string) error {
		return ch.DeleteMessage(ctx, slackToolFeedbackDeliveryChatKey(chatID), messageID)
	})
	return ch, nil
}

func (c *SlackChannel) slackMessageOptions(text, threadTS string) []slack.MsgOption {
	formatted := formatSlackMessage(text)
	opts := []slack.MsgOption{
		slack.MsgOptionText(formatted, false),
	}
	blocks := c.slackBlocks(formatted)
	if len(blocks) > 0 {
		opts = append(opts, slack.MsgOptionBlocks(blocks...))
	}
	return opts
}

func (c *SlackChannel) slackBlocks(text string) []slack.Block {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	// Avoid splitting fenced code blocks across section blocks; Slack renders
	// broken mrkdwn when a long code fence is chunked mid-block.
	if strings.Contains(text, "```") && len([]rune(text)) > slackMaxTextBlockLength {
		return nil
	}
	chunks := splitSlackText(text, slackMaxTextBlockLength)
	blocks := make([]slack.Block, 0, len(chunks))
	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		textObj := slack.NewTextBlockObject(slack.MarkdownType, chunk, false, false)
		blocks = append(blocks, slack.NewSectionBlock(textObj, nil, nil))
	}
	return blocks
}

func (c *SlackChannel) Start(ctx context.Context) error {
	logger.InfoC("slack", "Starting Slack channel (Socket Mode)")

	c.ctx, c.cancel = context.WithCancel(ctx)

	authResp, err := c.api.AuthTest()
	if err != nil {
		return fmt.Errorf("slack auth test failed: %w", err)
	}
	c.botUserID = authResp.UserID
	c.teamID = authResp.TeamID

	logger.InfoCF("slack", "Slack bot connected", map[string]any{
		"bot_user_id": c.botUserID,
		"team":        authResp.Team,
	})

	go c.eventLoop()

	c.SetRunning(true)
	go c.runSocketModeLoop()
	logger.InfoC("slack", "Slack channel started (Socket Mode)")
	return nil
}

func (c *SlackChannel) Stop(ctx context.Context) error {
	logger.InfoC("slack", "Stopping Slack channel")

	if c.cancel != nil {
		c.cancel()
	}
	if c.progress != nil {
		c.progress.StopAll()
	}

	c.SetRunning(false)
	logger.InfoC("slack", "Slack channel stopped")
	return nil
}

func (c *SlackChannel) runSocketModeLoop() {
	attempt := 0
	for {
		err := c.runSocketMode(c.ctx)
		if c.ctx.Err() != nil {
			return
		}

		c.SetRunning(false)
		if err != nil {
			logger.ErrorCF("slack", "Socket Mode connection error", map[string]any{
				"error": err.Error(),
			})
		} else {
			logger.WarnC("slack", "Socket Mode connection exited unexpectedly")
		}

		attempt++
		delay := c.socketReconnectDelay(attempt)
		logger.WarnCF("slack", "Reconnecting Slack Socket Mode", map[string]any{
			"attempt":     attempt,
			"retry_after": delay.String(),
		})

		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}

		c.SetRunning(true)
	}
}

func (c *SlackChannel) runSocketMode(ctx context.Context) error {
	if c.runSocketModeFn != nil {
		return c.runSocketModeFn(ctx)
	}
	return c.socketClient.RunContext(ctx)
}

func (c *SlackChannel) socketReconnectDelay(attempt int) time.Duration {
	if c.reconnectDelay != nil {
		return c.reconnectDelay(attempt)
	}
	delay := slackSocketReconnectInitialDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= slackSocketReconnectMaxDelay {
			return slackSocketReconnectMaxDelay
		}
	}
	return delay
}

func (c *SlackChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}

	deliveryChatID, channelID, threadTS := resolveSlackOutboundTarget(msg.ChatID, &msg.Context)
	if channelID == "" {
		return nil, fmt.Errorf("invalid slack chat ID: %s", msg.ChatID)
	}
	if len([]rune(msg.Content)) == 0 {
		return nil, nil
	}

	isToolFeedback := outboundMessageIsToolFeedback(msg)
	trackedChatID := slackToolFeedbackMessageKey(msg.ChatID, &msg.Context, msg.SessionKey)
	if isToolFeedback {
		if msgID, handled, err := c.progress.Update(ctx, trackedChatID, msg.Content); handled {
			if err != nil {
				return nil, err
			}
			return []string{msgID}, nil
		}
	}
	trackedMsgID, hasTrackedMsg := c.currentToolFeedbackMessage(trackedChatID)
	if channels.OutboundMessageFinalizesTrackedToolFeedback(msg) {
		if msgIDs, handled := c.FinalizeToolFeedbackMessage(ctx, msg); handled {
			return msgIDs, nil
		}
	}

	content := msg.Content
	if isToolFeedback {
		content = channels.InitialAnimatedToolFeedbackContent(msg.Content)
	}

	if msg.ReplyToMessageID != "" && threadTS == "" {
		// Answer to the message by creating a Thread under it
		threadTS = msg.ReplyToMessageID
	}

	ts, err := c.postMessageFn(ctx, channelID, threadTS, content)
	if err != nil {
		return nil, fmt.Errorf("slack send: %w", channels.ErrTemporary)
	}
	if isToolFeedback {
		c.RecordEditedToolFeedbackMessage(trackedChatID, ts, msg.Content)
	} else if hasTrackedMsg && channels.OutboundMessageDismissesTrackedToolFeedback(msg) {
		c.dismissTrackedToolFeedbackMessage(ctx, trackedChatID, trackedMsgID)
	}

	if ref, ok := c.pendingAcks.LoadAndDelete(deliveryChatID); ok {
		msgRef, ok := ref.(slackMessageRef)
		if !ok {
			return []string{ts}, nil
		}
		c.api.AddReaction("white_check_mark", slack.ItemRef{
			Channel:   msgRef.ChannelID,
			Timestamp: msgRef.Timestamp,
		})
	}

	logger.DebugCF("slack", "Message sent", map[string]any{
		"channel_id": channelID,
		"thread_ts":  threadTS,
	})

	return []string{ts}, nil
}

// SendMedia implements the channels.MediaSender interface.
func (c *SlackChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}

	_, channelID, threadTS := resolveSlackMediaOutboundTarget(msg.ChatID, &msg.Context)
	if channelID == "" {
		return nil, fmt.Errorf("invalid slack chat ID: %s", msg.ChatID)
	}

	store := c.GetMediaStore()
	if store == nil {
		return nil, fmt.Errorf("no media store available: %w", channels.ErrSendFailed)
	}

	caption := channels.FirstMediaCaption(msg.Parts)
	sentAny := false
	for _, part := range msg.Parts {
		localPath, err := store.Resolve(part.Ref)
		if err != nil {
			logger.ErrorCF("slack", "Failed to resolve media ref", map[string]any{
				"ref":   part.Ref,
				"error": err.Error(),
			})
			continue
		}

		filename := part.Filename
		if filename == "" {
			filename = "file"
		}

		title := part.Caption
		if title == "" {
			title = filename
		}

		err = c.uploadFileFn(ctx, slack.UploadFileParameters{
			Channel:         channelID,
			ThreadTimestamp: threadTS,
			File:            localPath,
			FileSize:        slackUploadFileSize(localPath),
			Filename:        filename,
			Title:           title,
		})
		if err != nil {
			logger.ErrorCF("slack", "Failed to upload media", map[string]any{
				"filename": filename,
				"error":    err.Error(),
			})
			return nil, fmt.Errorf("slack send media: %w", channels.ErrTemporary)
		}
		sentAny = true
	}

	if sentAny && caption != "" {
		if err := c.postTextFn(ctx, channelID, threadTS, caption); err != nil {
			return nil, fmt.Errorf("slack send media caption fallback: %w", channels.ErrTemporary)
		}
	}

	// UploadFile does not expose the posted message timestamp in its
	// response; returning nil avoids conflating file IDs with message IDs.
	return nil, nil
}

func (c *SlackChannel) ConfigureToolFeedbackAnimator(cfg channels.ToolFeedbackAnimatorConfig) {
	if c.progress != nil {
		c.progress.Configure(cfg)
	}
}

func (c *SlackChannel) EditMessage(ctx context.Context, chatID string, messageID string, content string) error {
	channelID, _ := parseSlackChatID(slackToolFeedbackDeliveryChatKey(chatID))
	if channelID == "" {
		return fmt.Errorf("invalid slack chat ID: %s", chatID)
	}
	return c.editMessageFn(ctx, channelID, messageID, content)
}

func (c *SlackChannel) DeleteMessage(ctx context.Context, chatID string, messageID string) error {
	channelID, _ := parseSlackChatID(slackToolFeedbackDeliveryChatKey(chatID))
	if channelID == "" {
		return fmt.Errorf("invalid slack chat ID: %s", chatID)
	}
	return c.deleteMessageFn(ctx, channelID, messageID)
}

func (c *SlackChannel) SendPlaceholder(ctx context.Context, chatID string) (string, error) {
	if c.bc == nil || !c.bc.Placeholder.Enabled {
		return "", nil
	}
	deliveryChatID, channelID, threadTS := resolveSlackOutboundTarget(chatID, nil)
	if channelID == "" {
		return "", fmt.Errorf("invalid slack chat ID: %s", chatID)
	}
	if deliveryChatID == "" {
		return "", nil
	}
	return c.postMessageFn(ctx, channelID, threadTS, c.bc.Placeholder.GetRandomText())
}

func (c *SlackChannel) currentToolFeedbackMessage(chatID string) (string, bool) {
	if c.progress == nil {
		return "", false
	}
	return c.progress.Current(chatID)
}

func (c *SlackChannel) RecordToolFeedbackMessage(chatID, messageID, content string) {
	if c.progress == nil {
		return
	}
	c.progress.Record(chatID, messageID, content)
}

func (c *SlackChannel) RecordEditedToolFeedbackMessage(chatID, messageID, content string) {
	if c.progress == nil {
		return
	}
	c.progress.RecordEdited(chatID, messageID, content)
}

func (c *SlackChannel) ClearToolFeedbackMessage(chatID string) {
	if c.progress == nil {
		return
	}
	c.progress.Clear(chatID)
}

func (c *SlackChannel) DismissToolFeedbackMessage(ctx context.Context, chatID string) {
	msgID, ok := c.currentToolFeedbackMessage(chatID)
	if !ok {
		return
	}
	c.dismissTrackedToolFeedbackMessage(ctx, chatID, msgID)
}

func (c *SlackChannel) dismissTrackedToolFeedbackMessage(ctx context.Context, chatID, messageID string) {
	if strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return
	}
	if !c.progress.ClearIfCurrent(chatID, messageID) {
		return
	}
	_ = c.DeleteMessage(ctx, chatID, messageID)
}

func (c *SlackChannel) finalizeTrackedToolFeedbackMessage(
	ctx context.Context,
	chatID string,
	content string,
	editFn func(context.Context, string, string, string) error,
) ([]string, bool) {
	if c.progress == nil || editFn == nil {
		return nil, false
	}
	snapshot, ok := c.progress.TakeRestorable(chatID)
	if !ok {
		return nil, false
	}
	if err := editFn(ctx, chatID, snapshot.MessageID, content); err != nil {
		c.progress.Restore(snapshot)
		return nil, false
	}
	return []string{snapshot.MessageID}, true
}

func (c *SlackChannel) FinalizeToolFeedbackMessage(ctx context.Context, msg bus.OutboundMessage) ([]string, bool) {
	if !channels.OutboundMessageFinalizesTrackedToolFeedback(msg) {
		return nil, false
	}
	return c.finalizeTrackedToolFeedbackMessage(
		ctx,
		slackToolFeedbackMessageKey(msg.ChatID, &msg.Context, msg.SessionKey),
		msg.Content,
		func(ctx context.Context, chatID, messageID, content string) error {
			return c.EditMessage(ctx, slackToolFeedbackDeliveryChatKey(chatID), messageID, content)
		},
	)
}

func slackUploadFileSize(path string) int {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if info.Size() <= 0 {
		return 0
	}
	if info.Size() > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(info.Size())
}

// ReactToMessage implements channels.ReactionCapable.
// It adds an "eyes" (👀) reaction to the inbound message and returns an undo function
// that removes the reaction.
func (c *SlackChannel) ReactToMessage(ctx context.Context, chatID, messageID string) (func(), error) {
	channelID, _ := parseSlackChatID(chatID)
	if channelID == "" {
		return func() {}, nil
	}

	c.api.AddReaction("eyes", slack.ItemRef{
		Channel:   channelID,
		Timestamp: messageID,
	})

	return func() {
		c.api.RemoveReaction("eyes", slack.ItemRef{
			Channel:   channelID,
			Timestamp: messageID,
		})
	}, nil
}

func (c *SlackChannel) eventLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-c.socketClient.Events:
			if !ok {
				return
			}
			switch event.Type {
			case socketmode.EventTypeEventsAPI:
				c.handleEventsAPI(event)
			case socketmode.EventTypeSlashCommand:
				c.handleSlashCommand(event)
			case socketmode.EventTypeInteractive:
				if event.Request != nil {
					c.socketClient.Ack(*event.Request)
				}
			}
		}
	}
}

func (c *SlackChannel) handleEventsAPI(event socketmode.Event) {
	if event.Request != nil {
		c.socketClient.Ack(*event.Request)
	}

	eventsAPIEvent, ok := event.Data.(slackevents.EventsAPIEvent)
	if !ok {
		return
	}

	switch ev := eventsAPIEvent.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		c.handleMessageEvent(ev)
	case *slackevents.AppMentionEvent:
		c.handleAppMention(ev)
	}
}

func (c *SlackChannel) handleMessageEvent(ev *slackevents.MessageEvent) {
	if ev.User == c.botUserID || ev.User == "" {
		return
	}
	if ev.BotID != "" {
		return
	}
	if ev.SubType != "" && ev.SubType != "file_share" {
		return
	}

	// check allowlist to avoid downloading attachments for rejected users
	sender := bus.SenderInfo{
		Platform:    "slack",
		PlatformID:  ev.User,
		CanonicalID: identity.BuildCanonicalID("slack", ev.User),
	}
	if !c.IsAllowedSender(sender) {
		logger.DebugCF("slack", "Message rejected by allowlist", map[string]any{
			"user_id": ev.User,
		})
		return
	}

	senderID := ev.User
	channelID := ev.Channel
	if !c.shouldProcessChannel(channelID) {
		logger.DebugCF("slack", "Message ignored by channel filter", map[string]any{
			"channel_id": channelID,
			"user_id":    ev.User,
		})
		return
	}
	threadTS := ev.ThreadTimeStamp
	messageTS := ev.TimeStamp
	if c.markInboundEventHandled("message", channelID, messageTS) {
		return
	}

	chatID := channelID
	if threadTS != "" {
		chatID = channelID + "/" + threadTS
	}

	c.pendingAcks.Store(chatID, slackMessageRef{
		ChannelID: channelID,
		Timestamp: messageTS,
	})

	content := ev.Text
	content = c.stripBotMention(content)

	// In non-DM channels, apply group trigger filtering
	if !strings.HasPrefix(channelID, "D") {
		respond, cleaned := c.ShouldRespondInGroup(false, content)
		if !respond {
			return
		}
		content = cleaned
	}

	var mediaPaths []string

	scope := channels.BuildMediaScope("slack", chatID, messageTS)

	// Helper to register a local file with the media store
	storeMedia := func(localPath, filename string) string {
		if store := c.GetMediaStore(); store != nil {
			ref, err := store.Store(localPath, media.MediaMeta{
				Filename:      filename,
				Source:        "slack",
				CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
			}, scope)
			if err == nil {
				return ref
			}
		}
		return localPath // fallback
	}

	if ev.Message != nil && len(ev.Message.Files) > 0 {
		for _, file := range ev.Message.Files {
			localPath := c.downloadSlackFile(file)
			if localPath == "" {
				continue
			}
			mediaPaths = append(mediaPaths, storeMedia(localPath, file.Name))
			content += fmt.Sprintf("\n[file: %s]", file.Name)
		}
	}

	if strings.TrimSpace(content) == "" {
		return
	}

	peerKind := "channel"
	if strings.HasPrefix(channelID, "D") {
		peerKind = "direct"
	}

	metadata := map[string]string{
		"message_ts": messageTS,
		"channel_id": channelID,
		"thread_ts":  threadTS,
		"platform":   "slack",
		"team_id":    c.teamID,
	}

	logger.DebugCF("slack", "Received message", map[string]any{
		"sender_id":  senderID,
		"chat_id":    chatID,
		"preview":    utils.Truncate(content, 50),
		"has_thread": threadTS != "",
	})

	inboundCtx := bus.InboundContext{
		Channel:   c.Name(),
		Account:   c.teamID,
		ChatID:    channelID,
		ChatType:  peerKind,
		SenderID:  senderID,
		MessageID: messageTS,
		SpaceID:   c.teamID,
		SpaceType: "workspace",
		Raw:       metadata,
	}
	if threadTS != "" {
		inboundCtx.TopicID = threadTS
	}

	c.HandleInboundContext(c.ctx, chatID, content, mediaPaths, inboundCtx, sender)
}

func (c *SlackChannel) handleAppMention(ev *slackevents.AppMentionEvent) {
	if ev.User == c.botUserID {
		return
	}

	if !c.IsAllowedSender(bus.SenderInfo{
		Platform:    "slack",
		PlatformID:  ev.User,
		CanonicalID: identity.BuildCanonicalID("slack", ev.User),
	}) {
		logger.DebugCF("slack", "Mention rejected by allowlist", map[string]any{
			"user_id": ev.User,
		})
		return
	}

	senderID := ev.User
	mentionSender := bus.SenderInfo{
		Platform:    "slack",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("slack", senderID),
	}
	channelID := ev.Channel
	if !c.shouldProcessChannel(channelID) {
		logger.DebugCF("slack", "Mention ignored by channel filter", map[string]any{
			"channel_id": channelID,
			"user_id":    ev.User,
		})
		return
	}
	threadTS := ev.ThreadTimeStamp
	messageTS := ev.TimeStamp
	if c.markInboundEventHandled("app_mention", channelID, messageTS) {
		return
	}

	var chatID string
	if threadTS != "" {
		chatID = channelID + "/" + threadTS
	} else {
		chatID = channelID + "/" + messageTS
	}

	c.pendingAcks.Store(chatID, slackMessageRef{
		ChannelID: channelID,
		Timestamp: messageTS,
	})

	content := c.stripBotMention(ev.Text)

	if strings.TrimSpace(content) == "" {
		return
	}

	mentionPeerKind := "channel"
	if strings.HasPrefix(channelID, "D") {
		mentionPeerKind = "direct"
	}

	metadata := map[string]string{
		"message_ts": messageTS,
		"channel_id": channelID,
		"thread_ts":  threadTS,
		"platform":   "slack",
		"is_mention": "true",
		"team_id":    c.teamID,
	}
	inboundCtx := bus.InboundContext{
		Channel:   c.Name(),
		Account:   c.teamID,
		ChatID:    channelID,
		ChatType:  mentionPeerKind,
		TopicID:   threadTS,
		SenderID:  senderID,
		MessageID: messageTS,
		SpaceID:   c.teamID,
		SpaceType: "workspace",
		Mentioned: true,
		Raw:       metadata,
	}

	c.HandleInboundContext(c.ctx, chatID, content, nil, inboundCtx, mentionSender)
}

func (c *SlackChannel) markInboundEventHandled(eventKind, channelID, messageTS string) bool {
	eventKind = strings.TrimSpace(eventKind)
	channelID = strings.TrimSpace(channelID)
	messageTS = strings.TrimSpace(messageTS)
	if eventKind == "" || channelID == "" || messageTS == "" {
		return false
	}
	if c.inboundDedup == nil {
		c.inboundDedup = channels.NewDedupStore(2*time.Minute, 0)
	}
	key := eventKind + "|" + channelID + "|" + messageTS
	return c.inboundDedup.Seen(key)
}

func (c *SlackChannel) shouldProcessChannel(channelID string) bool {
	if channelID == "" || c == nil || c.config == nil {
		return true
	}
	if len(c.config.AllowedChannelIDs) > 0 && !containsSlackChannelID(c.config.AllowedChannelIDs, channelID) {
		return false
	}
	if containsSlackChannelID(c.config.IgnoredChannelIDs, channelID) {
		return false
	}
	return true
}

func containsSlackChannelID(ids []string, channelID string) bool {
	for _, id := range ids {
		if strings.TrimSpace(id) == channelID {
			return true
		}
	}
	return false
}

func (c *SlackChannel) handleSlashCommand(event socketmode.Event) {
	cmd, ok := event.Data.(slack.SlashCommand)
	if !ok {
		return
	}

	if event.Request != nil {
		c.socketClient.Ack(*event.Request)
	}

	cmdSender := bus.SenderInfo{
		Platform:    "slack",
		PlatformID:  cmd.UserID,
		CanonicalID: identity.BuildCanonicalID("slack", cmd.UserID),
	}
	if !c.IsAllowedSender(cmdSender) {
		logger.DebugCF("slack", "Slash command rejected by allowlist", map[string]any{
			"user_id": cmd.UserID,
		})
		return
	}

	senderID := cmd.UserID
	channelID := cmd.ChannelID
	if !c.shouldProcessChannel(channelID) {
		logger.DebugCF("slack", "Slash command ignored by channel filter", map[string]any{
			"channel_id": channelID,
			"user_id":    cmd.UserID,
		})
		return
	}
	chatID := channelID
	content := cmd.Text

	if strings.TrimSpace(content) == "" {
		content = "help"
	}

	metadata := map[string]string{
		"channel_id": channelID,
		"platform":   "slack",
		"is_command": "true",
		"trigger_id": cmd.TriggerID,
		"team_id":    c.teamID,
	}

	logger.DebugCF("slack", "Slash command received", map[string]any{
		"sender_id": senderID,
		"command":   cmd.Command,
		"text":      utils.Truncate(content, 50),
	})
	peerKind := "channel"
	if strings.HasPrefix(channelID, "D") {
		peerKind = "direct"
	}
	inboundCtx := bus.InboundContext{
		Channel:   c.Name(),
		Account:   c.teamID,
		ChatID:    channelID,
		ChatType:  peerKind,
		SenderID:  senderID,
		SpaceID:   c.teamID,
		SpaceType: "workspace",
		Raw:       metadata,
	}

	c.HandleInboundContext(c.ctx, chatID, content, nil, inboundCtx, cmdSender)
}

func (c *SlackChannel) downloadSlackFile(file slack.File) string {
	downloadURL := file.URLPrivateDownload
	if downloadURL == "" {
		downloadURL = file.URLPrivate
	}
	if downloadURL == "" {
		logger.ErrorCF("slack", "No download URL for file", map[string]any{"file_id": file.ID})
		return ""
	}

	return utils.DownloadFile(downloadURL, file.Name, utils.DownloadOptions{
		LoggerPrefix: "slack",
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + c.config.BotToken.String(),
		},
	})
}

func (c *SlackChannel) stripBotMention(text string) string {
	mention := fmt.Sprintf("<@%s>", c.botUserID)
	text = strings.ReplaceAll(text, mention, "")
	return strings.TrimSpace(text)
}

func parseSlackChatID(chatID string) (channelID, threadTS string) {
	parts := strings.SplitN(chatID, "/", 2)
	channelID = parts[0]
	if len(parts) > 1 {
		threadTS = parts[1]
	}
	return channelID, threadTS
}

func resolveSlackOutboundTarget(chatID string, outboundCtx *bus.InboundContext) (string, string, string) {
	deliveryChatID := channels.EffectiveOutboundChatID(chatID, outboundCtx)
	channelID, threadTS := parseSlackChatID(deliveryChatID)
	if threadTS == "" {
		threadTS = channels.EffectiveOutboundTopicID("", outboundCtx)
		if threadTS != "" && channelID != "" {
			deliveryChatID = channelID + "/" + threadTS
		}
	}
	return deliveryChatID, channelID, threadTS
}

func resolveSlackMediaOutboundTarget(chatID string, outboundCtx *bus.InboundContext) (string, string, string) {
	deliveryChatID := channels.EffectiveOutboundChatID(chatID, outboundCtx)
	channelID, threadTS := parseSlackChatID(deliveryChatID)
	if threadTS == "" {
		threadTS = channels.EffectiveOutboundTopicID("", outboundCtx)
		if threadTS != "" && channelID != "" {
			deliveryChatID = channelID + "/" + threadTS
		}
	}
	return deliveryChatID, channelID, threadTS
}

func (c *SlackChannel) ResolveOutboundChatID(chatID string, outboundCtx *bus.InboundContext) string {
	return slackToolFeedbackChatKey(chatID, outboundCtx)
}

func (c *SlackChannel) ToolFeedbackMessageChatID(chatID string, outboundCtx *bus.InboundContext) string {
	return slackToolFeedbackChatKey(chatID, outboundCtx)
}

func slackToolFeedbackChatKey(chatID string, outboundCtx *bus.InboundContext) string {
	deliveryChatID, channelID, threadTS := resolveSlackOutboundTarget(chatID, outboundCtx)
	if threadTS == "" {
		threadTS = channels.EffectiveOutboundReplyToMessageID("", outboundCtx)
		if threadTS != "" && channelID != "" {
			deliveryChatID = channelID + "/" + threadTS
		}
	}
	return strings.TrimSpace(deliveryChatID)
}

func slackToolFeedbackMessageKey(chatID string, outboundCtx *bus.InboundContext, sessionKey string) string {
	key := slackToolFeedbackChatKey(chatID, outboundCtx)
	sessionKey = strings.TrimSpace(sessionKey)
	if key == "" || sessionKey == "" {
		return key
	}
	return key + "#session:" + sessionKey
}

func slackToolFeedbackDeliveryChatKey(chatID string) string {
	chatID = strings.TrimSpace(chatID)
	if idx := strings.Index(chatID, "#session:"); idx >= 0 {
		return strings.TrimSpace(chatID[:idx])
	}
	return chatID
}

func outboundMessageIsToolFeedback(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(msg.Context.Raw["message_kind"]), "tool_feedback")
}
