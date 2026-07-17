package telegram

import (
	"context"
	"fmt"
	"sync"

	"github.com/mymmrac/telego"

	"github.com/sipeed/picoclaw/pkg/logger"
)

const telegramUpdateQueueSize = 64

type telegramUpdateHandler func(context.Context, telego.Update) error

type telegramUpdateWorker struct {
	key     string
	updates chan telego.Update
}

// runTelegramUpdatesOrdered preserves Telegram's polling order within each
// chat topic while allowing unrelated conversations to download media in
// parallel.
func runTelegramUpdatesOrdered(
	ctx context.Context,
	updates <-chan telego.Update,
	handle telegramUpdateHandler,
) error {
	workers := make(map[string]*telegramUpdateWorker)
	var workerWG sync.WaitGroup

	defer func() {
		for _, worker := range workers {
			close(worker.updates)
		}
		workerWG.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update, ok := <-updates:
			if !ok {
				return nil
			}

			key := telegramUpdateConversationKey(update)
			worker := workers[key]
			if worker == nil {
				worker = &telegramUpdateWorker{
					key:     key,
					updates: make(chan telego.Update, telegramUpdateQueueSize),
				}
				workers[key] = worker
				workerWG.Add(1)
				go runTelegramUpdateWorker(ctx, worker, handle, &workerWG)
			}

			select {
			case worker.updates <- update:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func runTelegramUpdateWorker(
	ctx context.Context,
	worker *telegramUpdateWorker,
	handle telegramUpdateHandler,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for update := range worker.updates {
		if ctx.Err() != nil {
			return
		}
		if err := handle(ctx, update); err != nil {
			logger.ErrorCF("telegram", "Failed to handle Telegram update", map[string]any{
				"conversation": worker.key,
				"error":        err.Error(),
				"update_id":    update.UpdateID,
			})
		}
	}
}

func telegramUpdateConversationKey(update telego.Update) string {
	message := telegramMessageFromUpdate(update)
	if message == nil {
		return fmt.Sprintf("update:%d", update.UpdateID)
	}
	return fmt.Sprintf("chat:%d:topic:%d", message.Chat.ID, message.MessageThreadID)
}

func telegramMessageFromUpdate(update telego.Update) *telego.Message {
	switch {
	case update.Message != nil:
		return update.Message
	case update.EditedMessage != nil:
		return update.EditedMessage
	case update.ChannelPost != nil:
		return update.ChannelPost
	case update.EditedChannelPost != nil:
		return update.EditedChannelPost
	case update.BusinessMessage != nil:
		return update.BusinessMessage
	case update.EditedBusinessMessage != nil:
		return update.EditedBusinessMessage
	default:
		return nil
	}
}
