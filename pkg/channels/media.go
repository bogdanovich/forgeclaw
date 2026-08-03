package channels

import (
	"context"

	"github.com/bogdanovich/mintclaw/pkg/bus"
)

// MediaSender is an optional interface for channels that can send
// media attachments (images, files, audio, video).
// Manager discovers channels implementing this interface via type
// assertion and routes OutboundMediaMessage to them.
type MediaSender interface {
	SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error)
}

// MediaDeliverySender preserves typed transport metadata that the legacy
// MediaSender signature cannot represent.
type MediaDeliverySender interface {
	SendMediaResult(ctx context.Context, pending []bus.OutboundMediaMessage) DeliveryResult[bus.OutboundMediaMessage]
}
