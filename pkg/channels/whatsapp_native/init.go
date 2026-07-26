package whatsapp

import (
	"path/filepath"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelWhatsAppNative,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			c, ok := decoded.(*config.WhatsAppSettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			storePath := c.SessionStorePath
			if storePath == "" {
				storePath = filepath.Join(cfg.WorkspacePath(), "whatsapp")
			}
			ch, err := NewWhatsAppNativeChannel(bc, channelName, c, b, storePath)
			if err != nil {
				return nil, err
			}
			return ch, nil
		},
	)
}
