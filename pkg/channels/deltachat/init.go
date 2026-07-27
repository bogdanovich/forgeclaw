package deltachat

import (
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelDeltaChat,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			if bc == nil || !bc.Enabled {
				return nil, nil
			}
			settings := &config.DeltaChatSettings{}
			if err := bc.Decode(settings); err != nil {
				return nil, err
			}
			ch, err := NewDeltaChatChannel(bc, settings, b)
			if err != nil {
				return nil, err
			}
			if channelName != config.ChannelDeltaChat {
				ch.SetName(channelName)
			}
			return ch, nil
		},
	)
}
