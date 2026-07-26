package discord

import (
	"github.com/bogdanovich/mintclaw/pkg/audio/tts"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelDiscord,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			c, ok := decoded.(*config.DiscordSettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			ch, err := NewDiscordChannel(bc, c, b)
			if err == nil {
				ch.tts = tts.DetectTTS(cfg)
			}
			return ch, err
		},
	)
}
