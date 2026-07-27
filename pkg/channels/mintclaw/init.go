package mintclaw

import (
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelMintClaw,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			c, ok := decoded.(*config.MintClawSettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			ch, err := NewMintClawChannel(bc, c, b)
			if err != nil {
				return nil, err
			}
			if channelName != config.ChannelMintClaw {
				ch.SetName(channelName)
			}
			return ch, nil
		},
	)
	channels.RegisterFactory(
		config.ChannelMintClawClient,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			c, ok := decoded.(*config.MintClawClientSettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			ch, err := NewMintClawClientChannel(bc, c, b)
			if err != nil {
				return nil, err
			}
			if channelName != config.ChannelMintClawClient {
				ch.SetName(channelName)
			}
			return ch, nil
		},
	)
}
