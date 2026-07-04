package agent

import (
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

type configChannelStreamingProvider struct {
	cfg *config.Config
}

func newConfigChannelStreamingProvider(cfg *config.Config) channelStreamingConfigProvider {
	return configChannelStreamingProvider{cfg: cfg}
}

func (p configChannelStreamingProvider) channelStreamingConfig(channelName string) (config.StreamingConfig, bool) {
	if p.cfg == nil || p.cfg.Channels == nil {
		return config.StreamingConfig{}, false
	}
	ch := p.cfg.Channels[channelName]
	if ch == nil {
		return config.StreamingConfig{}, false
	}
	decoded, err := ch.GetDecoded()
	if err != nil {
		logger.WarnCF("agent", "channel streaming config decode failed", map[string]any{
			"channel": channelName,
			"error":   err.Error(),
		})
		return config.StreamingConfig{}, false
	}
	return streamingConfigFromDecodedSettings(decoded)
}
