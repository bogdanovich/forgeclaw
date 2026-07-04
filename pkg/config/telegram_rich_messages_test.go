package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTelegramRichMessagesConfig_DefaultDisabled(t *testing.T) {
	var settings TelegramSettings

	assert.False(t, settings.RichMessages.Enabled)
	assert.True(t, settings.RichMessages.IsZero())
}

func TestTelegramRichMessagesConfig_EnabledIsNotZero(t *testing.T) {
	settings := TelegramSettings{
		RichMessages: RichMessagesConfig{Enabled: true},
	}

	assert.True(t, settings.RichMessages.Enabled)
	assert.False(t, settings.RichMessages.IsZero())
}
