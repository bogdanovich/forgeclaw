package auth

import (
	"slices"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAuthCommand(t *testing.T) {
	cmd := NewAuthCommand()

	require.NotNil(t, cmd)

	assert.Equal(t, "auth", cmd.Use)
	assert.Equal(t, "Manage authentication (login, logout, status)", cmd.Short)

	assert.Len(t, cmd.Aliases, 0)

	assert.Nil(t, cmd.Run)
	assert.NotNil(t, cmd.RunE)

	assert.Nil(t, cmd.PersistentPreRun)
	assert.Nil(t, cmd.PersistentPostRun)

	assert.False(t, cmd.HasFlags())
	assert.True(t, cmd.HasSubCommands())

	allowedCommands := []string{
		"login",
		"logout",
		"status",
		"models",
		"weixin",
		"wecom",
	}

	subcommands := cmd.Commands()
	assert.Len(t, subcommands, len(allowedCommands))

	for _, subcmd := range subcommands {
		found := slices.Contains(allowedCommands, subcmd.Name())
		assert.True(t, found, "unexpected subcommand %q", subcmd.Name())

		assert.Len(t, subcmd.Aliases, 0)
		assert.False(t, subcmd.Hidden)

		assert.False(t, subcmd.HasSubCommands())

		assert.Nil(t, subcmd.Run)
		assert.NotNil(t, subcmd.RunE)

		assert.Nil(t, subcmd.PersistentPreRun)
		assert.Nil(t, subcmd.PersistentPostRun)
	}
}

func TestExplicitChannelAllowFrom(t *testing.T) {
	t.Run("requires explicit policy", func(t *testing.T) {
		_, err := explicitChannelAllowFrom(nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--allow-from")
	})

	t.Run("normalizes sender IDs", func(t *testing.T) {
		allowFrom, err := explicitChannelAllowFrom([]string{" owner-1 ", ""}, false)
		require.NoError(t, err)
		assert.Equal(t, []string{"owner-1"}, []string(allowFrom))
	})

	t.Run("public uses wildcard", func(t *testing.T) {
		allowFrom, err := explicitChannelAllowFrom(nil, true)
		require.NoError(t, err)
		assert.Equal(t, []string{"*"}, []string(allowFrom))
	})
}

func TestChannelAuthCommandsRequireExplicitAllowPolicy(t *testing.T) {
	commands := []*cobra.Command{newWeixinCommand(), newWeComCommand()}
	for _, command := range commands {
		err := command.RunE(command, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--allow-from")
	}
}
