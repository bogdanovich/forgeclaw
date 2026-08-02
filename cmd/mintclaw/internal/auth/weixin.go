package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/bogdanovich/mintclaw/cmd/mintclaw/internal"
	"github.com/bogdanovich/mintclaw/pkg/channels/weixin"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func newWeixinCommand() *cobra.Command {
	var baseURL string
	var proxy string
	var timeout int
	var allowFrom []string
	var public bool

	cmd := &cobra.Command{
		Use:   "weixin",
		Short: "Connect a WeChat personal account via QR code",
		Long: `Start the interactive Weixin (WeChat personal) QR code login flow.

A QR code is displayed in the terminal. Scan it with the WeChat mobile app
to authorize your account. On success, the bot token is saved to the mintclaw
config so you can start the gateway immediately.

Example:
  mintclaw auth weixin --allow-from YOUR_USER_ID`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			policy, err := explicitChannelAllowFrom(allowFrom, public)
			if err != nil {
				return err
			}
			return runWeixinOnboard(baseURL, proxy, time.Duration(timeout)*time.Second, policy)
		},
	}

	cmd.Flags().StringVar(&baseURL, "base-url", "https://ilinkai.weixin.qq.com/", "iLink API base URL")
	cmd.Flags().StringVar(&proxy, "proxy", "", "HTTP proxy URL (e.g. http://localhost:7890)")
	cmd.Flags().IntVar(&timeout, "timeout", 300, "Login timeout in seconds")
	cmd.Flags().StringSliceVar(&allowFrom, "allow-from", nil, "Sender ID allowed to use the channel (repeatable)")
	cmd.Flags().BoolVar(&public, "public", false, "Allow every sender")
	cmd.MarkFlagsMutuallyExclusive("allow-from", "public")

	return cmd
}

func runWeixinOnboard(
	baseURL, proxy string,
	timeout time.Duration,
	allowFrom config.FlexibleStringSlice,
) error {
	fmt.Println("Starting Weixin (WeChat personal) login...")
	fmt.Println()

	botToken, userID, accountID, returnedBaseURL, err := weixin.PerformLoginInteractive(
		context.Background(),
		weixin.AuthFlowOpts{
			BaseURL: baseURL,
			Timeout: timeout,
			Proxy:   proxy,
		},
	)
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}

	fmt.Println()
	fmt.Println("✅ Login successful!")
	fmt.Printf("   Account ID : %s\n", accountID)
	if userID != "" {
		fmt.Printf("   User ID    : %s\n", userID)
	}
	fmt.Println()

	// Prefer the server-returned base URL (may be region-specific)
	effectiveBaseURL := returnedBaseURL
	if effectiveBaseURL == "" {
		effectiveBaseURL = baseURL
	}

	if err := saveWeixinConfig(botToken, effectiveBaseURL, proxy, allowFrom); err != nil {
		fmt.Printf("⚠️  Could not auto-save to config: %v\n", err)
		printManualWeixinConfig(botToken, effectiveBaseURL, allowFrom)
		return nil
	}

	fmt.Println("✓ Config updated. Start the gateway with:")
	fmt.Println()
	fmt.Println("  mintclaw gateway")
	fmt.Println()
	return nil
}

// saveWeixinConfig patches channels.weixin in the config and saves it.
func saveWeixinConfig(
	token, baseURL, proxy string,
	allowFrom config.FlexibleStringSlice,
) error {
	cfgPath := internal.GetConfigPath()

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	bc := cfg.Channels.GetByType(config.ChannelWeixin)
	if bc == nil {
		bc = &config.Channel{Type: config.ChannelWeixin}
		cfg.Channels[config.ChannelWeixin] = bc
	}
	bc.Enabled = true
	bc.AllowFrom = allowFrom

	if decoded, err := bc.GetDecoded(); err == nil && decoded != nil {
		if weixinCfg, ok := decoded.(*config.WeixinSettings); ok {
			weixinCfg.Token = *config.NewSecureString(token)
			const defaultBase = "https://ilinkai.weixin.qq.com/"
			if baseURL != "" && baseURL != defaultBase {
				weixinCfg.BaseURL = baseURL
			}
			if proxy != "" {
				weixinCfg.Proxy = proxy
			}
		}
	}

	return config.SaveConfig(cfgPath, cfg)
}

func printManualWeixinConfig(token, baseURL string, allowFrom config.FlexibleStringSlice) {
	fmt.Println()
	fmt.Println("Add the following to the channels section of your mintclaw config:")
	fmt.Println()
	fmt.Println(`  "weixin": {`)
	fmt.Println(`    "enabled": true,`)
	fmt.Printf("    \"token\": %q,\n", token)
	const defaultBase = "https://ilinkai.weixin.qq.com/"
	if baseURL != "" && baseURL != defaultBase {
		fmt.Printf("    \"base_url\": %q,\n", baseURL)
	}
	encodedAllowFrom, _ := json.Marshal(allowFrom)
	fmt.Printf("    \"allow_from\": %s\n", encodedAllowFrom)
	fmt.Println(`  }`)
}
