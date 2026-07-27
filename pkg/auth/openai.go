package auth

import "fmt"

// GetOpenAIToken returns the current OpenAI credential, refreshing OAuth
// credentials when they are close to expiry. The account ID is returned for
// Codex backend calls that require the Chatgpt-Account-Id header.
func GetOpenAIToken() (accessToken, accountID string, err error) {
	cred, err := GetCredential("openai")
	if err != nil {
		return "", "", fmt.Errorf("loading auth credentials: %w", err)
	}
	if cred == nil {
		return "", "", fmt.Errorf("no credentials for openai. Run: mintclaw auth login --provider openai")
	}

	if cred.AuthMethod == "oauth" && cred.NeedsRefresh() && cred.RefreshToken != "" {
		var refreshedAccessToken, refreshedAccountID string
		if err := withAuthStoreLock(func() error {
			current, err := GetCredential("openai")
			if err != nil {
				return fmt.Errorf("loading auth credentials: %w", err)
			}
			if current == nil {
				return fmt.Errorf("no credentials for openai. Run: mintclaw auth login --provider openai")
			}
			if current.AuthMethod != "oauth" || !current.NeedsRefresh() || current.RefreshToken == "" {
				refreshedAccessToken = current.AccessToken
				refreshedAccountID = current.AccountID
				return nil
			}

			refreshed, err := RefreshAccessToken(current, OpenAIOAuthConfig())
			if err != nil {
				return fmt.Errorf("refreshing token: %w", err)
			}
			if refreshed.AccountID == "" {
				refreshed.AccountID = current.AccountID
			}
			if err := SetCredential("openai", refreshed); err != nil {
				return fmt.Errorf("saving refreshed token: %w", err)
			}
			refreshedAccessToken = refreshed.AccessToken
			refreshedAccountID = refreshed.AccountID
			return nil
		}); err != nil {
			return "", "", err
		}
		return refreshedAccessToken, refreshedAccountID, nil
	}

	return cred.AccessToken, cred.AccountID, nil
}
