// PicoClaw - Ultra-lightweight personal AI agent

package agent

import "github.com/sipeed/picoclaw/pkg/providers"

func (p *Pipeline) updateAutoFallbackSelection(
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
	usedLight bool,
) {
	if p == nil || p.FallbackState == nil {
		return
	}
	p.FallbackState.updateAutoFallbackSelection(routeSessionKey, selectedCandidates, result, usedLight)
}
