// PicoClaw - Ultra-lightweight personal AI agent

package agent

import "github.com/sipeed/picoclaw/pkg/providers"

func (p *Pipeline) updateAutoFallbackSelection(
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
	usedLight bool,
) {
	if p == nil || p.Context.ModelExecution == nil {
		return
	}
	p.Context.ModelExecution.updateAutoFallbackSelection(
		routeSessionKey,
		selectedCandidates,
		result,
		usedLight,
	)
}
