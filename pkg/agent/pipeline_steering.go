// PicoClaw - Ultra-lightweight personal AI agent

package agent

import "github.com/sipeed/picoclaw/pkg/providers"

func (p *Pipeline) dequeueSteeringMessagesForTurn(ts *turnState) []providers.Message {
	if p == nil || p.Steering == nil || ts == nil {
		return nil
	}
	return p.Steering.dequeueSteeringMessagesForTurn(ts.sessionKey, ts.opts.Dispatch.SenderID())
}
