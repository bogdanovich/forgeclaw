// PicoClaw - Ultra-lightweight personal AI agent

package agent

func (p *Pipeline) abortTurn(ts *turnState) (turnResult, error) {
	if p == nil || p.Runtime.TurnControl == nil {
		return turnResult{status: TurnEndStatusAborted}, nil
	}
	return p.Runtime.TurnControl.abortTurn(ts)
}
