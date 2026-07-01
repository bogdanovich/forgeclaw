// PicoClaw - Ultra-lightweight personal AI agent

package agent

func (p *Pipeline) abortTurn(ts *turnState) (turnResult, error) {
	if p == nil || p.TurnControl == nil {
		return turnResult{status: TurnEndStatusAborted}, nil
	}
	return p.TurnControl.abortTurn(ts)
}
