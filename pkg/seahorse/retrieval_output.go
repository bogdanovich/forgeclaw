package seahorse

import (
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/tokenizer"
)

const retrievalToolMaxTokens = 16 * 1024

func estimateRetrievalResultTokens(data []byte) int {
	return tokenizer.EstimateMessageTokens(providers.Message{Role: "tool", Content: string(data)})
}
