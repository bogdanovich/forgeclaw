package seahorse

import (
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tokenizer"
)

const retrievalToolMaxTokens = 16 * 1024

func estimateRetrievalResultTokens(data []byte) int {
	return tokenizer.EstimateMessageTokens(providers.Message{Role: "tool", Content: string(data)})
}
