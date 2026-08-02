package bus

// InboundMetadataKeyInteractionChoice carries a channel-validated, bounded
// interaction choice separately from any quoted context added to Content.
const InboundMetadataKeyInteractionChoice = "interaction_choice"

const (
	InboundInteractionChoiceAllowOnce = "allow_once"
	InboundInteractionChoiceDeny      = "deny"
)
