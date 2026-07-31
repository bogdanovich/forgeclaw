package agent

import (
	"errors"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

type nodeFileMediaOwnerBinder interface {
	BindOwner(ref string, owner media.MediaOwner) error
}

func bindNodeFileMediaOwner(
	resolver mediaResolver,
	ts *turnState,
	refs []string,
) error {
	if len(refs) == 0 || resolver == nil || ts == nil || ts.agent == nil ||
		ts.agent.Tools == nil || !ts.agent.Tools.HasRegistered("nodes_upload") {
		return nil
	}
	binder, ok := resolver.(nodeFileMediaOwnerBinder)
	if !ok {
		return errors.New("media store does not support durable owner binding")
	}
	owner, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		return err
	}
	var bindErr error
	for _, ref := range refs {
		if !strings.HasPrefix(strings.TrimSpace(ref), "media://") {
			continue
		}
		if err := binder.BindOwner(ref, owner); err != nil {
			bindErr = errors.Join(bindErr, err)
		}
	}
	return bindErr
}

func nodeFileMediaOwnerForTurn(ts *turnState) (media.MediaOwner, error) {
	if ts == nil || ts.agent == nil {
		return media.MediaOwner{}, errors.New("turn media owner is unavailable")
	}
	actorID := ""
	topicID := ""
	if inbound := ts.opts.Dispatch.InboundContext; inbound != nil {
		actorID = strings.TrimSpace(inbound.ActorID)
		if actorID == "" {
			actorID = strings.TrimSpace(inbound.SenderID)
		}
		topicID = originTopicID(inbound)
	}
	if actorID == "" {
		actorID = strings.TrimSpace(ts.opts.SenderID)
	}
	if actorID == "" {
		actorID = ts.agent.ID
	}
	routeSession := strings.TrimSpace(ts.opts.Dispatch.RouteSessionKey)
	if routeSession == "" {
		routeSession = strings.TrimSpace(ts.opts.Dispatch.SessionKey)
	}
	return media.NewMediaOwner(
		ts.workspace,
		ts.agent.ID,
		actorID,
		routeSession,
		ts.channel,
		ts.chatID,
		topicID,
	)
}

func projectNodeFileMediaAttachments(
	messages []providers.Message,
	ts *turnState,
	refs []string,
	resolver mediaResolver,
) []providers.Message {
	if len(messages) == 0 || len(refs) == 0 || resolver == nil || ts == nil ||
		ts.agent == nil || ts.agent.Tools == nil ||
		!ts.agent.Tools.HasRegistered("nodes_upload") {
		return messages
	}
	allowed := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if strings.HasPrefix(strings.TrimSpace(ref), "media://") {
			allowed[ref] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return messages
	}
	projected := append([]providers.Message(nil), messages...)
	for index := range projected {
		if projected[index].Role != "user" || len(projected[index].Media) == 0 {
			continue
		}
		for _, ref := range projected[index].Media {
			if _, ok := allowed[ref]; !ok || providerAttachmentHasRef(projected[index].Attachments, ref) {
				continue
			}
			attachments := buildProviderAttachments(resolver, []string{ref})
			if len(attachments) == 1 {
				projected[index].Attachments = append(projected[index].Attachments, attachments[0])
			}
		}
	}
	return projected
}

func providerAttachmentHasRef(attachments []providers.Attachment, ref string) bool {
	for _, attachment := range attachments {
		if attachment.Ref == ref {
			return true
		}
	}
	return false
}
