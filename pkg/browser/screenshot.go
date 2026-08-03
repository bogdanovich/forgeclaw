package browser

import (
	"bytes"
	"context"
	"fmt"
)

var pngSignature = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}

// CaptureScreenshot captures one PNG only for the exact fresh observation
// owned by the caller. Artifact persistence and routed delivery remain gateway
// responsibilities outside the broker.
func (broker *Broker) CaptureScreenshot(
	ctx context.Context,
	request ScreenshotRequest,
) (ScreenshotCapture, error) {
	if request.Owner.Validate() != nil || !validIdentifier(request.RequestID) ||
		!validIdentifier(request.SessionID) ||
		!validIdentifier(request.TabID) || !validIdentifier(request.SnapshotID) ||
		request.SnapshotGeneration == 0 {
		return ScreenshotCapture{}, fmt.Errorf("%w: malformed screenshot request", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, _, worker, err := broker.actionSessionLocked(
		ctx, request.Owner, request.SessionID, request.TabID,
	)
	if err != nil {
		return ScreenshotCapture{}, err
	}
	if session.SnapshotID != request.SnapshotID ||
		session.SnapshotGeneration != request.SnapshotGeneration {
		return ScreenshotCapture{}, ErrStale
	}
	screenshotWorker, ok := worker.(ScreenshotWorker)
	if !ok {
		return ScreenshotCapture{}, ErrDriverIncompatible
	}
	maximum := broker.config.Limits.Effective().ScreenshotBytes
	screenshot, err := screenshotWorker.CaptureScreenshot(ctx, maximum)
	if err != nil {
		return ScreenshotCapture{}, err
	}
	if screenshot.ContentType != "image/png" || len(screenshot.Data) == 0 ||
		len(screenshot.Data) > maximum || !bytes.HasPrefix(screenshot.Data, pngSignature) {
		return ScreenshotCapture{}, ErrDriverIncompatible
	}
	return ScreenshotCapture{
		SessionID: session.ID, Target: session.Target, Profile: session.Profile,
		PolicyRevision: session.PolicyRevision, TabID: session.TabID,
		SnapshotID: session.SnapshotID, SnapshotGeneration: session.SnapshotGeneration,
		Data: append([]byte(nil), screenshot.Data...), ContentType: screenshot.ContentType,
	}, nil
}
