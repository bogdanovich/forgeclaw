package agent

import (
	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/logger"
)

func installDurableProjectionSource[Snapshot any](
	coordinator *evalcapture.Coordinator,
	sourceID string,
	source evalcapture.DurableSource,
	maxEvents int,
	protect func(int) error,
	subscribe func() (Snapshot, func(), func()),
) (Snapshot, func(), func(), string, error) {
	var empty Snapshot
	if err := protect(maxEvents); err != nil {
		return empty, nil, nil, "protect_snapshot", err
	}
	if err := coordinator.RegisterSource(sourceID, source); err != nil {
		return empty, nil, nil, "register_source", err
	}
	snapshot, activate, unsubscribe := subscribe()
	return snapshot, activate, unsubscribe, "", nil
}

func bindDurableProjectionSource[
	Source evalcapture.DurableSource,
	Snapshot any,
](
	eligible bool,
	kind string,
	workspace string,
	coordinator *evalcapture.Coordinator,
	sourceID string,
	source Source,
	maxEvents int,
	protect func(int) error,
	subscribe func() (Snapshot, func(), func()),
	bind func(Source, func()),
	request func(string, Snapshot),
	scheduleRetry func(),
) func() {
	if !eligible {
		return nil
	}
	snapshot, activate, unsubscribe, stage, err := installDurableProjectionSource[Snapshot](
		coordinator,
		sourceID,
		source,
		maxEvents,
		protect,
		subscribe,
	)
	if err != nil {
		scheduleRetry()
		logDurableProjectionInstallFailure(
			kind, workspace, stage, err,
		)
		return nil
	}
	bind(source, unsubscribe)
	request(sourceID, snapshot)
	return activate
}

func logDurableProjectionInstallFailure(
	kind, workspace, stage string,
	err error,
) {
	logger.WarnCF("evaltrace", "Failed to install durable trace source", map[string]any{
		"kind":      kind,
		"workspace": workspace,
		"stage":     stage,
		"error":     err.Error(),
	})
}
