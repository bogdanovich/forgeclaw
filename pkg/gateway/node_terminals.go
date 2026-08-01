package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
)

const gatewayTerminalPlanTTL = time.Minute

type nodeTerminalHandler interface {
	OpenTerminal(
		context.Context,
		nodes.ID,
		nodes.TerminalOpenPlan,
		func() error,
	) (nodes.TerminalMetadata, bool, error)
	AttachTerminal(
		context.Context,
		nodes.ID,
		nodes.TerminalSessionRequest,
	) (*nodews.TerminalStream, nodes.TerminalMetadata, error)
	TerminalStatus(
		context.Context,
		nodes.ID,
		nodes.TerminalSessionRequest,
	) (nodes.TerminalMetadata, error)
	TerminateTerminal(
		context.Context,
		nodes.ID,
		nodes.TerminalSessionRequest,
	) (nodes.TerminalMetadata, error)
}

type nodeTerminalSource struct {
	nodeDiscoverySource
	store      *nodes.GatewayTerminalStore
	generation uint64
	now        func() time.Time
}

// nodeTerminalHubSource pins deterministic operator opens to the exact hub
// generation serving their authenticated HTTP request. The model-facing
// source continues to resolve the current runtime hub for its own sessions.
type nodeTerminalHubSource struct {
	*nodeTerminalSource
	hub *nodeTerminalOperatorHub
}

func (source *nodeTerminalHubSource) BindTerminalOperator(
	owner nodes.TerminalOwner,
	terminalID string,
	operatorSessionID string,
) error {
	if source == nil || source.nodeTerminalSource == nil || source.hub == nil {
		return errNodeDiscoveryAuthorityUnavailable
	}
	return source.hub.bind(source.nodeTerminalSource, owner, terminalID, operatorSessionID)
}

func newNodeTerminalSource(
	cfg *config.Config,
	runtime *nodeAdmissionRuntime,
) (*nodeTerminalSource, error) {
	if cfg == nil || runtime == nil {
		return nil, nil
	}
	workspace := cfg.WorkspacePath()
	storePath := nodes.GatewayTerminalStorePath(workspace)
	enabled := cfg.Nodes.Enabled && cfg.Nodes.TerminalEnabled
	token, _, err := terminalOperatorAuthentication(cfg)
	if err != nil {
		return nil, err
	}
	if !enabled || token == "" {
		if configureErr := runtime.configureTerminalOperator(nil, nil); configureErr != nil {
			return nil, configureErr
		}
		_, _, err = runtime.existingGatewayTerminalStore(
			storePath,
			nodes.DefaultGatewayTerminalLimit,
			nodes.DefaultGatewayTerminalStoreBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("recover disabled gateway terminal store: %w", err)
		}
		return nil, nil
	}
	registryPath := nodes.RegistryPath(workspace)
	generation := runtime.invocationGeneration()
	if _, snapshotErr := runtime.terminalHandlerSnapshot(registryPath, generation); snapshotErr != nil {
		return nil, snapshotErr
	}
	store, err := runtime.gatewayTerminalStore(
		storePath,
		nodes.DefaultGatewayTerminalLimit,
		nodes.DefaultGatewayTerminalStoreBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("open gateway terminal store: %w", err)
	}
	source := &nodeTerminalSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime:      runtime,
			registryPath: registryPath,
		},
		store:      store,
		generation: generation,
		now:        time.Now,
	}
	if err := runtime.configureTerminalOperator(cfg, source); err != nil {
		return nil, err
	}
	return source, nil
}

func (source *nodeTerminalSource) PrepareTerminal(
	nodeID nodes.ID,
	nodeRef string,
	openID string,
	idempotencyKey string,
	owner nodes.TerminalOwner,
	workingScope string,
	columns int,
	rows int,
	allowCreate bool,
) (nodes.GatewayTerminalRecord, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.GatewayTerminalRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	handler, err := source.runtime.invocationHandlerSnapshot(
		source.registryPath,
		source.generation,
	)
	if err != nil {
		return nodes.GatewayTerminalRecord{}, false, err
	}
	var (
		record             nodes.GatewayTerminalRecord
		created            bool
		authorityValidated bool
	)
	_, err = handler.WithPreparationAuthority(
		nodeID,
		nodeRef,
		"shell.exec.v1",
		func(
			registration nodes.Registration,
			approval nodes.CommandApproval,
		) error {
			contract := approval.Descriptor.ModelContract
			if registration.Snapshot.ID != nodeID ||
				approval.Descriptor.Name != "shell.exec.v1" ||
				approval.Descriptor.Risk != nodes.RiskPrivileged ||
				contract == nil ||
				contract.Availability != nodes.ModelAvailable ||
				contract.ApprovalMode != "each_command" ||
				!slices.Contains(contract.Constraints.ProfileAliases, owner.Profile) ||
				!slices.Contains(contract.Constraints.WorkingScopes, workingScope) {
				return nodes.ErrCommandDenied
			}
			return source.runtime.withInvocationHandler(
				source.registryPath,
				source.generation,
				func(current nodeAdmissionHandler) error {
					if current != handler {
						return errNodeDiscoveryAuthorityUnavailable
					}
					authorityValidated = true
					existing, found, lookupErr := source.store.Lookup(owner, openID)
					if lookupErr != nil {
						return lookupErr
					}
					if found {
						if !terminalPreparationMatches(
							existing,
							nodeID,
							openID,
							idempotencyKey,
							owner,
							approval.CatalogHash,
							contract.AuthorityDigest,
							workingScope,
							columns,
							rows,
						) {
							return nodes.ErrGatewayTerminalConflict
						}
						record = existing
						created = false
						return nil
					}
					if !allowCreate {
						return nodes.ErrGatewayTerminalNotFound
					}
					plan, planErr := nodes.PrepareTerminalOpenPlan(nodes.TerminalOpenPlan{
						OpenID:          openID,
						IdempotencyKey:  idempotencyKey,
						NodeID:          nodeID,
						Owner:           owner,
						CatalogHash:     approval.CatalogHash,
						AuthorityDigest: contract.AuthorityDigest,
						WorkingScope:    workingScope,
						Columns:         columns,
						Rows:            rows,
						ApprovalMode:    "session_start",
					}, source.now(), gatewayTerminalPlanTTL)
					if planErr != nil {
						return planErr
					}
					var storeErr error
					record, created, storeErr = source.store.Prepare(plan)
					return storeErr
				},
			)
		},
	)
	if err != nil &&
		!errors.Is(err, errNodeDiscoveryAuthorityUnavailable) &&
		!authorityValidated {
		return nodes.GatewayTerminalRecord{}, false, fmt.Errorf(
			"terminal preparation authority changed: %w",
			err,
		)
	}
	return record, created, err
}

func terminalPreparationMatches(
	record nodes.GatewayTerminalRecord,
	nodeID nodes.ID,
	openID string,
	idempotencyKey string,
	owner nodes.TerminalOwner,
	catalogHash string,
	authorityDigest string,
	workingScope string,
	columns int,
	rows int,
) bool {
	plan := record.Plan
	return plan.NodeID == nodeID &&
		plan.OpenID == openID &&
		plan.IdempotencyKey == idempotencyKey &&
		plan.Owner == owner &&
		plan.CatalogHash == catalogHash &&
		plan.AuthorityDigest == authorityDigest &&
		plan.WorkingScope == workingScope &&
		plan.Columns == columns &&
		plan.Rows == rows &&
		plan.ApprovalMode == "session_start"
}

func (source *nodeTerminalSource) OpenTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	openID string,
	expectedPlanHash string,
) (nodes.TerminalMetadata, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.TerminalMetadata{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	handler, terminalHandler, record, err := source.terminalRecordSnapshot(owner, openID)
	if err != nil {
		return nodes.TerminalMetadata{}, false, err
	}
	if record.State != nodes.GatewayTerminalPrepared ||
		record.ExpectedPlanHash != expectedPlanHash {
		return nodes.TerminalMetadata{}, false, nodes.ErrGatewayTerminalConflict
	}
	metadata, dispatched, openErr := terminalHandler.OpenTerminal(
		ctx,
		record.Plan.NodeID,
		record.Plan,
		func() error {
			return source.runtime.withInvocationHandler(
				source.registryPath,
				source.generation,
				func(current nodeAdmissionHandler) error {
					if current != handler {
						return errNodeDiscoveryAuthorityUnavailable
					}
					_, transitioned, markErr := source.store.MarkDispatched(
						owner,
						openID,
						expectedPlanHash,
					)
					if markErr != nil {
						return markErr
					}
					if !transitioned {
						return nodes.ErrGatewayTerminalConflict
					}
					return nil
				},
			)
		},
	)
	if openErr != nil && (metadata.TerminalID == "" || !dispatched) {
		return nodes.TerminalMetadata{}, dispatched, openErr
	}
	var persistErr error
	runtimeErr := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			if current != handler {
				return errNodeDiscoveryAuthorityUnavailable
			}
			_, _, persistErr = source.store.RecordOpened(owner, openID, metadata)
			return persistErr
		},
	)
	if persistErr == nil {
		persistErr = runtimeErr
	}
	openedRetained := persistErr == nil
	if persistErr == nil && openErr == nil {
		return metadata, true, nil
	}
	request := nodes.TerminalSessionRequest{
		TerminalID: metadata.TerminalID,
		Owner:      owner,
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		nodeAdmissionDrainTimeout,
	)
	defer cleanupCancel()
	cleanupMetadata, cleanupErr := terminalHandler.TerminateTerminal(
		cleanupCtx,
		record.Plan.NodeID,
		request,
	)
	if cleanupErr == nil &&
		(cleanupMetadata.State == string(nodes.GatewayTerminalClosed) ||
			cleanupMetadata.State == string(nodes.GatewayTerminalUnknown)) {
		if openedRetained || fileutil.IsCommittedWriteError(persistErr) {
			_, _, lifecycleErr := source.store.RecordLifecycle(
				owner,
				metadata.TerminalID,
				cleanupMetadata,
			)
			persistErr = errors.Join(persistErr, lifecycleErr)
		}
	}
	return nodes.TerminalMetadata{}, true, errors.Join(
		errors.New("retain opened terminal authority"),
		openErr,
		persistErr,
		cleanupErr,
	)
}

func (source *nodeTerminalSource) AttachTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (*nodews.TerminalStream, nodes.TerminalMetadata, error) {
	handler, terminalHandler, record, err := source.terminalRecordSnapshot(owner, terminalID)
	if err != nil {
		return nil, nodes.TerminalMetadata{}, err
	}
	if record.TerminalID != terminalID ||
		record.State != nodes.GatewayTerminalPendingAttach {
		return nil, nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	request := nodes.TerminalSessionRequest{TerminalID: terminalID, Owner: owner}
	stream, metadata, err := terminalHandler.AttachTerminal(ctx, record.Plan.NodeID, request)
	if err != nil {
		return nil, nodes.TerminalMetadata{}, err
	}
	persistErr := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			if current != handler {
				return errNodeDiscoveryAuthorityUnavailable
			}
			_, _, recordErr := source.store.RecordLifecycle(owner, terminalID, metadata)
			return recordErr
		},
	)
	if persistErr != nil {
		return nil, nodes.TerminalMetadata{}, errors.Join(
			persistErr,
			stream.Close(context.WithoutCancel(ctx)),
		)
	}
	return stream, metadata, nil
}

func (source *nodeTerminalSource) attachOperatorTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodeTerminalOperatorStream, nodes.TerminalMetadata, error) {
	return source.AttachTerminal(ctx, owner, terminalID)
}

func (source *nodeTerminalSource) terminalOperatorStatus(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	return source.TerminalStatus(ctx, owner, terminalID)
}

func (source *nodeTerminalSource) closeOperatorTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	return source.CloseTerminal(ctx, owner, terminalID)
}

func (source *nodeTerminalSource) BindTerminalOperator(
	owner nodes.TerminalOwner,
	terminalID string,
	operatorSessionID string,
) error {
	if source == nil || source.runtime == nil {
		return errNodeDiscoveryAuthorityUnavailable
	}
	hub := source.runtime.terminalOperatorHub()
	if hub == nil {
		return errNodeDiscoveryAuthorityUnavailable
	}
	return hub.bind(source, owner, terminalID, operatorSessionID)
}

func (source *nodeTerminalSource) TerminalStatus(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	handler, terminalHandler, record, err := source.terminalRecordSnapshot(owner, terminalID)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	if record.TerminalID != terminalID ||
		record.State == nodes.GatewayTerminalPrepared ||
		record.State == nodes.GatewayTerminalDispatched {
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	request := nodes.TerminalSessionRequest{TerminalID: terminalID, Owner: owner}
	metadata, err := terminalHandler.TerminalStatus(ctx, record.Plan.NodeID, request)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	err = source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			if current != handler {
				return errNodeDiscoveryAuthorityUnavailable
			}
			if metadata.State == string(nodes.GatewayTerminalPendingAttach) {
				_, _, persistErr := source.store.RecordOpened(
					owner,
					record.Plan.OpenID,
					metadata,
				)
				return persistErr
			}
			_, _, persistErr := source.store.RecordLifecycle(
				owner,
				terminalID,
				metadata,
			)
			return persistErr
		},
	)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	return metadata, nil
}

func (source *nodeTerminalSource) SignalTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
	signal string,
) (nodes.TerminalMetadata, error) {
	if source == nil || source.runtime == nil {
		return nodes.TerminalMetadata{}, errNodeDiscoveryAuthorityUnavailable
	}
	hub := source.runtime.terminalOperatorHub()
	if hub == nil {
		return nodes.TerminalMetadata{}, errNodeDiscoveryAuthorityUnavailable
	}
	if err := hub.signal(ctx, owner, terminalID, signal); err != nil {
		return nodes.TerminalMetadata{}, err
	}
	return source.TerminalStatus(ctx, owner, terminalID)
}

func (source *nodeTerminalSource) CloseTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	handler, terminalHandler, record, err := source.terminalRecordSnapshot(owner, terminalID)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	if record.TerminalID != terminalID ||
		record.State == nodes.GatewayTerminalPrepared ||
		record.State == nodes.GatewayTerminalDispatched {
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	if hub := source.runtime.terminalOperatorHub(); hub != nil {
		if closeErr := hub.closeTerminal(ctx, owner, terminalID); closeErr == nil {
			return source.TerminalStatus(ctx, owner, terminalID)
		} else if !errors.Is(closeErr, nodes.ErrGatewayTerminalNotFound) {
			return nodes.TerminalMetadata{}, closeErr
		}
		hub.unbind(owner, terminalID)
	}
	request := nodes.TerminalSessionRequest{TerminalID: terminalID, Owner: owner}
	metadata, err := terminalHandler.TerminateTerminal(ctx, record.Plan.NodeID, request)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	err = source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			if current != handler {
				return errNodeDiscoveryAuthorityUnavailable
			}
			_, _, persistErr := source.store.RecordLifecycle(owner, terminalID, metadata)
			return persistErr
		},
	)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	return metadata, nil
}

func (source *nodeTerminalSource) terminalRecordSnapshot(
	owner nodes.TerminalOwner,
	terminalRef string,
) (nodeAdmissionHandler, nodeTerminalHandler, nodes.GatewayTerminalRecord, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nil, nil, nodes.GatewayTerminalRecord{}, errNodeDiscoveryAuthorityUnavailable
	}
	handler, err := source.runtime.invocationHandlerSnapshot(
		source.registryPath,
		source.generation,
	)
	if err != nil {
		return nil, nil, nodes.GatewayTerminalRecord{}, err
	}
	terminalHandler, ok := handler.(nodeTerminalHandler)
	if !ok {
		return nil, nil, nodes.GatewayTerminalRecord{}, errNodeDiscoveryAuthorityUnavailable
	}
	record, found, err := source.store.Lookup(owner, terminalRef)
	if err != nil {
		return nil, nil, nodes.GatewayTerminalRecord{}, err
	}
	if !found {
		return nil, nil, nodes.GatewayTerminalRecord{}, nodes.ErrGatewayTerminalConflict
	}
	if err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			if current != handler {
				return errNodeDiscoveryAuthorityUnavailable
			}
			return nil
		},
	); err != nil {
		return nil, nil, nodes.GatewayTerminalRecord{}, err
	}
	return handler, terminalHandler, record, nil
}
