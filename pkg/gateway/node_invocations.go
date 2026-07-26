package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/nodes"
)

type nodeInvocationSource struct {
	nodeDiscoverySource
	store      *nodes.GatewayInvocationStore
	generation uint64
}

func newNodeInvocationSource(
	cfg *config.Config,
	runtime *nodeAdmissionRuntime,
) (*nodeInvocationSource, error) {
	if cfg == nil || !cfg.Nodes.Enabled {
		return nil, nil
	}
	if runtime == nil {
		return nil, errNodeDiscoveryAuthorityUnavailable
	}
	workspace := cfg.WorkspacePath()
	registryPath := nodes.RegistryPath(workspace)
	generation := runtime.invocationGeneration()
	if err := runtime.withInvocationHandler(
		registryPath,
		generation,
		func(nodeAdmissionHandler) error { return nil },
	); err != nil {
		return nil, err
	}
	store, err := nodes.NewGatewayInvocationStore(
		nodes.GatewayInvocationStorePath(workspace),
		nodes.DefaultGatewayInvocationLimit,
		nodes.DefaultGatewayInvocationStoreBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("open gateway node invocation store: %w", err)
	}
	return &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime:      runtime,
			registryPath: registryPath,
		},
		store:      store,
		generation: generation,
	}, nil
}

func (source *nodeInvocationSource) PrepareInvocation(
	target string,
	toolCallID string,
	plan nodes.ExecutionPlan,
	descriptor nodes.CommandDescriptor,
) (nodes.GatewayInvocationRecord, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.GatewayInvocationRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	var (
		record  nodes.GatewayInvocationRecord
		created bool
	)
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(nodeAdmissionHandler) error {
			var prepareErr error
			record, created, prepareErr = source.store.Prepare(
				target,
				toolCallID,
				plan,
				descriptor,
			)
			return prepareErr
		},
	)
	return record, created, err
}

func (source *nodeInvocationSource) LookupInvocationByToolCall(
	principal nodes.GatewayInvocationPrincipal,
	toolCallID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.GatewayInvocationRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	var (
		record nodes.GatewayInvocationRecord
		found  bool
	)
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(nodeAdmissionHandler) error {
			var lookupErr error
			record, found, lookupErr = source.store.ByToolCall(principal, toolCallID)
			return lookupErr
		},
	)
	return record, found, err
}

func (source *nodeInvocationSource) LookupInvocation(
	principal nodes.GatewayInvocationPrincipal,
	invocationID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.GatewayInvocationRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	var (
		record nodes.GatewayInvocationRecord
		found  bool
	)
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(nodeAdmissionHandler) error {
			var lookupErr error
			record, found, lookupErr = source.store.Lookup(principal, invocationID)
			return lookupErr
		},
	)
	return record, found, err
}

func (source *nodeInvocationSource) DispatchInvocation(
	ctx context.Context,
	owner nodes.GatewayInvocationOwner,
	invocationID string,
	expectedPlanHash string,
) (json.RawMessage, bool, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nil, false, errNodeDiscoveryAuthorityUnavailable
	}
	var (
		handler nodeAdmissionHandler
		record  nodes.GatewayInvocationRecord
		found   bool
	)
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			var lookupErr error
			record, found, lookupErr = source.store.Lookup(
				nodes.GatewayInvocationPrincipal{
					AgentID:   owner.AgentID,
					SessionID: owner.SessionID,
					ActorID:   owner.ActorID,
				},
				invocationID,
			)
			if lookupErr == nil {
				handler = current
			}
			return lookupErr
		},
	)
	if err != nil {
		return nil, false, err
	}
	if !found ||
		!gatewayInvocationMatchesOwner(record, owner) ||
		record.ExpectedPlanHash != expectedPlanHash {
		return nil, false, nodes.ErrGatewayInvocationConflict
	}
	if record.State == nodes.GatewayInvocationDispatched {
		return nil, false, nodes.ErrGatewayInvocationDispatched
	}
	return handler.Invoke(
		ctx,
		record.Plan.NodeID,
		record.Plan,
		func() error {
			return source.runtime.withInvocationHandler(
				source.registryPath,
				source.generation,
				func(nodeAdmissionHandler) error {
					_, transitioned, markErr := source.store.MarkDispatched(
						owner,
						invocationID,
						expectedPlanHash,
					)
					if markErr != nil {
						return markErr
					}
					if !transitioned {
						return nodes.ErrGatewayInvocationDispatched
					}
					return nil
				},
			)
		},
	)
}

func gatewayInvocationMatchesOwner(
	record nodes.GatewayInvocationRecord,
	owner nodes.GatewayInvocationOwner,
) bool {
	return record.Target == owner.Target &&
		record.Plan.AgentID == owner.AgentID &&
		record.Plan.SessionID == owner.SessionID &&
		record.Plan.ActorID == owner.ActorID &&
		record.ToolCallID == owner.ToolCallID
}

func (source *nodeInvocationSource) QueryInvocation(
	ctx context.Context,
	principal nodes.GatewayInvocationPrincipal,
	target string,
	nodeID nodes.ID,
	invocationID string,
) (nodes.InvocationRecord, error) {
	if source == nil || source.store == nil || source.runtime == nil {
		return nodes.InvocationRecord{}, errNodeDiscoveryAuthorityUnavailable
	}
	var (
		handler nodeAdmissionHandler
		record  nodes.GatewayInvocationRecord
		found   bool
	)
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(current nodeAdmissionHandler) error {
			var lookupErr error
			record, found, lookupErr = source.store.Lookup(principal, invocationID)
			if lookupErr == nil {
				handler = current
			}
			return lookupErr
		},
	)
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if !found ||
		record.Target != target ||
		record.Plan.NodeID != nodeID ||
		record.State != nodes.GatewayInvocationDispatched {
		return nodes.InvocationRecord{}, nodes.ErrGatewayInvocationConflict
	}
	remote, err := handler.Invocation(ctx, nodeID, invocationID)
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if err := verifyRemoteInvocation(record, &remote); err != nil {
		return nodes.InvocationRecord{}, err
	}
	return remote, nil
}

func verifyRemoteInvocation(
	gateway nodes.GatewayInvocationRecord,
	remote *nodes.InvocationRecord,
) error {
	if remote == nil {
		return nodes.ErrGatewayInvocationConflict
	}
	if err := remote.Validate(); err != nil ||
		remote.InvocationID != gateway.Plan.InvocationID ||
		remote.IdempotencyKey != gateway.Plan.IdempotencyKey ||
		remote.PlanHash != gateway.ExpectedPlanHash ||
		remote.NodeID != gateway.Plan.NodeID ||
		remote.CatalogHash != gateway.Plan.CatalogHash ||
		remote.Command != gateway.Plan.Command ||
		remote.Risk != gateway.Plan.Risk {
		return nodes.ErrGatewayInvocationConflict
	}
	if remote.State == nodes.InvocationSucceeded {
		result, err := nodes.ValidateInvocationOutput(
			gateway.Descriptor,
			remote.Result,
			gateway.Plan.OutputLimitBytes,
		)
		if err != nil {
			return nodes.ErrGatewayInvocationConflict
		}
		remote.Result = result
	}
	return nil
}
