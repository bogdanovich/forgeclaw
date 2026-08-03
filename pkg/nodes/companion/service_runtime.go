package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type serviceCommandHandler struct {
	command         string
	descriptorValue nodes.CommandDescriptor
	manager         ServiceManager
	controller      ServiceController
}

type serviceStatusInput struct {
	Service string `json:"service"`
}

type serviceLogsInput struct {
	Service      string  `json:"service"`
	Entries      float64 `json:"entries,omitempty"`
	SinceSeconds float64 `json:"since_seconds,omitempty"`
}

type serviceActionInput struct {
	Service string              `json:"service"`
	Action  nodes.ServiceAction `json:"action"`
}

func newServiceCommandHandlers(
	manager ServiceManager,
	policy nodes.LocalCommandPolicy,
) ([]commandHandler, error) {
	descriptors := manager.Descriptors()
	handlers := make([]commandHandler, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if !nodes.IsServiceCommand(descriptor.Name) {
			return nil, fmt.Errorf("manager returned non-service capability %q", descriptor.Name)
		}
		if !slices.Contains(policy.AllowedCommands, descriptor.Name) ||
			modelRiskRank(descriptor.Risk) > modelRiskRank(policy.MaximumRisk) {
			continue
		}
		if descriptor.ModelContract == nil {
			return nil, fmt.Errorf("service capability %q has no model contract", descriptor.Name)
		}
		contract := *descriptor.ModelContract
		contract.TimeoutSecondsMax = min(contract.TimeoutSecondsMax, policy.MaxTimeoutSeconds)
		contract.OutputBytesMax = min(contract.OutputBytesMax, policy.MaxOutputBytes)
		descriptor.ModelContract = &contract
		if err := descriptor.Validate(); err != nil {
			return nil, err
		}
		handler := &serviceCommandHandler{
			command:         descriptor.Name,
			descriptorValue: descriptor,
			manager:         manager,
		}
		if descriptor.Name == "service.action.v1" {
			controller, ok := manager.(ServiceController)
			if !ok {
				return nil, errors.New("service action capability lacks controller enforcement")
			}
			handler.controller = controller
		}
		handlers = append(handlers, handler)
	}
	return handlers, nil
}

func (handler *serviceCommandHandler) descriptor() nodes.CommandDescriptor {
	return handler.descriptorValue
}

func (handler *serviceCommandHandler) authorize(plan nodes.ExecutionPlan) error {
	if plan.ServiceProfile == "" {
		return errors.New("service profile is required")
	}
	_, err := handler.prepare(plan)
	return err
}

func (handler *serviceCommandHandler) execute(
	ctx context.Context,
	invocation commandInvocation,
) (any, error) {
	request, err := handler.prepare(invocation.Plan)
	if err != nil {
		return nil, newCommandFailure("COMMAND_DENIED", "service command input denied", err)
	}
	switch typed := request.(type) {
	case ServiceStatusRequest:
		result, statusErr := handler.manager.Status(ctx, typed)
		return result, serviceCommandError(ctx, statusErr)
	case ServiceLogRequest:
		result, logsErr := handler.manager.Logs(ctx, typed)
		if logsErr != nil {
			return nil, serviceCommandError(ctx, logsErr)
		}
		result, logsErr = boundServiceLogs(result, invocation.OutputLimitBytes)
		return result, serviceCommandError(ctx, logsErr)
	case ServiceActionRequest:
		if envelopeErr := handler.validateActionOutputEnvelope(
			typed,
			invocation.OutputLimitBytes,
		); envelopeErr != nil {
			return nil, newCommandFailure(
				"OUTPUT_LIMIT_TOO_SMALL",
				"service action output limit is too small",
				envelopeErr,
			)
		}
		result, actionErr := handler.controller.Action(ctx, typed)
		if actionErr != nil {
			return nil, serviceCommandError(ctx, actionErr)
		}
		switch result.State {
		case "completed":
			return result, nil
		case "unknown":
			return nil, fmt.Errorf("%w: service action outcome is uncertain", ErrInvocationOutcomeUnknown)
		case "canceled":
			return nil, fmt.Errorf("%w: service action canceled", errCommandCancellationConfirmed)
		case "failed":
			return nil, newCommandFailure("SERVICE_ACTION_FAILED", "service action failed", nil)
		default:
			return nil, fmt.Errorf("%w: service action returned invalid state", ErrInvocationOutcomeUnknown)
		}
	default:
		return nil, ErrCommandUnavailable
	}
}

func (handler *serviceCommandHandler) validateActionOutputEnvelope(
	request ServiceActionRequest,
	bytesMax int,
) error {
	minimum := ServiceActionResult{
		Service: request.Service, Action: request.Action,
		State: "unknown", AcceptedAt: 1, Code: "unknown",
	}
	raw, err := json.Marshal(minimum)
	if err != nil {
		return err
	}
	_, err = nodes.ValidateInvocationOutput(handler.descriptorValue, raw, bytesMax)
	return err
}

func boundServiceLogs(logs ServiceLogs, bytesMax int) (ServiceLogs, error) {
	if serviceLogsFit(logs, bytesMax) {
		return logs, nil
	}
	logs.Truncated = true
	for len(logs.Records) > 0 {
		logs.Records = logs.Records[:len(logs.Records)-1]
		if serviceLogsFit(logs, bytesMax) {
			return logs, nil
		}
	}
	if serviceLogsFit(logs, bytesMax) {
		return logs, nil
	}
	return ServiceLogs{}, &ServiceManagerError{Code: "output_limit_too_small"}
}

func (handler *serviceCommandHandler) prepare(plan nodes.ExecutionPlan) (any, error) {
	switch handler.command {
	case "service.status.v1":
		var input serviceStatusInput
		if err := decodeStrictJSON(plan.Input, &input); err != nil {
			return nil, err
		}
		return ServiceStatusRequest{Profile: plan.ServiceProfile, Service: input.Service}, nil
	case "service.logs.v1":
		var input serviceLogsInput
		if err := decodeStrictJSON(plan.Input, &input); err != nil {
			return nil, err
		}
		if input.Entries < 0 || input.SinceSeconds < 0 ||
			math.Trunc(input.Entries) != input.Entries ||
			math.Trunc(input.SinceSeconds) != input.SinceSeconds {
			return nil, errors.New("service log bounds are invalid")
		}
		return ServiceLogRequest{
			Profile: plan.ServiceProfile, Service: input.Service,
			Entries: int(input.Entries), SinceSeconds: int(input.SinceSeconds),
		}, nil
	case "service.action.v1":
		var input serviceActionInput
		if err := decodeStrictJSON(plan.Input, &input); err != nil {
			return nil, err
		}
		return ServiceActionRequest{
			Profile: plan.ServiceProfile, Service: input.Service, Action: input.Action,
		}, nil
	default:
		return nil, ErrCommandUnavailable
	}
}

func serviceCommandError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errCommandCancellationConfirmed) ||
		(errors.Is(err, context.Canceled) && context.Cause(ctx) != nil) {
		return fmt.Errorf("%w: service command canceled", errCommandCancellationConfirmed)
	}
	var managerErr *ServiceManagerError
	if errors.As(err, &managerErr) {
		if managerErr.Code == "request_canceled" {
			return fmt.Errorf("%w: service command canceled", errCommandCancellationConfirmed)
		}
		return newCommandFailure("SERVICE_MANAGER_FAILED", "service manager request failed", err)
	}
	return newCommandFailure("SERVICE_MANAGER_FAILED", "service manager request failed", err)
}
