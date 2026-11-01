package schedule

import (
	"fmt"
	"math"
	"strconv"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func (w *Workflow) Start(instanceID string) (*pb.Transition, error) {
	if instanceID == "" || !validText(instanceID) {
		return nil, fmt.Errorf("%w: instance ID is required", ErrInvalidEvent)
	}

	instance := &pb.Instance{
		Id:             instanceID,
		DefinitionHash: w.hash,
		Revision:       1,
		Mode:           modeRunning,
		Nodes:          make([]*pb.NodeState, len(w.nodes)),
	}
	for i, node := range w.nodes {
		instance.Nodes[i] = &pb.NodeState{
			NodeId: node.Id,
			Phase:  pending,
		}
	}

	w.reconcile(instance)

	return w.transition(nil, instance), nil
}

// Restore 验证完整快照并返回副本；未确认命令由接入方按原 ID 补发。
func (w *Workflow) Restore(instance *pb.Instance) (*pb.Transition, error) {
	if err := w.validate(instance); err != nil {
		return nil, err
	}
	return w.view(proto.Clone(instance).(*pb.Instance)), nil
}

// Advance 只计算状态和意图，不执行 I/O。事件 ID 的持久化去重及版本 CAS 由接入方完成。
func (w *Workflow) Advance(instance *pb.Instance, event *pb.Event) (*pb.Transition, error) {
	if err := w.validate(instance); err != nil {
		return nil, err
	}

	if event == nil || event.Id == "" || event.InstanceId != instance.Id ||
		!validText(event.Id, event.ResultRef, event.GetErrorMessage()) {
		return nil, fmt.Errorf("%w: event ID or instance binding", ErrInvalidEvent)
	}

	next := proto.Clone(instance).(*pb.Instance)
	if event.Kind >= pb.EventKind_EVENT_KIND_PAUSE && event.Kind <= pb.EventKind_EVENT_KIND_STOP {
		if event.NodeId != "" || event.ExecutionId != "" || event.ErrorMessage != nil || event.ResultRef != "" {
			return nil, fmt.Errorf("%w: control event must not carry a task report", ErrInvalidEvent)
		}
		if settled(instance) {
			return nil, fmt.Errorf("%w: instance is settled", ErrConflict)
		}
		if next.Mode == modeStopped && event.Kind != pb.EventKind_EVENT_KIND_STOP {
			return nil, fmt.Errorf("%w: stopped instance cannot pause or resume", ErrConflict)
		}

		switch event.Kind {
		case pb.EventKind_EVENT_KIND_PAUSE:
			next.Mode = modePaused
		case pb.EventKind_EVENT_KIND_RESUME:
			next.Mode = modeRunning
		case pb.EventKind_EVENT_KIND_STOP:
			next.Mode = modeStopped
		}
	} else {
		i, exists := w.byID[event.NodeId]
		if !exists || w.nodes[i].Kind != task ||
			event.ExecutionId == "" || event.ExecutionId != next.Nodes[i].ExecutionId {
			return nil, fmt.Errorf("%w: node or execution binding", ErrInvalidEvent)
		}
		state := next.Nodes[i]
		if state.Phase == pending || state.Phase == skipped {
			return nil, fmt.Errorf("%w: task %q was not dispatched", ErrInvalidEvent, event.NodeId)
		}

		var target pb.Phase
		switch event.Kind {
		case pb.EventKind_EVENT_KIND_STARTED:
			if event.ErrorMessage != nil || event.ResultRef != "" {
				return nil, fmt.Errorf("%w: start event has a result", ErrInvalidEvent)
			}
			if state.Phase == running || terminal(state.Phase) {
				return w.view(next), nil
			}
			target = running
		case pb.EventKind_EVENT_KIND_SUCCEEDED:
			if event.ErrorMessage != nil {
				return nil, fmt.Errorf("%w: success event has an error", ErrInvalidEvent)
			}
			target = succeeded
		case pb.EventKind_EVENT_KIND_FAILED:
			target = failed
		case pb.EventKind_EVENT_KIND_STOPPED:
			target = stopped
		default:
			return nil, fmt.Errorf("%w: unknown event kind", ErrInvalidEvent)
		}

		if terminal(state.Phase) {
			candidate := proto.Clone(state).(*pb.NodeState)
			candidate.Phase, candidate.ErrorMessage, candidate.ResultRef = target, event.ErrorMessage, event.ResultRef
			if proto.Equal(candidate, state) {
				return w.view(next), nil
			}
			return nil, fmt.Errorf("%w: task %q already has a different result", ErrConflict, event.NodeId)
		}

		state.Phase = target
		state.ErrorMessage = cloneString(event.ErrorMessage)
		state.ResultRef = event.ResultRef

		// 在激活任何后继之前，先向上传播结果，使祖先失败能及时阻止分派。
		for child, parent := i, w.parent[i]; parent >= 0; child, parent = parent, w.parent[parent] {
			w.resolve(next, parent, child)
		}
	}

	w.reconcile(next)
	if proto.Equal(instance, next) {
		return w.view(next), nil
	}
	if instance.Revision == math.MaxUint64 {
		return nil, fmt.Errorf("%w: revision exhausted", ErrConflict)
	}

	next.Revision++

	return w.transition(instance, next), nil
}

func (w *Workflow) view(instance *pb.Instance) *pb.Transition {
	return &pb.Transition{
		Instance:    instance,
		ResultReady: terminal(instance.Nodes[0].Phase),
		Settled:     settled(instance),
	}
}

func settled(instance *pb.Instance) bool {
	for _, state := range instance.Nodes {
		if !terminal(state.Phase) {
			return false
		}
	}
	return true
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	return proto.String(*value)
}

func (w *Workflow) transition(previous, instance *pb.Instance) *pb.Transition {
	result := w.view(instance)
	previousMode := pb.Mode_MODE_UNSPECIFIED
	if previous != nil {
		previousMode = previous.Mode
	}

	command := func(i int, kind pb.CommandKind, previousPhase pb.Phase) *pb.Command {
		return &pb.Command{
			InstanceId:     instance.Id,
			DefinitionHash: w.hash,
			Revision:       instance.Revision,
			Kind:           kind,
			State:          proto.Clone(instance.Nodes[i]).(*pb.NodeState),
			PreviousPhase:  previousPhase,
			Mode:           instance.Mode,
			PreviousMode:   previousMode,
			Path:           w.path(i),
		}
	}

	for i, state := range instance.Nodes {
		before := pending
		if previous != nil {
			before = previous.Nodes[i].Phase
		}
		changed := state.Phase != before
		if previous != nil {
			changed = !proto.Equal(state, previous.Nodes[i])
		}

		if changed || (i == 0 && previousMode != instance.Mode) {
			notification := command(i, pb.CommandKind_COMMAND_KIND_STATE, before)
			notification.Id = stableID("state", instance.Id, state.NodeId, strconv.FormatUint(instance.Revision, 10))
			result.Commands = append(result.Commands, notification)
		}
	}

	for i, state := range instance.Nodes {
		if state.Phase == ready && (previous == nil || previous.Nodes[i].Phase == pending) {
			dispatch := command(i, pb.CommandKind_COMMAND_KIND_TASK, pending)
			dispatch.Id = state.ExecutionId
			dispatch.Task = proto.Clone(w.nodes[i]).(*pb.Node)
			result.Commands = append(result.Commands, dispatch)
		}
	}

	if result.Settled && previous != nil && !settled(previous) {
		end := command(0, pb.CommandKind_COMMAND_KIND_END, previous.Nodes[0].Phase)
		end.Id = stableID("end", instance.Id)
		result.Commands = append(result.Commands, end)
	}

	return result
}
