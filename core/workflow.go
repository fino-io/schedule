package schedule

import (
	"fmt"
	"math"

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

// Advance 只计算状态和意图，不执行 I/O。事件 ID 的持久化去重及版本 CAS 由接入方完成。
func (w *Workflow) Advance(instance *pb.Instance, event *pb.Event) (*pb.Transition, error) {
	restored, err := w.Restore(instance)
	if err != nil {
		return nil, err
	}

	if event == nil || event.Id == "" || event.InstanceId != instance.Id ||
		!validText(event.Id, event.ResultRef, event.GetErrorMessage()) {
		return nil, fmt.Errorf("%w: event ID or instance binding", ErrInvalidEvent)
	}

	next := restored.Instance
	if event.Kind >= pb.EventKind_EVENT_KIND_PAUSE && event.Kind <= pb.EventKind_EVENT_KIND_STOP {
		if event.NodeId != "" || event.ExecutionId != "" || event.ErrorMessage != nil || event.ResultRef != "" {
			return nil, fmt.Errorf("%w: control event must not carry a task report", ErrInvalidEvent)
		}
		if restored.Settled {
			return nil, fmt.Errorf("%w: instance is settled", ErrConflict)
		}
		if next.Mode == modeStopped && event.Kind != pb.EventKind_EVENT_KIND_STOP {
			return nil, fmt.Errorf("%w: stopped instance cannot pause or resume", ErrConflict)
		}

		var targetMode pb.Mode
		switch event.Kind {
		case pb.EventKind_EVENT_KIND_PAUSE:
			targetMode = modePaused
		case pb.EventKind_EVENT_KIND_RESUME:
			targetMode = modeRunning
		case pb.EventKind_EVENT_KIND_STOP:
			targetMode = modeStopped
		}
		if next.Mode == targetMode {
			return restored, nil
		}
		next.Mode = targetMode
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
				return restored, nil
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

		state.Phase, state.ResultRef = target, event.ResultRef
		state.ErrorMessage = nil
		if event.ErrorMessage != nil {
			state.ErrorMessage = proto.String(*event.ErrorMessage)
		}
		if terminal(instance.Nodes[i].Phase) {
			if proto.Equal(state, instance.Nodes[i]) {
				return restored, nil
			}
			return nil, fmt.Errorf("%w: task %q already has a different result", ErrConflict, event.NodeId)
		}

		// 在激活任何后继之前，先向上传播结果，使祖先失败能及时阻止分派。
		for child, parent := i, w.nodes[i].parent; parent >= 0; child, parent = parent, w.nodes[parent].parent {
			w.resolve(next, parent, child)
		}
	}

	w.reconcile(next)
	if instance.Revision == math.MaxUint64 {
		return nil, fmt.Errorf("%w: revision exhausted", ErrConflict)
	}

	next.Revision++

	return w.transition(instance, next), nil
}
