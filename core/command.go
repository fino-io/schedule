package schedule

import (
	"strconv"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func (w *Workflow) transition(previous, instance *pb.Instance) *pb.Transition {
	result := view(instance)
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
		// 结果与决策随阶段一次写入，只有根控制通知可以保持原阶段。
		if state.Phase != before || (i == 0 && previousMode != instance.Mode) {
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

func validCommand(command *pb.Command) bool {
	if command == nil || command.Id == "" || command.InstanceId == "" ||
		command.DefinitionHash == "" || command.Revision == 0 || command.State == nil {
		return false
	}

	state := command.State
	if state.NodeId == "" || state.Phase < pending || state.Phase > skipped ||
		command.Mode < modeRunning || command.Mode > modeStopped ||
		len(command.Path) == 0 || command.Path[len(command.Path)-1] != state.NodeId {
		return false
	}

	switch command.Kind {
	case pb.CommandKind_COMMAND_KIND_TASK:
		return command.Task != nil && command.Task.Kind == task && len(command.Task.Children) == 0 &&
			command.Task.Id == state.NodeId && state.Phase == ready &&
			state.ExecutionId == command.Id && command.Id == executionID(command.InstanceId, state.NodeId)
	case pb.CommandKind_COMMAND_KIND_STATE:
		return command.Task == nil &&
			command.Id == stableID("state", command.InstanceId, state.NodeId, strconv.FormatUint(command.Revision, 10))
	case pb.CommandKind_COMMAND_KIND_END:
		return command.Task == nil && len(command.Path) == 1 && terminal(state.Phase) &&
			command.Id == stableID("end", command.InstanceId)
	}

	return false
}
