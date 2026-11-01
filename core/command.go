package schedule

import (
	"strconv"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// transition 只用于初始化或未收尾实例的实际推进；重复事件在 Advance 中直接返回。
func (w *Workflow) transition(previous, instance *pb.Instance) *pb.Transition {
	result := view(instance)
	modeChanged := previous == nil || previous.Mode != instance.Mode

	command := func(i int, kind pb.CommandKind) *pb.Command {
		return &pb.Command{
			InstanceId:     instance.Id,
			DefinitionHash: w.hash,
			Revision:       instance.Revision,
			Kind:           kind,
			State:          proto.Clone(instance.Nodes[i]).(*pb.NodeState),
			Mode:           instance.Mode,
		}
	}

	for i, state := range instance.Nodes {
		before := pending
		if previous != nil {
			before = previous.Nodes[i].Phase
		}
		// 结果与决策随阶段一次写入，只有根控制通知可以保持原阶段。
		if state.Phase != before || (i == 0 && modeChanged) {
			notification := command(i, pb.CommandKind_COMMAND_KIND_STATE)
			notification.Id = stableID("state", instance.Id, state.NodeId, strconv.FormatUint(instance.Revision, 10))
			result.Commands = append(result.Commands, notification)
		}
	}

	for i, state := range instance.Nodes {
		if state.Phase == ready && (previous == nil || previous.Nodes[i].Phase == pending) {
			dispatch := command(i, pb.CommandKind_COMMAND_KIND_TASK)
			dispatch.Id = executionID(instance.Id, state.NodeId)
			dispatch.Task = proto.Clone(w.nodes[i].Node).(*pb.Node)
			result.Commands = append(result.Commands, dispatch)
		}
	}

	if result.Settled && previous != nil {
		end := command(0, pb.CommandKind_COMMAND_KIND_END)
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
		command.Mode < modeRunning || command.Mode > modeStopped {
		return false
	}

	switch command.Kind {
	case pb.CommandKind_COMMAND_KIND_TASK:
		return command.Task != nil && command.Task.Kind == task && len(command.Task.Children) == 0 &&
			command.Task.Id == state.NodeId && state.Phase == ready &&
			command.Id == executionID(command.InstanceId, state.NodeId)
	case pb.CommandKind_COMMAND_KIND_STATE:
		return command.Task == nil &&
			command.Id == stableID("state", command.InstanceId, state.NodeId, strconv.FormatUint(command.Revision, 10))
	case pb.CommandKind_COMMAND_KIND_END:
		return command.Task == nil && terminal(state.Phase) &&
			command.Id == stableID("end", command.InstanceId)
	}

	return false
}
