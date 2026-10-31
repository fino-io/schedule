package schedule

import (
	"context"
	"fmt"
	"strconv"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Scheduler 只消费已持久化的命令。实现方按命令 ID 幂等处理，实际重试由接入方负责。
type Scheduler interface {
	OnTask(context.Context, *pb.Command) error
	OnState(context.Context, *pb.Command) error
	OnEnd(context.Context, *pb.Command) error
}

type DispatchError struct {
	Index     int
	CommandID string
	Err       error
}

func (e *DispatchError) Error() string {
	return fmt.Sprintf("dispatch command %q at %d: %v", e.CommandID, e.Index, e.Err)
}

func (e *DispatchError) Unwrap() error {
	return e.Err
}

// Dispatch 按已记录顺序消费命令，首个失败时停止；返回位置和原始错误，不回滚或自动重放。
func Dispatch(ctx context.Context, scheduler Scheduler, commands []*pb.Command) error {
	for i, command := range commands {
		var err error
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if scheduler == nil || !validCommand(command) {
			err = ErrInvalidCommand
		} else {
			copy := proto.Clone(command).(*pb.Command)
			switch command.Kind {
			case pb.CommandKind_COMMAND_KIND_TASK:
				err = scheduler.OnTask(ctx, copy)
			case pb.CommandKind_COMMAND_KIND_STATE:
				err = scheduler.OnState(ctx, copy)
			case pb.CommandKind_COMMAND_KIND_END:
				err = scheduler.OnEnd(ctx, copy)
			default:
				err = ErrInvalidCommand
			}
		}
		if err != nil {
			return &DispatchError{Index: i, CommandID: command.GetId(), Err: err}
		}
	}
	return nil
}

func validCommand(command *pb.Command) bool {
	if command == nil || command.Id == "" || command.InstanceId == "" || command.DefinitionHash == "" || command.Revision == 0 || command.State == nil {
		return false
	}
	state := command.State
	if state.NodeId == "" || state.Phase < pending || state.Phase > skipped || command.Mode < modeRunning || command.Mode > modeStopped || len(command.Path) == 0 || command.Path[len(command.Path)-1] != state.NodeId {
		return false
	}
	switch command.Kind {
	case pb.CommandKind_COMMAND_KIND_TASK:
		return command.Task != nil && command.Task.Kind == task && len(command.Task.Children) == 0 && command.Task.Id == state.NodeId && state.Phase == ready && state.ExecutionId == command.Id && command.Id == executionID(command.InstanceId, state.NodeId)
	case pb.CommandKind_COMMAND_KIND_STATE:
		return command.Task == nil && command.Id == stableID("state", command.InstanceId, state.NodeId, strconv.FormatUint(command.Revision, 10))
	case pb.CommandKind_COMMAND_KIND_END:
		return command.Task == nil && len(command.Path) == 1 && terminal(state.Phase) && command.Id == stableID("end", command.InstanceId)
	}
	return false
}
