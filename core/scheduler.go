package schedule

import (
	"context"
	"fmt"

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
			return &DispatchError{
				Index:     i,
				CommandID: command.GetId(),
				Err:       err,
			}
		}
	}

	return nil
}
