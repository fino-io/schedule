package schedule

import (
	"context"
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

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
// handle 接收命令副本，按命令 ID 幂等处理，实际重试由接入方负责。
func Dispatch(ctx context.Context, handle func(context.Context, *pb.Command) error, commands []*pb.Command) error {
	for i, command := range commands {
		var err error
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if handle == nil || !validCommand(command) {
			err = ErrInvalidCommand
		} else {
			err = handle(ctx, proto.Clone(command).(*pb.Command))
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
