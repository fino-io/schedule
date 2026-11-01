package schedule

import (
	"context"
	"errors"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

type receiver struct {
	ids  []string
	fail error
}

func (r *receiver) OnTask(_ context.Context, command *pb.Command) error {
	r.ids = append(r.ids, command.Id)
	command.Task.Id = "mutated"
	return r.fail
}

func (r *receiver) OnState(_ context.Context, command *pb.Command) error {
	r.ids = append(r.ids, command.Id)
	command.State.NodeId = "mutated"
	return nil
}

func (r *receiver) OnEnd(_ context.Context, command *pb.Command) error {
	r.ids = append(r.ids, command.Id)
	return nil
}

func TestDispatch(t *testing.T) {
	w := mustCompile(t, Task("a"))
	initial := start(t, w)
	before := proto.Clone(initial)
	failure := errors.New("worker unavailable")
	r := &receiver{fail: failure}

	err := Dispatch(context.Background(), r, initial.Commands)
	var dispatchError *DispatchError
	if !errors.Is(err, failure) || !errors.As(err, &dispatchError) || dispatchError.Index != 1 {
		t.Fatalf("dispatch error=%v", err)
	}
	if !proto.Equal(before, initial) {
		t.Fatal("callback mutated stored commands")
	}

	r.fail = nil
	if err := Dispatch(context.Background(), r, initial.Commands[dispatchError.Index:]); err != nil {
		t.Fatal(err)
	}

	finished := advance(t, w, initial, report(initial.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	if err := Dispatch(context.Background(), r, finished.Commands); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(commands(finished, pb.CommandKind_COMMAND_KIND_END), []string{"a"}) {
		t.Fatal("missing end notification")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	count := len(r.ids)
	if err := Dispatch(ctx, r, initial.Commands); !errors.Is(err, context.Canceled) || len(r.ids) != count {
		t.Fatal("canceled dispatch invoked a callback")
	}
	if err := Dispatch(context.Background(), nil, initial.Commands); !errors.Is(err, ErrInvalidCommand) {
		t.Fatal(err)
	}
	if err := Dispatch(context.Background(), r, []*pb.Command{nil}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatal(err)
	}
}

func TestDispatchRejectsCorruptedCommands(t *testing.T) {
	w := mustCompile(t, Task("a"))
	initial := start(t, w)
	var original *pb.Command
	for _, command := range initial.Commands {
		if command.Kind == pb.CommandKind_COMMAND_KIND_TASK {
			original = command
		}
	}
	for _, change := range []func(*pb.Command){
		func(c *pb.Command) {
			c.Id = "wrong"
		},
		func(c *pb.Command) {
			c.Revision = 0
		},
		func(c *pb.Command) {
			c.DefinitionHash = ""
		},
		func(c *pb.Command) {
			c.Kind = 99
		},
		func(c *pb.Command) {
			c.Path = nil
		},
		func(c *pb.Command) {
			c.State.Phase = succeeded
		},
		func(c *pb.Command) {
			c.Task.Children = []*pb.Node{Task("child")}
		},
	} {
		command := proto.Clone(original).(*pb.Command)
		change(command)
		receiver := &receiver{}

		if err := Dispatch(context.Background(), receiver, []*pb.Command{command}); !errors.Is(err, ErrInvalidCommand) || len(receiver.ids) != 0 {
			t.Fatal("invalid command reached a callback")
		}
	}
}
