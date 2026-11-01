package schedule_test

import (
	"context"
	"testing"

	schedule "github.com/fino-io/schedule/core"
	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestPublicRecoveryAndDispatch(t *testing.T) {
	w, err := schedule.Compile(schedule.Task("send"))
	if err != nil {
		t.Fatal(err)
	}
	initial, err := w.Start("run")
	if err != nil {
		t.Fatal(err)
	}

	// 快照和 outbox 分别持久化，模拟提交成功、尚未投递时退出。
	snapshot, err := proto.Marshal(initial.Instance)
	if err != nil {
		t.Fatal(err)
	}
	var outbox []*pb.Command
	for _, command := range initial.Commands {
		data, err := proto.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		decoded := &pb.Command{}
		if err := proto.Unmarshal(data, decoded); err != nil {
			t.Fatal(err)
		}
		outbox = append(outbox, decoded)
	}

	w, err = schedule.Compile(w.Root())
	if err != nil {
		t.Fatal(err)
	}
	decoded := &pb.Instance{}
	if err := proto.Unmarshal(snapshot, decoded); err != nil {
		t.Fatal(err)
	}
	current, err := w.Restore(decoded)
	if err != nil || len(current.GetCommands()) != 0 {
		t.Fatalf("restore failed or regenerated commands: %v", err)
	}

	seen := make(map[string]bool)
	var task *pb.Command
	var states []*pb.Command
	tasks, ends := 0, 0
	handle := func(_ context.Context, command *pb.Command) error {
		if seen[command.Id] {
			return nil
		}
		seen[command.Id] = true
		switch command.Kind {
		case pb.CommandKind_COMMAND_KIND_TASK:
			task = command
			tasks++
		case pb.CommandKind_COMMAND_KIND_STATE:
			states = append(states, command)
		case pb.CommandKind_COMMAND_KIND_END:
			ends++
		}
		return nil
	}
	for range 2 {
		if err := schedule.Dispatch(context.Background(), handle, outbox); err != nil {
			t.Fatal(err)
		}
	}
	if tasks != 1 || task.Task.Id != "send" || task.State.NodeId != task.Task.Id ||
		task.InstanceId != current.Instance.Id || task.DefinitionHash != current.Instance.DefinitionHash {
		t.Fatal("saved task command was not sufficient for dispatch")
	}
	if task.Id != "846fb15b35da245519641f7e879f353e63069c49236dc56dd538eb068645b188" {
		t.Fatal("stable execution ID changed")
	}

	for _, control := range []struct {
		kind pb.EventKind
		mode pb.Mode
	}{
		{pb.EventKind_EVENT_KIND_PAUSE, pb.Mode_MODE_PAUSED},
		{pb.EventKind_EVENT_KIND_RESUME, pb.Mode_MODE_RUNNING},
	} {
		current, err = w.Advance(current.Instance, &pb.Event{
			Id:         control.kind.String(),
			InstanceId: "run",
			Kind:       control.kind,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := schedule.Dispatch(context.Background(), handle, current.Commands); err != nil {
			t.Fatal(err)
		}
		state := states[len(states)-1]
		if len(current.Commands) != 1 || state.Mode != control.mode || state.State.Phase != pb.Phase_PHASE_READY {
			t.Fatal("control notification did not describe the current mode and node state")
		}
	}

	event := &pb.Event{
		Id:          "completed",
		InstanceId:  task.InstanceId,
		Kind:        pb.EventKind_EVENT_KIND_SUCCEEDED,
		NodeId:      task.Task.Id,
		ExecutionId: task.Id,
		ResultRef:   "results/send",
	}
	current, err = w.Advance(current.Instance, event)
	if err != nil || !current.GetResultReady() || !current.GetSettled() {
		t.Fatalf("execution report did not settle the workflow: %v", err)
	}
	for range 2 {
		if err := schedule.Dispatch(context.Background(), handle, current.Commands); err != nil {
			t.Fatal(err)
		}
	}
	if tasks != 1 || ends != 1 || len(states) != 4 || states[3].State.ResultRef != event.ResultRef {
		t.Fatal("redelivery duplicated execution or lost a current-state notification")
	}

	duplicate, err := w.Advance(current.Instance, event)
	if err != nil || len(duplicate.GetCommands()) != 0 || !proto.Equal(duplicate.GetInstance(), current.Instance) {
		t.Fatalf("duplicate report changed the completed instance: %v", err)
	}
}
