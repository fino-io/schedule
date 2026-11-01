package schedule

import (
	"fmt"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func mustCompile(t testing.TB, root *pb.Node) *Workflow {
	t.Helper()

	w, err := Compile(root)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func start(t testing.TB, w *Workflow) *pb.Transition {
	t.Helper()

	result, err := w.Start("run")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Restore(result.Instance); err != nil {
		t.Fatalf("invalid initialized snapshot: %v", err)
	}
	return result
}

func report(instance *pb.Instance, id string, kind pb.EventKind) *pb.Event {
	event := &pb.Event{
		Id:         fmt.Sprintf("event-%d-%s-%d", instance.Revision, id, kind),
		InstanceId: instance.Id,
		Kind:       kind,
		NodeId:     id,
	}
	for _, state := range instance.Nodes {
		if state.NodeId == id {
			event.ExecutionId = state.ExecutionId
		}
	}

	return event
}

func control(instance *pb.Instance, kind pb.EventKind) *pb.Event {
	return &pb.Event{
		Id:         fmt.Sprintf("control-%d-%d", instance.Revision, kind),
		InstanceId: instance.Id,
		Kind:       kind,
	}
}

func advance(t testing.TB, w *Workflow, current *pb.Transition, event *pb.Event) *pb.Transition {
	t.Helper()

	before := proto.Clone(current.Instance)
	result, err := w.Advance(current.Instance, event)
	if err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(before, current.Instance) {
		t.Fatal("advance modified its input")
	}
	if _, err := w.Restore(result.Instance); err != nil {
		t.Fatalf("advance produced invalid snapshot: %v\n%s", err, result.Instance)
	}

	return result
}

func nodeState(instance *pb.Instance, id string) *pb.NodeState {
	for _, state := range instance.Nodes {
		if state.NodeId == id {
			return state
		}
	}
	panic("unknown node: " + id)
}

func commands(result *pb.Transition, kind pb.CommandKind) []string {
	var ids []string
	for _, command := range result.Commands {
		if command.Kind == kind {
			ids = append(ids, command.State.NodeId)
		}
	}
	return ids
}

func wantTasks(t testing.TB, result *pb.Transition, ids ...string) {
	t.Helper()

	got := commands(result, pb.CommandKind_COMMAND_KIND_TASK)
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("tasks=%v, want %v", got, ids)
	}
}
