package schedule

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func mustCompile(t testing.TB, root *pb.Node) *Workflow {
	t.Helper()
	w, err := Compile(&pb.Definition{Root: root})
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
	return &pb.Event{Id: fmt.Sprintf("control-%d-%d", instance.Revision, kind), InstanceId: instance.Id, Kind: kind}
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

func TestTaskLifecycleAndDeterminism(t *testing.T) {
	w := mustCompile(t, Task("a"))
	first := start(t, w)
	wantTasks(t, first, "a")
	repeated, err := w.Start("run")
	if err != nil || !proto.Equal(first, repeated) {
		t.Fatal("start is not deterministic")
	}
	other, err := w.Start("other")
	if err != nil || nodeState(first.Instance, "a").ExecutionId == nodeState(other.Instance, "a").ExecutionId {
		t.Fatal("execution IDs not scoped to instance")
	}
	if _, err := w.Start(""); err == nil {
		t.Fatal("empty instance ID accepted")
	}
	started := advance(t, w, first, report(first.Instance, "a", pb.EventKind_EVENT_KIND_STARTED))
	wantTasks(t, started)
	finished := advance(t, w, started, report(started.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	if !finished.ResultReady || !finished.Settled || len(commands(finished, pb.CommandKind_COMMAND_KIND_END)) != 1 {
		t.Fatal("task did not settle exactly once")
	}
	for _, kind := range []pb.EventKind{pb.EventKind_EVENT_KIND_STARTED, pb.EventKind_EVENT_KIND_SUCCEEDED} {
		duplicate := advance(t, w, finished, report(finished.Instance, "a", kind))
		if len(duplicate.Commands) != 0 || duplicate.Instance.Revision != finished.Instance.Revision {
			t.Fatal("duplicate changed snapshot or emitted effects")
		}
	}
	before := proto.Clone(finished.Instance)
	if _, err := w.Advance(finished.Instance, report(finished.Instance, "a", pb.EventKind_EVENT_KIND_FAILED)); !errors.Is(err, ErrConflict) || !proto.Equal(before, finished.Instance) {
		t.Fatal("conflicting result was accepted or modified input")
	}
}

func TestSequenceAndFailure(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b"), Task("c")))
	current := start(t, w)
	wantTasks(t, current, "a")
	current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	wantTasks(t, current, "b")
	event := report(current.Instance, "b", pb.EventKind_EVENT_KIND_FAILED)
	event.ErrorMessage = proto.String("")
	current = advance(t, w, current, event)
	wantTasks(t, current)
	if nodeState(current.Instance, "root").CauseId != "b" || nodeState(current.Instance, "c").Phase != skipped || !current.Settled {
		t.Fatal(current.Instance)
	}
	if nodeState(current.Instance, "b").ErrorMessage == nil {
		t.Fatal("empty error lost presence")
	}
	event.ErrorMessage = nil
	if _, err := w.Advance(current.Instance, event); !errors.Is(err, ErrConflict) {
		t.Fatal("nil and empty error were conflated")
	}
}

func TestAllFailureDrainsDispatchedTasks(t *testing.T) {
	w := mustCompile(t, All("root", Task("a"), Task("b")))
	current := start(t, w)
	wantTasks(t, current, "a", "b")
	current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_FAILED))
	if !current.ResultReady || current.Settled || nodeState(current.Instance, "b").Phase != ready {
		t.Fatal("failure prematurely settled the running scope")
	}
	current = advance(t, w, current, report(current.Instance, "b", pb.EventKind_EVENT_KIND_SUCCEEDED))
	if !current.Settled || nodeState(current.Instance, "root").Phase != failed || nodeState(current.Instance, "root").CauseId != "a" {
		t.Fatal(current.Instance)
	}
}

func TestAnyWinnerAndNestedDrain(t *testing.T) {
	w := mustCompile(t, Seq("root", Any("choice", Seq("branch", Task("b"), Task("c")), Task("d")), Task("e")))
	current := start(t, w)
	wantTasks(t, current, "b", "d")
	event := report(current.Instance, "d", pb.EventKind_EVENT_KIND_SUCCEEDED)
	event.ResultRef = "result:d"
	current = advance(t, w, current, event)
	wantTasks(t, current, "e")
	current = advance(t, w, current, report(current.Instance, "e", pb.EventKind_EVENT_KIND_SUCCEEDED))
	if !current.ResultReady || current.Settled {
		t.Fatal("root result was conflated with scope settlement")
	}
	current = advance(t, w, current, report(current.Instance, "b", pb.EventKind_EVENT_KIND_SUCCEEDED))
	wantTasks(t, current, "c")
	current = advance(t, w, current, report(current.Instance, "c", pb.EventKind_EVENT_KIND_SUCCEEDED))
	choice := nodeState(current.Instance, "choice")
	if !current.Settled || choice.WinnerId != "d" || choice.ResultRef != "result:d" || len(commands(current, pb.CommandKind_COMMAND_KIND_END)) != 1 {
		t.Fatal(current)
	}
}

func TestAncestorFailureOverridesAnyDrain(t *testing.T) {
	w := mustCompile(t, All("root", Any("choice", Seq("branch", Task("b"), Task("c")), Task("d")), Task("e")))
	current := start(t, w)
	current = advance(t, w, current, report(current.Instance, "d", pb.EventKind_EVENT_KIND_SUCCEEDED))
	current = advance(t, w, current, report(current.Instance, "e", pb.EventKind_EVENT_KIND_FAILED))
	if nodeState(current.Instance, "c").Phase != skipped || nodeState(current.Instance, "choice").Phase != succeeded {
		t.Fatal(current.Instance)
	}
	current = advance(t, w, current, report(current.Instance, "b", pb.EventKind_EVENT_KIND_SUCCEEDED))
	wantTasks(t, current)
	if !current.Settled || nodeState(current.Instance, "root").CauseId != "e" {
		t.Fatal(current.Instance)
	}
}

func TestPauseResumeAndStop(t *testing.T) {
	w := mustCompile(t, Any("root", Seq("branch", Task("a"), Task("b")), Task("d")))
	current := start(t, w)
	current = advance(t, w, current, report(current.Instance, "d", pb.EventKind_EVENT_KIND_SUCCEEDED))
	current = advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_PAUSE))
	paused := advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_PAUSE))
	if len(paused.Commands) != 0 || paused.Instance.Revision != current.Instance.Revision {
		t.Fatal("repeated pause produced effects")
	}
	current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	wantTasks(t, current)
	if current.Settled || nodeState(current.Instance, "b").Phase != pending {
		t.Fatal("paused scope with a pending successor incorrectly settled")
	}
	current = advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_RESUME))
	wantTasks(t, current, "b")
	current = advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_STOP))
	if nodeState(current.Instance, "root").Phase != succeeded || nodeState(current.Instance, "b").Phase != ready || current.Settled {
		t.Fatal("stop rewrote known result or pretended a task was canceled")
	}
	if _, err := w.Advance(current.Instance, control(current.Instance, pb.EventKind_EVENT_KIND_RESUME)); !errors.Is(err, ErrConflict) {
		t.Fatal("stopped instance resumed")
	}
	current = advance(t, w, current, report(current.Instance, "b", pb.EventKind_EVENT_KIND_STOPPED))
	if !current.Settled || nodeState(current.Instance, "root").Phase != succeeded {
		t.Fatal(current.Instance)
	}
	if _, err := w.Advance(current.Instance, control(current.Instance, pb.EventKind_EVENT_KIND_PAUSE)); !errors.Is(err, ErrConflict) {
		t.Fatal("settled instance accepted new control")
	}
}

func TestStopPendingAndInFlight(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	current := start(t, w)
	current = advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_STOP))
	if nodeState(current.Instance, "root").Phase != stopped || nodeState(current.Instance, "b").Phase != skipped || current.Settled {
		t.Fatal(current.Instance)
	}
	repeated := advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_STOP))
	if len(repeated.Commands) != 0 {
		t.Fatal("repeated stop produced effects")
	}
	current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	if !current.Settled || nodeState(current.Instance, "root").Phase != stopped {
		t.Fatal("late success overwrote stop")
	}
}

func TestAnyAllUnsuccessfulAndAllSuccess(t *testing.T) {
	for _, kind := range []pb.NodeKind{all, any} {
		w := mustCompile(t, &pb.Node{Id: "root", Kind: kind, Children: []*pb.Node{Task("a"), Task("b")}})
		current := start(t, w)
		current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_FAILED))
		if kind == any && current.ResultReady {
			t.Fatal("first failure ended Any")
		}
		current = advance(t, w, current, report(current.Instance, "b", pb.EventKind_EVENT_KIND_STOPPED))
		if !current.Settled || nodeState(current.Instance, "root").Phase != failed {
			t.Fatal(current.Instance)
		}
	}
	w := mustCompile(t, All("root", Task("a"), Task("b")))
	current := start(t, w)
	for _, id := range []string{"b", "a"} {
		current = advance(t, w, current, report(current.Instance, id, pb.EventKind_EVENT_KIND_SUCCEEDED))
	}
	if !current.Settled || nodeState(current.Instance, "root").Phase != succeeded {
		t.Fatal(current.Instance)
	}
}
