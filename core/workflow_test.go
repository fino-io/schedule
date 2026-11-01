package schedule

import (
	"errors"
	"fmt"
	"math"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestWorkflowTaskLifecycleAndDeterminism(t *testing.T) {
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
	if _, err := w.Advance(
		finished.Instance,
		report(finished.Instance, "a", pb.EventKind_EVENT_KIND_FAILED),
	); !errors.Is(err, ErrConflict) || !proto.Equal(before, finished.Instance) {
		t.Fatal("conflicting result was accepted or modified input")
	}
}

func TestWorkflowPauseResumeAndStop(t *testing.T) {
	w := mustCompile(t, Any("root",
		Seq("branch", Task("a"), Task("b")),
		Task("d"),
	))
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
	if nodeState(current.Instance, "root").Phase != succeeded ||
		nodeState(current.Instance, "b").Phase != ready || current.Settled {
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

func TestWorkflowStopPendingAndInFlight(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	current := start(t, w)

	current = advance(t, w, current, control(current.Instance, pb.EventKind_EVENT_KIND_STOP))
	if nodeState(current.Instance, "root").Phase != stopped ||
		nodeState(current.Instance, "b").Phase != skipped || current.Settled {
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

func TestWorkflowEventValidation(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	invalid := []*pb.Event{
		nil,
		{},
		{
			Id:         "id",
			InstanceId: "other",
			Kind:       pb.EventKind_EVENT_KIND_PAUSE,
		},
		{
			Id:         "id",
			InstanceId: "run",
			Kind:       pb.EventKind_EVENT_KIND_PAUSE,
			NodeId:     "a",
		},
		{
			Id:          "id",
			InstanceId:  "run",
			Kind:        pb.EventKind_EVENT_KIND_STARTED,
			NodeId:      "root",
			ExecutionId: "wrong",
		},
		{
			Id:          "id",
			InstanceId:  "run",
			Kind:        pb.EventKind_EVENT_KIND_SUCCEEDED,
			NodeId:      "b",
			ExecutionId: executionID("run", "b"),
		},
		{
			Id:          "id",
			InstanceId:  "run",
			Kind:        pb.EventKind_EVENT_KIND_SUCCEEDED,
			NodeId:      "a",
			ExecutionId: "wrong",
		},
	}

	badStart := report(initial.Instance, "a", pb.EventKind_EVENT_KIND_STARTED)
	badStart.ResultRef = "not allowed"
	badSuccess := report(initial.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED)
	badSuccess.ErrorMessage = proto.String("not allowed")
	unknown := report(initial.Instance, "a", 99)
	invalid = append(invalid, badStart, badSuccess, unknown)

	before := proto.Clone(initial.Instance)
	for _, event := range invalid {
		if _, err := w.Advance(initial.Instance, event); !errors.Is(err, ErrInvalidEvent) || !proto.Equal(before, initial.Instance) {
			t.Fatalf("invalid event accepted or mutated input: %v, %v", event, err)
		}
	}
}

func TestWorkflowRevisionOverflow(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	maxRevision := proto.Clone(initial.Instance).(*pb.Instance)
	maxRevision.Revision = math.MaxUint64
	if _, err := w.Advance(maxRevision, report(maxRevision, "a", pb.EventKind_EVENT_KIND_SUCCEEDED)); !errors.Is(err, ErrConflict) {
		t.Fatal("revision overflow accepted")
	}
}

func TestWorkflowRejectsInvalidText(t *testing.T) {
	w := mustCompile(t, Task("a"))
	if _, err := w.Start("\xff"); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal("invalid UTF-8 instance ID accepted")
	}

	current := start(t, w)
	event := report(current.Instance, "a", pb.EventKind_EVENT_KIND_FAILED)
	event.ErrorMessage = proto.String("\xff")
	if _, err := w.Advance(current.Instance, event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal("nonserializable error accepted")
	}
}

func BenchmarkAdvance(b *testing.B) {
	items := make([]*pb.Node, 1000)
	for i := range items {
		items[i] = Task(fmt.Sprint(i))
	}
	w := mustCompile(b, All("root", items...))
	current := start(b, w)
	event := report(current.Instance, "0", pb.EventKind_EVENT_KIND_SUCCEEDED)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := w.Advance(current.Instance, event); err != nil {
			b.Fatal(err)
		}
	}
}
