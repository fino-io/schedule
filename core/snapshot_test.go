package schedule

import (
	"errors"
	"math"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotIntegrity(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	mutations := map[string]func(*pb.Instance){
		"identity":            func(i *pb.Instance) { i.Id = "" },
		"definition":          func(i *pb.Instance) { i.DefinitionHash = "other" },
		"revision":            func(i *pb.Instance) { i.Revision = 0 },
		"mode":                func(i *pb.Instance) { i.Mode = pb.Mode_MODE_UNSPECIFIED },
		"missing node":        func(i *pb.Instance) { i.Nodes = i.Nodes[:2] },
		"nil node":            func(i *pb.Instance) { i.Nodes[1] = nil },
		"reordered":           func(i *pb.Instance) { i.Nodes[1], i.Nodes[2] = i.Nodes[2], i.Nodes[1] },
		"unknown phase":       func(i *pb.Instance) { i.Nodes[1].Phase = 99 },
		"undispatched root":   func(i *pb.Instance) { i.Nodes[0].Phase = pending },
		"group dispatch":      func(i *pb.Instance) { i.Nodes[0].ExecutionId = "invalid" },
		"leaf binding":        func(i *pb.Instance) { i.Nodes[1].ExecutionId = "wrong" },
		"premature error":     func(i *pb.Instance) { i.Nodes[1].ErrorMessage = proto.String("error") },
		"leaf winner":         func(i *pb.Instance) { i.Nodes[1].WinnerId = "a" },
		"forged success":      func(i *pb.Instance) { i.Nodes[0].Phase = succeeded },
		"forged stop":         func(i *pb.Instance) { i.Nodes[0].Phase, i.Nodes[0].CauseId = stopped, "root" },
		"illegal skip":        func(i *pb.Instance) { i.Nodes[2].Phase, i.Nodes[2].CauseId = skipped, "a" },
		"bypass dependency":   func(i *pb.Instance) { i.Nodes[2].Phase, i.Nodes[2].ExecutionId = ready, executionID(i.Id, "b") },
		"missing propagation": func(i *pb.Instance) { i.Nodes[1].Phase = succeeded },
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := proto.Clone(initial.Instance).(*pb.Instance)
			mutation(copy)
			if _, err := w.Restore(copy); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("corrupted snapshot accepted: %v", err)
			}
		})
	}
	if _, err := w.Restore(nil); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatal(err)
	}
	other := mustCompile(t, Seq("root", Task("b"), Task("a")))
	if _, err := other.Restore(initial.Instance); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatal("different definition accepted")
	}
	maxRevision := proto.Clone(initial.Instance).(*pb.Instance)
	maxRevision.Revision = math.MaxUint64
	if _, err := w.Advance(maxRevision, report(maxRevision, "a", pb.EventKind_EVENT_KIND_SUCCEEDED)); !errors.Is(err, ErrConflict) {
		t.Fatal("revision overflow accepted")
	}
}

func TestEventValidation(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	invalid := []*pb.Event{
		nil, {},
		{Id: "id", InstanceId: "other", Kind: pb.EventKind_EVENT_KIND_PAUSE},
		{Id: "id", InstanceId: "run", Kind: pb.EventKind_EVENT_KIND_PAUSE, NodeId: "a"},
		{Id: "id", InstanceId: "run", Kind: pb.EventKind_EVENT_KIND_STARTED, NodeId: "root", ExecutionId: "wrong"},
		{Id: "id", InstanceId: "run", Kind: pb.EventKind_EVENT_KIND_SUCCEEDED, NodeId: "b", ExecutionId: executionID("run", "b")},
		{Id: "id", InstanceId: "run", Kind: pb.EventKind_EVENT_KIND_SUCCEEDED, NodeId: "a", ExecutionId: "wrong"},
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

func TestSerializationAndReplay(t *testing.T) {
	w := mustCompile(t, Any("root", Seq("branch", Task("a"), Task("b")), Task("d")))
	current := start(t, w)
	var events []*pb.Event
	for _, step := range []struct {
		node string
		kind pb.EventKind
	}{
		{"d", pb.EventKind_EVENT_KIND_FAILED},
		{"a", pb.EventKind_EVENT_KIND_SUCCEEDED},
		{"b", pb.EventKind_EVENT_KIND_SUCCEEDED},
	} {
		event := report(current.Instance, step.node, step.kind)
		events = append(events, event)
		current = advance(t, w, current, event)
		data, err := protojson.Marshal(current.Instance)
		if err != nil {
			t.Fatal(err)
		}
		decoded := &pb.Instance{}
		if err := protojson.Unmarshal(data, decoded); err != nil {
			t.Fatal(err)
		}
		restored, err := w.Restore(decoded)
		if err != nil || !proto.Equal(restored.Instance, current.Instance) || len(restored.Commands) != 0 {
			t.Fatal("restore changed snapshot or generated commands")
		}
		current = restored
	}
	replayed := start(t, w)
	for _, event := range events {
		replayed = advance(t, w, replayed, event)
	}
	if !proto.Equal(current.Instance, replayed.Instance) || !current.Settled {
		t.Fatal("replay is not deterministic")
	}
}

func TestAnyWinnerSnapshot(t *testing.T) {
	w := mustCompile(t, Any("root", Task("a"), Task("b")))
	current := start(t, w)
	current = advance(t, w, current, report(current.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED))
	for _, winner := range []string{"", "b", "root"} {
		copy := proto.Clone(current.Instance).(*pb.Instance)
		copy.Nodes[0].WinnerId = winner
		if _, err := w.Restore(copy); !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatal("invalid winner accepted")
		}
	}
}

func TestWireTextMustBeSerializable(t *testing.T) {
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
	if _, err := CompileStateflow(&pb.StateflowDefinition{Init: "\xff"}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatal("nonserializable state name accepted")
	}
}
