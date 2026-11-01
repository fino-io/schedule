package schedule

import (
	"errors"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotIntegrity(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	mutations := map[string]func(*pb.Instance){
		"identity": func(i *pb.Instance) {
			i.Id = ""
		},
		"definition": func(i *pb.Instance) {
			i.DefinitionHash = "other"
		},
		"revision": func(i *pb.Instance) {
			i.Revision = 0
		},
		"mode": func(i *pb.Instance) {
			i.Mode = pb.Mode_MODE_UNSPECIFIED
		},
		"missing node": func(i *pb.Instance) {
			i.Nodes = i.Nodes[:2]
		},
		"nil node": func(i *pb.Instance) {
			i.Nodes[1] = nil
		},
		"reordered": func(i *pb.Instance) {
			i.Nodes[1], i.Nodes[2] = i.Nodes[2], i.Nodes[1]
		},
		"unknown phase": func(i *pb.Instance) {
			i.Nodes[1].Phase = 99
		},
		"undispatched root": func(i *pb.Instance) {
			i.Nodes[0].Phase = pending
		},
		"group dispatch": func(i *pb.Instance) {
			i.Nodes[0].ExecutionId = "invalid"
		},
		"leaf binding": func(i *pb.Instance) {
			i.Nodes[1].ExecutionId = "wrong"
		},
		"premature error": func(i *pb.Instance) {
			i.Nodes[1].ErrorMessage = proto.String("error")
		},
		"leaf winner": func(i *pb.Instance) {
			i.Nodes[1].WinnerId = "a"
		},
		"forged success": func(i *pb.Instance) {
			i.Nodes[0].Phase = succeeded
		},
		"forged stop": func(i *pb.Instance) {
			i.Nodes[0].Phase, i.Nodes[0].CauseId = stopped, "root"
		},
		"illegal skip": func(i *pb.Instance) {
			i.Nodes[2].Phase, i.Nodes[2].CauseId = skipped, "a"
		},
		"bypass dependency": func(i *pb.Instance) {
			i.Nodes[2].Phase, i.Nodes[2].ExecutionId = ready, executionID(i.Id, "b")
		},
		"missing propagation": func(i *pb.Instance) {
			i.Nodes[1].Phase = succeeded
		},
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
}

func TestSnapshotSerializationAndReplay(t *testing.T) {
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

func TestSnapshotAnyWinner(t *testing.T) {
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
