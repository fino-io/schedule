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

func TestSnapshotPreservesLockedDecisions(t *testing.T) {
	for _, kind := range []pb.NodeKind{all, any} {
		t.Run(kind.String(), func(t *testing.T) {
			w := mustCompile(t, &pb.Node{
				Id:       "root",
				Kind:     kind,
				Children: []*pb.Node{Task("a"), Task("b")},
			})
			current := start(t, w)
			eventKind := pb.EventKind_EVENT_KIND_FAILED
			if kind == any {
				eventKind = pb.EventKind_EVENT_KIND_SUCCEEDED
			}
			for _, id := range []string{"b", "a"} {
				event := report(current.Instance, id, eventKind)
				event.ResultRef = "result:" + id
				current = advance(t, w, current, event)
			}

			before := proto.Clone(current.Instance)
			restored, err := w.Restore(current.Instance)
			if err != nil || !proto.Equal(before, restored.GetInstance()) || len(restored.GetCommands()) != 0 {
				t.Fatalf("restore changed the locked decision: %v", err)
			}
			root := restored.Instance.Nodes[0]
			if kind == all && root.CauseId != "b" || kind == any && (root.WinnerId != "b" || root.ResultRef != "result:b") {
				t.Fatal("restore chose definition order instead of the accepted decision")
			}

			root.CauseId, root.WinnerId = "mutated", "mutated"
			restored.Instance.Nodes[1].ResultRef = "mutated"
			if !proto.Equal(before, current.Instance) {
				t.Fatal("restored snapshot shares mutable state with its input")
			}
		})
	}
}

func TestSnapshotRejectsInconsistentCombinationResults(t *testing.T) {
	for _, kind := range []pb.NodeKind{seq, all, any} {
		for _, eventKind := range []pb.EventKind{pb.EventKind_EVENT_KIND_SUCCEEDED, pb.EventKind_EVENT_KIND_FAILED} {
			t.Run(kind.String()+"/"+eventKind.String(), func(t *testing.T) {
				w := mustCompile(t, &pb.Node{
					Id:       "root",
					Kind:     kind,
					Children: []*pb.Node{Task("a"), Task("b")},
				})
				current := start(t, w)
				for _, id := range []string{"a", "b"} {
					if nodeState(current.Instance, id).Phase != skipped {
						current = advance(t, w, current, report(current.Instance, id, eventKind))
					}
				}

				for name, mutate := range map[string]func(*pb.NodeState){
					"phase": func(state *pb.NodeState) {
						state.Phase = running
					},
					"winner": func(state *pb.NodeState) {
						state.WinnerId = "root"
					},
					"cause": func(state *pb.NodeState) {
						state.CauseId = "root"
					},
					"result": func(state *pb.NodeState) {
						state.ResultRef = "forged"
					},
					"error": func(state *pb.NodeState) {
						state.ErrorMessage = proto.String("forged")
					},
				} {
					t.Run(name, func(t *testing.T) {
						copy := proto.Clone(current.Instance).(*pb.Instance)
						mutate(copy.Nodes[0])
						before := proto.Clone(copy)
						if _, err := w.Restore(copy); !errors.Is(err, ErrInvalidSnapshot) {
							t.Fatalf("inconsistent result accepted: %v", err)
						}
						if !proto.Equal(before, copy) {
							t.Fatal("failed restore modified its input")
						}
					})
				}
			})
		}
	}
}
