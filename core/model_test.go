package schedule

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestMixedEventSequences(t *testing.T) {
	for seed := int64(0); seed < 120; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			random := rand.New(rand.NewSource(seed))
			kind := []pb.NodeKind{seq, all, any}[seed%3]
			w := mustCompile(t, &pb.Node{Id: "root", Kind: kind, Children: []*pb.Node{
				Any("choice", Seq("ab", Task("a"), Task("b")), All("cd", Task("c"), Task("d"))),
				Seq("tail", Task("e"), Task("f")),
			}})
			current := start(t, w)
			issued := make(map[string]bool)
			for iteration := 0; ; iteration++ {
				for _, command := range current.Commands {
					if issued[command.Id] {
						t.Fatal("command emitted twice")
					}
					issued[command.Id] = true
				}
				for _, state := range current.Instance.Nodes {
					if state.ExecutionId != "" && !issued[state.ExecutionId] {
						t.Fatal("execution has no command")
					}
				}
				if current.Settled {
					break
				}
				if iteration > 100 {
					t.Fatal("workflow did not make progress")
				}
				var active []string
				for _, state := range current.Instance.Nodes {
					if state.ExecutionId != "" && !terminal(state.Phase) {
						active = append(active, state.NodeId)
					}
				}
				var event *pb.Event
				switch {
				case current.Instance.Mode == modePaused:
					event = control(current.Instance, pb.EventKind_EVENT_KIND_RESUME)
				case current.Instance.Mode != modeStopped && random.Intn(7) == 0:
					event = control(current.Instance, pb.EventKind_EVENT_KIND_PAUSE)
				case current.Instance.Mode != modeStopped && random.Intn(9) == 0:
					event = control(current.Instance, pb.EventKind_EVENT_KIND_STOP)
				default:
					if len(active) == 0 {
						t.Fatalf("no active execution or control progress: %s", current.Instance)
					}
					kind := []pb.EventKind{pb.EventKind_EVENT_KIND_SUCCEEDED, pb.EventKind_EVENT_KIND_FAILED, pb.EventKind_EVENT_KIND_STOPPED}[random.Intn(3)]
					event = report(current.Instance, active[random.Intn(len(active))], kind)
				}
				current = advance(t, w, current, event)
			}
		})
	}
}

func TestCompiledWorkflowConcurrentUse(t *testing.T) {
	w := mustCompile(t, All("root", Task("a"), Task("b")))
	var group sync.WaitGroup
	for i := range 16 {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			current, err := w.Start(fmt.Sprint(id))
			if err != nil {
				t.Error(err)
				return
			}
			for _, node := range []string{"a", "b"} {
				before := proto.Clone(current.Instance)
				next, err := w.Advance(current.Instance, report(current.Instance, node, pb.EventKind_EVENT_KIND_SUCCEEDED))
				if err != nil || !proto.Equal(before, current.Instance) {
					t.Errorf("advance failed or mutated input: %v", err)
					return
				}
				current = next
			}
			if !current.Settled {
				t.Error("workflow did not settle")
			}
		}(i)
	}
	group.Wait()
}

func BenchmarkCompile(b *testing.B) {
	items := make([]*pb.Node, 1000)
	for i := range items {
		items[i] = Task(fmt.Sprint(i))
	}
	definition := &pb.Definition{Root: All("root", items...)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := Compile(definition); err != nil {
			b.Fatal(err)
		}
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
