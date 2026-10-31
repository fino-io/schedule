package schedule

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

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
