package schedule

import (
	"errors"
	"strconv"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func FuzzWorkflow(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{1, 1, 0, 0, 16},
		{2, 1, 0, 0, 0, 16},
		{3, 1, 1, 1, 0, 0, 0, 3, 0, 0},
		{1, 1, 0, 0, 1, 0, 0, 0},
		{3, 1, 1, 1, 0, 0, 0, 3, 1, 0, 2},
		{2, 2, 3, 1, 0, 0, 1, 1, 0, 0, 2, 1, 0, 0, 48, 128, 32},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}
		cursor := 0
		take := func() byte {
			if cursor == len(data) {
				return 0
			}
			value := data[cursor]
			cursor++
			return value
		}

		// 深度最多 4 层，每个组合最多 3 个子项，总节点数不超过 40。
		count := 0
		var tree func(int) *pb.Node
		tree = func(depth int) *pb.Node {
			node := Task(strconv.Itoa(count))
			count++
			kind := pb.NodeKind(1 + take()%4)
			if depth == 3 || kind == task {
				return node
			}
			node.Kind = kind
			children := 1 + int(take()%3)
			for range children {
				node.Children = append(node.Children, tree(depth+1))
			}
			return node
		}
		w := mustCompile(t, tree(0))
		current := start(t, w)
		issued := make(map[string]bool)

		for step := 0; ; step++ {
			for _, command := range current.Commands {
				if !validCommand(command) || issued[command.Id] {
					t.Fatalf("invalid or repeated command: %s", command)
				}
				issued[command.Id] = true
			}
			var active []string
			for i, state := range current.Instance.Nodes {
				if w.nodes[i].Kind == task && activated(state.Phase) {
					if !issued[executionID(current.Instance.Id, state.NodeId)] {
						t.Fatal("execution has no dispatch command")
					}
					if !terminal(state.Phase) {
						active = append(active, state.NodeId)
					}
				}
			}
			if current.Settled {
				break
			}
			if step >= len(data)+2*len(w.nodes) {
				t.Fatal("workflow did not settle after consuming its input")
			}

			choice := take()
			var event *pb.Event
			switch {
			case current.Instance.Mode == modePaused && len(active) == 0:
				event = control(current.Instance, pb.EventKind_EVENT_KIND_RESUME)
			case current.Instance.Mode != modeStopped && choice%16 == 1:
				event = control(current.Instance, pb.EventKind_EVENT_KIND_PAUSE)
			case current.Instance.Mode != modeStopped && choice%16 == 2:
				event = control(current.Instance, pb.EventKind_EVENT_KIND_STOP)
			default:
				if len(active) == 0 {
					t.Fatalf("no execution or control progress: %s", current.Instance)
				}
				kind := []pb.EventKind{
					pb.EventKind_EVENT_KIND_SUCCEEDED,
					pb.EventKind_EVENT_KIND_FAILED,
					pb.EventKind_EVENT_KIND_STOPPED,
					pb.EventKind_EVENT_KIND_STARTED,
				}[(choice>>4)%4]
				event = report(current.Instance, active[int(choice)%len(active)], kind)
				if kind != pb.EventKind_EVENT_KIND_STARTED {
					event.ResultRef = "results/" + event.NodeId
				}
				if kind > pb.EventKind_EVENT_KIND_SUCCEEDED && choice&128 != 0 {
					event.ErrorMessage = proto.String("")
				}
			}

			previous := current
			current = advance(t, w, current, event)
			for i, state := range previous.Instance.Nodes {
				if terminal(state.Phase) && !proto.Equal(state, current.Instance.Nodes[i]) {
					t.Fatalf("locked result or decision changed: %s", state.NodeId)
				}
			}
			if current.Settled && event.Kind >= pb.EventKind_EVENT_KIND_PAUSE {
				// 停止可能立即收尾，已收尾实例按协议拒绝后续控制。
				before := proto.Clone(current.Instance)
				if next, err := w.Advance(current.Instance, event); !errors.Is(err, ErrConflict) ||
					next != nil || !proto.Equal(before, current.Instance) {
					t.Fatalf("settled instance accepted control or modified input: %v", err)
				}
				continue
			}
			duplicate := advance(t, w, current, event)
			if len(duplicate.Commands) != 0 || !proto.Equal(current.Instance, duplicate.Instance) {
				t.Fatal("duplicate event changed the revision or generated commands")
			}

			if event.Kind >= pb.EventKind_EVENT_KIND_SUCCEEDED && event.Kind <= pb.EventKind_EVENT_KIND_STOPPED {
				conflict := proto.Clone(event).(*pb.Event)
				conflict.ResultRef += "/conflict"
				before := proto.Clone(current.Instance)
				if next, err := w.Advance(current.Instance, conflict); !errors.Is(err, ErrConflict) ||
					next != nil || !proto.Equal(before, current.Instance) {
					t.Fatalf("conflicting result accepted or modified input: %v", err)
				}
			}
		}
	})
}
