package schedule

import (
	"context"
	"errors"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// journal 模拟接入方的原子提交协议，不是核心库的存储实现。
type journal struct {
	instance *pb.Instance
	receipts map[string]*pb.Event
	outbox   []*pb.Command
}

func (j *journal) commit(revision uint64, event *pb.Event, next *pb.Transition) error {
	if j.instance.Revision != revision {
		return ErrConflict
	}

	j.instance = proto.Clone(next.Instance).(*pb.Instance)
	j.receipts[event.Id] = proto.Clone(event).(*pb.Event)
	for _, command := range next.Commands {
		j.outbox = append(j.outbox, proto.Clone(command).(*pb.Command))
	}

	return nil
}

func (j *journal) accept(w *Workflow, event *pb.Event) error {
	if accepted := j.receipts[event.Id]; accepted != nil {
		if !proto.Equal(accepted, event) {
			return ErrConflict
		}
		return nil
	}

	next, err := w.Advance(j.instance, event)
	if err != nil {
		return err
	}
	return j.commit(j.instance.Revision, event, next)
}

type idempotentQueue struct {
	seen map[string]bool
	jobs map[string]*pb.Node
}

func (q *idempotentQueue) handle(_ context.Context, command *pb.Command) error {
	if command.Kind == pb.CommandKind_COMMAND_KIND_TASK && !q.seen[command.Id] {
		q.jobs[command.Id] = command.Task
	}
	q.seen[command.Id] = true
	return nil
}

func TestCrashRecoveryAndDeliveryProtocol(t *testing.T) {
	w := mustCompile(t, Seq("root", Task("a"), Task("b")))
	initial := start(t, w)
	store := &journal{
		instance: initial.Instance,
		receipts: make(map[string]*pb.Event),
		outbox:   initial.Commands,
	}
	queue := &idempotentQueue{
		seen: make(map[string]bool),
		jobs: make(map[string]*pb.Node),
	}

	// 保存快照后、入队前崩溃：恢复不制造命令，直接重发已保存的 outbox。
	data, err := proto.Marshal(store.instance)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &pb.Instance{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	restored, err := w.Restore(decoded)
	if err != nil || len(restored.Commands) != 0 {
		t.Fatal("restore regenerated effects")
	}
	store.instance = restored.Instance
	for range 2 {
		if err := Dispatch(context.Background(), queue.handle, store.outbox); err != nil {
			t.Fatal(err)
		}
	}
	if len(queue.jobs) != 1 {
		t.Fatal("lost or duplicated initial execution")
	}

	event := report(store.instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED)
	if err := store.accept(w, event); err != nil {
		t.Fatal(err)
	}

	count, revision := len(store.outbox), store.instance.Revision
	if err := store.accept(w, event); err != nil || len(store.outbox) != count || store.instance.Revision != revision {
		t.Fatal("event redelivery created effects")
	}

	conflict := proto.Clone(event).(*pb.Event)
	conflict.Kind = pb.EventKind_EVENT_KIND_FAILED
	if err := store.accept(w, conflict); !errors.Is(err, ErrConflict) {
		t.Fatal("same event ID with another payload accepted")
	}
	if err := Dispatch(context.Background(), queue.handle, store.outbox); err != nil || len(queue.jobs) != 2 {
		t.Fatal("successor was lost or duplicated")
	}

	pause := control(store.instance, pb.EventKind_EVENT_KIND_PAUSE)
	if err := store.accept(w, pause); err != nil {
		t.Fatal(err)
	}
	if err := store.accept(w, control(store.instance, pb.EventKind_EVENT_KIND_RESUME)); err != nil {
		t.Fatal(err)
	}
	if err := store.accept(w, pause); err != nil || store.instance.Mode != modeRunning {
		t.Fatal("old pause event changed the current control state")
	}
}

func TestConcurrentResultsUseCompareAndSwap(t *testing.T) {
	w := mustCompile(t, Any("root", Task("a"), Task("b")))
	initial := start(t, w)
	store := &journal{
		instance: initial.Instance,
		receipts: make(map[string]*pb.Event),
	}

	a := report(initial.Instance, "a", pb.EventKind_EVENT_KIND_SUCCEEDED)
	b := report(initial.Instance, "b", pb.EventKind_EVENT_KIND_SUCCEEDED)
	first, err := w.Advance(initial.Instance, a)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := w.Advance(initial.Instance, b)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.commit(initial.Instance.Revision, a, first); err != nil {
		t.Fatal(err)
	}
	if err := store.commit(initial.Instance.Revision, b, stale); !errors.Is(err, ErrConflict) {
		t.Fatal("stale result overwrote the winner")
	}
	if err := store.accept(w, b); err != nil || store.instance.Nodes[0].WinnerId != "a" || !settled(store.instance) {
		t.Fatal("rebased result changed the first accepted winner")
	}
}
