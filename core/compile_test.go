package schedule

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestCompileTree(t *testing.T) {
	branch := Any("branch", Task("a"), Seq("nested", Task("b"), Task("c")))
	root := Seq("root", branch)
	w := mustCompile(t, root)

	path, err := w.Path("c")
	if err != nil || !reflect.DeepEqual(path, []string{"root", "branch", "nested", "c"}) {
		t.Fatalf("path=%v err=%v", path, err)
	}

	branch.Id = "mutated"
	copy := w.Root()
	copy.Id = "mutated"
	node, err := w.Node("a")
	if err != nil {
		t.Fatal(err)
	}
	node.Id = "mutated"
	if _, err := w.Node("a"); err != nil || w.Root().Id != "root" {
		t.Fatal("compiled definition exposed mutable data")
	}

	if _, err := w.Node("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := w.Path("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestCompileRejectsInvalidTrees(t *testing.T) {
	cycle := Seq("cycle")
	cycle.Children = []*pb.Node{cycle}
	shared := Task("shared")
	deep := Task("leaf")
	for i := range maxDepth {
		deep = Seq(fmt.Sprint(i), deep)
	}

	cases := map[string]*pb.Node{
		"nil":          nil,
		"empty ID":     Task(""),
		"missing kind": {Id: "root"},
		"unknown kind": {
			Id:   "root",
			Kind: 99,
		},
		"nil child": Seq("root", nil),
		"empty Seq": Seq("root"),
		"empty All": All("root"),
		"empty Any": Any("root"),
		"leaf children": {
			Id:       "root",
			Kind:     task,
			Children: []*pb.Node{Task("a")},
		},
		"duplicate": All("root", Task("a"), Task("a")),
		"shared":    All("root", shared, shared),
		"cycle":     cycle,
		"depth":     deep,
	}
	for name, root := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(root); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("invalid definition accepted: %v", err)
			}
		})
	}
	if _, err := Compile(nil); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatal(err)
	}
}

func TestCompileLimits(t *testing.T) {
	items := make([]*pb.Node, maxNodes-1)
	for i := range items {
		items[i] = Task(fmt.Sprint(i))
	}
	root := All("root", items...)
	if _, err := Compile(root); err != nil {
		t.Fatal(err)
	}

	root.Children = append(root.Children, Task("extra"))
	if _, err := Compile(root); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatal("node limit not enforced")
	}
}

func TestCompileConcurrentUse(t *testing.T) {
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
	root := All("root", items...)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := Compile(root); err != nil {
			b.Fatal(err)
		}
	}
}
