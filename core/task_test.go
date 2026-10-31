package schedule

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
)

func TestCompileAndAssembly(t *testing.T) {
	root, branch := Seq("root"), Any("branch")
	if err := Append(root, branch); err != nil {
		t.Fatal(err)
	}
	if err := Append(branch, Task("a"), Seq("nested", Task("b"), Task("c"))); err != nil {
		t.Fatal(err)
	}
	w := mustCompile(t, root)
	path, err := w.Path("c")
	if err != nil || !reflect.DeepEqual(path, []string{"root", "branch", "nested", "c"}) {
		t.Fatalf("path=%v err=%v", path, err)
	}
	branch.Id = "mutated"
	definition := w.Definition()
	definition.Root.Id = "mutated"
	node, err := w.Node("a")
	if err != nil {
		t.Fatal(err)
	}
	node.Id = "mutated"
	if _, err := w.Node("a"); err != nil || w.Definition().Root.Id != "root" {
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
		"nil": nil, "empty ID": Task(""), "missing kind": {Id: "root"},
		"unknown kind": {Id: "root", Kind: 99}, "nil child": Seq("root", nil),
		"empty Seq": Seq("root"), "empty All": All("root"), "empty Any": Any("root"),
		"leaf children": {Id: "root", Kind: task, Children: []*pb.Node{Task("a")}},
		"duplicate":     All("root", Task("a"), Task("a")),
		"shared":        All("root", shared, shared), "cycle": cycle, "depth": deep,
	}
	for name, root := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(&pb.Definition{Root: root}); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("invalid definition accepted: %v", err)
			}
		})
	}
	if _, err := Compile(nil); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatal(err)
	}
	if err := Append(Task("leaf"), Task("child")); err == nil {
		t.Fatal("leaf accepted children")
	}
	parent := Seq("parent", Task("existing"))
	if err := Append(parent, Task("new"), nil); err == nil || len(parent.Children) != 1 {
		t.Fatal("invalid append modified its parent")
	}
	if err := Append(parent, parent); err == nil {
		t.Fatal("direct self-reference accepted")
	}
}

func TestDefinitionLimits(t *testing.T) {
	items := make([]*pb.Node, maxNodes-1)
	for i := range items {
		items[i] = Task(fmt.Sprint(i))
	}
	root := All("root", items...)
	if _, err := Compile(&pb.Definition{Root: root}); err != nil {
		t.Fatal(err)
	}
	root.Children = append(root.Children, Task("extra"))
	if _, err := Compile(&pb.Definition{Root: root}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatal("node limit not enforced")
	}
}
