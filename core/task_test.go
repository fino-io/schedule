package schedule

import "testing"

func TestAppend(t *testing.T) {
	root, branch := Seq("root"), Any("branch")
	if err := Append(root, branch); err != nil {
		t.Fatal(err)
	}
	if err := Append(branch, Task("a"), Seq("nested", Task("b"), Task("c"))); err != nil {
		t.Fatal(err)
	}
	if len(root.Children) != 1 || root.Children[0] != branch || len(branch.Children) != 2 {
		t.Fatal("children were not appended to the requested parent")
	}
}

func TestAppendRejectsInvalidChildren(t *testing.T) {
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
