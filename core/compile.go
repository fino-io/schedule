package schedule

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"unicode/utf8"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

const (
	maxDepth = 256
	maxNodes = 10000
)

// Workflow 是可并发复用的不可变定义和索引，不保存任何实例的运行状态。
type Workflow struct {
	hash  string
	nodes []compiledNode
	byID  map[string]int
}

type compiledNode struct {
	*pb.Node
	parent   int
	children []int
}

func Compile(root *pb.Node) (*Workflow, error) {
	w := &Workflow{byID: make(map[string]int)}
	var validate func(*pb.Node, int) error
	validate = func(node *pb.Node, depth int) error {
		if node == nil || node.Id == "" {
			return fmt.Errorf("%w: node and ID are required", ErrInvalidDefinition)
		}

		if depth > maxDepth || len(w.byID) >= maxNodes {
			return fmt.Errorf("%w: tree exceeds depth %d or node count %d", ErrInvalidDefinition, maxDepth, maxNodes)
		}

		// 共享节点和循环都会再次遇到同一个 ID，无须单独维护指针集合。
		if _, exists := w.byID[node.Id]; exists {
			return fmt.Errorf("%w: shared, cyclic or duplicate node %q", ErrInvalidDefinition, node.Id)
		}

		if node.Kind < task || node.Kind > any || (node.Kind == task) != (len(node.Children) == 0) {
			return fmt.Errorf("%w: invalid kind or children of %q", ErrInvalidDefinition, node.Id)
		}

		// 按前序分配索引，复制后的树保持同一顺序。
		w.byID[node.Id] = len(w.byID)
		for _, child := range node.Children {
			if err := validate(child, depth+1); err != nil {
				return err
			}
		}

		return nil
	}
	if err := validate(root, 1); err != nil {
		return nil, err
	}

	root = proto.Clone(root).(*pb.Node)

	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}

	digest := sha256.Sum256(data)
	w.hash = hex.EncodeToString(digest[:])

	var index func(*pb.Node, int) int
	index = func(node *pb.Node, parent int) int {
		i := len(w.nodes)
		w.nodes = append(w.nodes, compiledNode{
			Node:   node,
			parent: parent,
		})

		for _, child := range node.Children {
			childIndex := index(child, i)
			w.nodes[i].children = append(w.nodes[i].children, childIndex)
		}

		return i
	}

	index(root, -1)

	return w, nil
}

func (w *Workflow) Root() *pb.Node {
	return proto.Clone(w.nodes[0].Node).(*pb.Node)
}

func (w *Workflow) Node(id string) (*pb.Node, error) {
	i, exists := w.byID[id]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return proto.Clone(w.nodes[i].Node).(*pb.Node), nil
}

func (w *Workflow) Path(id string) ([]string, error) {
	i, exists := w.byID[id]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return w.path(i), nil
}

func (w *Workflow) path(i int) []string {
	var path []string
	for i >= 0 {
		path = append(path, w.nodes[i].Id)
		i = w.nodes[i].parent
	}

	slices.Reverse(path)

	return path
}

func stableID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func executionID(instanceID, nodeID string) string {
	return stableID("task", instanceID, nodeID)
}

func validText(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}
