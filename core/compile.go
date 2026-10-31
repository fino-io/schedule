package schedule

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	definition *pb.Definition
	hash       string
	nodes      []*pb.Node
	byID       map[string]int
	parent     []int
	children   [][]int
}

func Compile(definition *pb.Definition) (*Workflow, error) {
	ids := make(map[string]bool)
	seen := make(map[*pb.Node]bool)
	var validate func(*pb.Node, int) error
	validate = func(node *pb.Node, depth int) error {
		if node == nil || node.Id == "" {
			return fmt.Errorf("%w: node and ID are required", ErrInvalidDefinition)
		}
		if depth > maxDepth || len(seen) >= maxNodes {
			return fmt.Errorf("%w: tree exceeds depth %d or node count %d", ErrInvalidDefinition, maxDepth, maxNodes)
		}
		if seen[node] || ids[node.Id] {
			return fmt.Errorf("%w: shared, cyclic or duplicate node %q", ErrInvalidDefinition, node.Id)
		}
		if node.Kind < task || node.Kind > any || (node.Kind == task) != (len(node.Children) == 0) {
			return fmt.Errorf("%w: invalid kind or children of %q", ErrInvalidDefinition, node.Id)
		}
		seen[node], ids[node.Id] = true, true
		for _, child := range node.Children {
			if err := validate(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validate(definition.GetRoot(), 1); err != nil {
		return nil, err
	}

	w := &Workflow{
		definition: proto.Clone(definition).(*pb.Definition),
		byID:       make(map[string]int, len(seen)),
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(w.definition)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	digest := sha256.Sum256(data)
	w.hash = hex.EncodeToString(digest[:])
	var index func(*pb.Node, int) int
	index = func(node *pb.Node, parent int) int {
		i := len(w.nodes)
		w.nodes = append(w.nodes, node)
		w.parent = append(w.parent, parent)
		w.children = append(w.children, nil)
		w.byID[node.Id] = i
		for _, child := range node.Children {
			childIndex := index(child, i)
			w.children[i] = append(w.children[i], childIndex)
		}
		return i
	}
	index(w.definition.Root, -1)
	return w, nil
}

func (w *Workflow) Definition() *pb.Definition {
	return proto.Clone(w.definition).(*pb.Definition)
}

func (w *Workflow) Node(id string) (*pb.Node, error) {
	i, exists := w.byID[id]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return proto.Clone(w.nodes[i]).(*pb.Node), nil
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
		i = w.parent[i]
	}
	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}
	return path
}

func stableID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		fmt.Fprintf(h, "%d:%s", len(part), part)
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
