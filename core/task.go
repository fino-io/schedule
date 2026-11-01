package schedule

import (
	"errors"
	"slices"

	pb "github.com/fino-io/schedule/go/schedule"
)

var (
	ErrInvalidDefinition = errors.New("invalid definition")
	ErrInvalidSnapshot   = errors.New("invalid snapshot")
	ErrInvalidEvent      = errors.New("invalid event")
	ErrConflict          = errors.New("conflict")
	ErrNotFound          = errors.New("not found")
	ErrInvalidCommand    = errors.New("invalid command")
)

const (
	task = pb.NodeKind_NODE_KIND_TASK
	seq  = pb.NodeKind_NODE_KIND_SEQ
	all  = pb.NodeKind_NODE_KIND_ALL
	any  = pb.NodeKind_NODE_KIND_ANY

	pending   = pb.Phase_PHASE_PENDING
	ready     = pb.Phase_PHASE_READY
	running   = pb.Phase_PHASE_RUNNING
	succeeded = pb.Phase_PHASE_SUCCEEDED
	failed    = pb.Phase_PHASE_FAILED
	stopped   = pb.Phase_PHASE_STOPPED
	skipped   = pb.Phase_PHASE_SKIPPED

	modeRunning = pb.Mode_MODE_RUNNING
	modePaused  = pb.Mode_MODE_PAUSED
	modeStopped = pb.Mode_MODE_STOPPED
)

func Task(id string) *pb.Node {
	return &pb.Node{
		Id:   id,
		Kind: task,
	}
}

func Seq(id string, children ...*pb.Node) *pb.Node {
	return &pb.Node{
		Id:       id,
		Kind:     seq,
		Children: slices.Clone(children),
	}
}

func All(id string, children ...*pb.Node) *pb.Node {
	return &pb.Node{
		Id:       id,
		Kind:     all,
		Children: slices.Clone(children),
	}
}

func Any(id string, children ...*pb.Node) *pb.Node {
	return &pb.Node{
		Id:       id,
		Kind:     any,
		Children: slices.Clone(children),
	}
}

func terminal(phase pb.Phase) bool {
	return phase >= succeeded && phase <= skipped
}

func activated(phase pb.Phase) bool {
	return phase >= ready && phase <= stopped
}
