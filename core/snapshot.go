package schedule

import (
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Restore 验证完整快照并返回副本；未确认命令由接入方按原 ID 补发。
func (w *Workflow) Restore(instance *pb.Instance) (*pb.Transition, error) {
	if instance == nil || instance.Id == "" || !validText(instance.Id) ||
		instance.DefinitionHash != w.hash || instance.Revision == 0 {
		return nil, fmt.Errorf("%w: instance identity, definition or revision", ErrInvalidSnapshot)
	}
	if instance.Mode < modeRunning || instance.Mode > modeStopped || len(instance.Nodes) != len(w.nodes) {
		return nil, fmt.Errorf("%w: mode or node count", ErrInvalidSnapshot)
	}

	for i, state := range instance.Nodes {
		if state == nil || state.NodeId != w.nodes[i].Id ||
			state.Phase < pending || state.Phase > skipped || !validText(state.ResultRef, state.GetErrorMessage()) {
			return nil, fmt.Errorf("%w: node order, identity or phase at %d", ErrInvalidSnapshot, i)
		}
	}
	if instance.Nodes[0].Phase == pending || instance.Nodes[0].Phase == skipped {
		return nil, fmt.Errorf("%w: root was never activated", ErrInvalidSnapshot)
	}

	normalized := proto.Clone(instance).(*pb.Instance)
	activated := make([]bool, len(w.nodes))
	for i := len(w.nodes) - 1; i >= 0; i-- {
		node, state := w.nodes[i], instance.Nodes[i]
		bad := func(reason string) error {
			return fmt.Errorf("%w: node %q: %s", ErrInvalidSnapshot, node.Id, reason)
		}

		if node.Kind == task {
			executed := state.Phase != pending && state.Phase != skipped
			if executed && state.ExecutionId != executionID(instance.Id, node.Id) ||
				!executed && state.ExecutionId != "" {
				return nil, bad("execution identity")
			}
			activated[i] = executed

			if state.WinnerId != "" || state.Phase != skipped && state.CauseId != "" {
				return nil, bad("leaf has combination metadata")
			}
			if !terminal(state.Phase) && (state.ErrorMessage != nil || state.ResultRef != "") ||
				state.Phase == succeeded && state.ErrorMessage != nil {
				return nil, bad("result attached to a non-result state")
			}
		} else {
			for _, child := range w.children[i] {
				activated[i] = activated[i] || activated[child]
			}
			if state.ExecutionId != "" || state.Phase == ready {
				return nil, bad("combination cannot be dispatched")
			}
			if state.Phase != pending && state.Phase != skipped && !activated[i] {
				return nil, bad("combination has no activated descendant")
			}

			if node.Kind == seq {
				prefixSucceeded := true
				for _, child := range w.children[i] {
					childState := instance.Nodes[child]
					if activated[child] && !prefixSucceeded {
						return nil, bad("later sequence item activated before its predecessor succeeded")
					}
					prefixSucceeded = prefixSucceeded && childState.Phase == succeeded
				}
			} else if state.Phase != pending && state.Phase != skipped {
				for _, child := range w.children[i] {
					if instance.Nodes[child].Phase == pending {
						return nil, bad("parallel branch was not activated")
					}
				}
			}
			if state.Phase == pending || state.Phase == skipped {
				for _, child := range w.children[i] {
					if instance.Nodes[child].Phase != state.Phase {
						return nil, bad("inactive subtree has active descendants")
					}
				}
			}

			if state.Phase == stopped && (instance.Mode != modeStopped || state.CauseId != w.nodes[0].Id) {
				return nil, bad("combination stopped without instance control")
			}

			// 重算组合结果，复用推进规则；Any 胜者和 All 首次失败来源属于历史决策。
			expected := normalized.Nodes[i]
			expected.WinnerId, expected.CauseId, expected.ResultRef = "", "", ""
			expected.ErrorMessage = nil
			if state.Phase == pending || state.Phase == skipped || state.Phase == stopped {
				if state.Phase != pending {
					expected.CauseId = state.CauseId
				}
			} else {
				preferred := -1
				decision := ""
				if node.Kind == any && state.Phase == succeeded {
					decision = state.WinnerId
				} else if node.Kind == all && state.Phase == failed {
					decision = state.CauseId
				}
				if decision != "" {
					child, exists := w.byID[decision]
					if !exists || w.parent[child] != i {
						return nil, bad("decision does not reference a direct child")
					}
					preferred = child
				}

				expected.Phase = running
				w.resolve(normalized, i, preferred)
			}
			if !proto.Equal(state, expected) {
				return nil, bad("inconsistent combination result or metadata")
			}
		}

		if state.Phase == skipped {
			cause, exists := w.byID[state.CauseId]
			validCause := exists && (instance.Nodes[cause].Phase == failed ||
				instance.Nodes[cause].Phase == stopped || instance.Mode == modeStopped && cause == 0)

			ancestor := false
			for parent := w.parent[i]; parent >= 0; parent = w.parent[parent] {
				ancestor = ancestor || parent == cause
			}

			if !validCause || !ancestor || state.ErrorMessage != nil || state.ResultRef != "" {
				return nil, bad("invalid skip source")
			}
		}
	}

	// 通过同一归约规则检查是否遗漏汇总、激活或跳过，不维护另一套推进行为。
	w.reconcile(normalized)
	if !proto.Equal(instance, normalized) {
		return nil, fmt.Errorf("%w: snapshot has unapplied transitions", ErrInvalidSnapshot)
	}

	return view(normalized), nil
}

func view(instance *pb.Instance) *pb.Transition {
	return &pb.Transition{
		Instance:    instance,
		ResultReady: terminal(instance.Nodes[0].Phase),
		Settled:     settled(instance),
	}
}

func settled(instance *pb.Instance) bool {
	for _, state := range instance.Nodes {
		if !terminal(state.Phase) {
			return false
		}
	}
	return true
}
