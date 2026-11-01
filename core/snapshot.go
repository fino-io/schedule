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
	for i := len(w.nodes) - 1; i >= 0; i-- {
		node, state := w.nodes[i], instance.Nodes[i]
		bad := func(reason string) error {
			return fmt.Errorf("%w: node %q: %s", ErrInvalidSnapshot, node.Id, reason)
		}

		if node.Kind == task {
			if state.WinnerId != "" || state.Phase != skipped && state.CauseId != "" {
				return nil, bad("leaf has combination metadata")
			}
			if !terminal(state.Phase) && (state.ErrorMessage != nil || state.ResultRef != "") ||
				state.Phase == succeeded && state.ErrorMessage != nil {
				return nil, bad("result attached to a non-result state")
			}
		} else {
			if state.Phase == ready {
				return nil, bad("combination cannot be dispatched")
			}

			active := activated(state.Phase)
			prefixSucceeded, hasActiveChild := true, false
			for _, child := range node.children {
				phase := instance.Nodes[child].Phase
				childActive := activated(phase)
				if !active && phase != state.Phase {
					return nil, bad("inactive subtree has active descendants")
				}
				if node.Kind == seq && childActive && !prefixSucceeded {
					return nil, bad("later sequence item activated before its predecessor succeeded")
				}
				if node.Kind != seq && active && phase == pending {
					return nil, bad("parallel branch was not activated")
				}

				hasActiveChild = hasActiveChild || childActive
				prefixSucceeded = prefixSucceeded && phase == succeeded
			}
			if active && !hasActiveChild {
				return nil, bad("combination has no activated descendant")
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
					if !exists || w.nodes[child].parent != i {
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
			for parent := node.parent; parent >= 0; parent = w.nodes[parent].parent {
				if parent == cause {
					ancestor = true
					break
				}
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
