package schedule

import (
	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// reconcile 推进活跃子树。结果确定的组合仍须遍历后代，Any 的剩余分支才能收尾。
func (w *Workflow) reconcile(instance *pb.Instance) {
	var visit func(int, bool, string)
	visit = func(i int, allowed bool, blocker string) {
		state, node := instance.Nodes[i], w.nodes[i]
		if instance.Mode == modeStopped {
			blocker = w.nodes[0].Id
		}

		if state.Phase == pending {
			if blocker != "" {
				state.Phase, state.CauseId = skipped, blocker
				for _, child := range node.children {
					visit(child, false, blocker)
				}
				return
			}

			if !allowed {
				return
			}

			if node.Kind == task {
				state.Phase = ready
			} else {
				state.Phase = running
			}
		}

		if node.Kind == task || state.Phase == skipped {
			return
		}

		if instance.Mode == modeStopped && !terminal(state.Phase) {
			state.Phase, state.CauseId = stopped, w.nodes[0].Id
		}

		if blocker == "" && (state.Phase == failed || state.Phase == stopped) {
			blocker = node.Id
		}

		prefixSucceeded := true
		for _, child := range node.children {
			phase := instance.Nodes[child].Phase
			canActivate := node.Kind != seq || prefixSucceeded || phase != pending
			prefixSucceeded = prefixSucceeded && phase == succeeded
			visit(child, allowed && canActivate, blocker)
		}

		w.resolve(instance, i, -1)
	}

	visit(0, instance.Mode == modeRunning, "")
}

// resolve 只锁定组合结果，不分派工作；preferred 是本次报告影响或恢复校验指定的直接子项。
func (w *Workflow) resolve(instance *pb.Instance, i, preferred int) {
	state, node := instance.Nodes[i], w.nodes[i]
	if state.Phase != running || node.Kind == task {
		return
	}

	children := node.children
	fail := func(child int) {
		state.Phase, state.CauseId = failed, w.nodes[child].Id
	}

	switch node.Kind {
	case seq, all:
		if node.Kind == all && preferred >= 0 {
			phase := instance.Nodes[preferred].Phase
			if terminal(phase) && phase != succeeded {
				fail(preferred)
				return
			}
		}

		complete := true
		for _, child := range children {
			phase := instance.Nodes[child].Phase
			if phase == succeeded {
				continue
			}
			if terminal(phase) {
				fail(child)
				return
			}
			if node.Kind == seq {
				return
			}
			complete = false
		}
		if complete {
			state.Phase = succeeded
		}

	case any:
		winner := -1
		complete := true
		for _, child := range children {
			phase := instance.Nodes[child].Phase
			if phase == succeeded && (winner < 0 || child == preferred) {
				winner = child
			}
			complete = complete && terminal(phase)
		}

		if winner >= 0 {
			state.Phase, state.WinnerId = succeeded, w.nodes[winner].Id
			state.ResultRef = instance.Nodes[winner].ResultRef
		} else if complete {
			state.Phase = failed
			state.ErrorMessage = proto.String("no successful branch")
		}
	}
}
