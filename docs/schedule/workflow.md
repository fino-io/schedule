## `schedule.Definition`

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `root` | `schedule.Node` |  | 否 |  |  |


## `schedule.Instance`

调用方按 ID 原子创建实例，并用 revision 控制并发更新。
 快照与命令、已接受事件 ID 必须在同一事务中保存。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `id` | `string` |  | 否 |  |  |
| `definitionHash` | `string` |  | 否 |  |  |
| `revision` | `integer` | `Uint64` | 否 |  |  |
| `mode` | [`schedule.Mode`](workflow.md#schedule-mode) |  | 否 |  |  |
| `nodes` | `Array<schedule.NodeState>` |  | 否 |  | 严格按定义树前序排列，每个节点恰好一项。 |


## `schedule.NodeState`

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `nodeId` | `string` |  | 否 |  |  |
| `phase` | [`schedule.Phase`](workflow.md#schedule-phase) |  | 否 |  |  |
| `executionId` | `string` |  | 否 |  | 只供已产生分派意图的 Task 使用，重试不改变逻辑执行标识。 |
| `errorMessage` | `string` |  | 否 |  | 未设置与显式空字符串不同；不能由文本恢复 Go 错误链。 |
| `resultRef` | `string` |  | 否 |  | 由执行器管理的结果引用，不在快照内存放业务输出。 |
| `winnerId` | `string` |  | 否 |  | Any 首次成功的直接子项，锁定后不再修改。 |
| `causeId` | `string` |  | 否 |  | Seq/All 失败的直接子项，或跳过/停止的控制来源。 |


## `schedule.Event`

STARTED/SUCCEEDED/FAILED/STOPPED 是叶子报告，绑定实例、节点和逻辑执行。
 PAUSE/RESUME/STOP 是实例控制，node_id/execution_id 及结果载荷必须为空。
 事件 ID 的持久化去重由接入方负责；核心另外保证重复报告的语义幂等。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `id` | `string` |  | 否 |  |  |
| `instanceId` | `string` |  | 否 |  |  |
| `kind` | [`schedule.EventKind`](workflow.md#schedule-eventkind) |  | 否 |  |  |
| `nodeId` | `string` |  | 否 |  |  |
| `executionId` | `string` |  | 否 |  |  |
| `errorMessage` | `string` |  | 否 |  |  |
| `resultRef` | `string` |  | 否 |  |  |


## `schedule.Command`

自包含、可持久化的执行或通知意图。接入方按 id 幂等投递。
 STATE 包含节点状态变化或根的控制模式变化；END 只在首次全部收尾时产生。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `id` | `string` |  | 否 |  |  |
| `instanceId` | `string` |  | 否 |  |  |
| `definitionHash` | `string` |  | 否 |  |  |
| `revision` | `integer` | `Uint64` | 否 |  |  |
| `kind` | [`schedule.CommandKind`](workflow.md#schedule-commandkind) |  | 否 |  |  |
| `state` | `schedule.NodeState` |  | 否 |  |  |
| `previousPhase` | [`schedule.Phase`](workflow.md#schedule-phase) |  | 否 |  |  |
| `mode` | [`schedule.Mode`](workflow.md#schedule-mode) |  | 否 |  |  |
| `previousMode` | [`schedule.Mode`](workflow.md#schedule-mode) |  | 否 |  |  |
| `path` | `Array<string>` |  | 否 |  |  |
| `task` | `schedule.Node` |  | 否 |  | 只有 TASK 命令包含叶子定义，通知无需重新装载整棵树。 |


## `schedule.Transition`

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `instance` | `schedule.Instance` |  | 否 |  |  |
| `commands` | `Array<schedule.Command>` |  | 否 |  | 固定按状态通知、任务分派、结束通知排列；同类按定义顺序输出。 |
| `resultReady` | `boolean` |  | 否 |  | 派生视图：根结果已确定，不表示其他分支已经收尾。 |
| `settled` | `boolean` |  | 否 |  | 派生视图：全部节点已终结，没有剩余执行或待激活后继。 |


<a id="schedule-phase"></a>

## `schedule.Phase`

| 枚举值 | 数值 | 说明 |
|---|---:|---|
| `PHASE_UNSPECIFIED` | 0 |  |
| `PHASE_PENDING` | 1 |  |
| `PHASE_READY` | 2 |  |
| `PHASE_RUNNING` | 3 |  |
| `PHASE_SUCCEEDED` | 4 |  |
| `PHASE_FAILED` | 5 |  |
| `PHASE_STOPPED` | 6 |  |
| `PHASE_SKIPPED` | 7 |  |


<a id="schedule-mode"></a>

## `schedule.Mode`

| 枚举值 | 数值 | 说明 |
|---|---:|---|
| `MODE_UNSPECIFIED` | 0 |  |
| `MODE_RUNNING` | 1 |  |
| `MODE_PAUSED` | 2 |  |
| `MODE_STOPPED` | 3 |  |


<a id="schedule-eventkind"></a>

## `schedule.EventKind`

| 枚举值 | 数值 | 说明 |
|---|---:|---|
| `EVENT_KIND_UNSPECIFIED` | 0 |  |
| `EVENT_KIND_STARTED` | 1 |  |
| `EVENT_KIND_SUCCEEDED` | 2 |  |
| `EVENT_KIND_FAILED` | 3 |  |
| `EVENT_KIND_STOPPED` | 4 |  |
| `EVENT_KIND_PAUSE` | 5 |  |
| `EVENT_KIND_RESUME` | 6 |  |
| `EVENT_KIND_STOP` | 7 |  |


<a id="schedule-commandkind"></a>

## `schedule.CommandKind`

| 枚举值 | 数值 | 说明 |
|---|---:|---|
| `COMMAND_KIND_UNSPECIFIED` | 0 |  |
| `COMMAND_KIND_TASK` | 1 |  |
| `COMMAND_KIND_STATE` | 2 |  |
| `COMMAND_KIND_END` | 3 |  |
