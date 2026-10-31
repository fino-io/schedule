## `schedule.WorkItemDefinition`

任务树的不可变定义；执行状态单独保存在运行快照中。
 每个节点必须选择一种任务结构，节点 ID 在整棵树内唯一。
 ID 沿用调用方提供的业务标识，不要求 UUID；公共字段保留原任务元数据。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `id` | `string` |  | 是 |  |  |
| `name` | `string` |  | 否 |  |  |
| `type` | `string` |  | 否 |  |  |
| `entityId` | `string` |  | 否 |  |  |
| `operationName` | `string` |  | 否 |  |  |
| `simpleTask` | `schedule.SimpleTaskDefinition` |  | 否 |  |  |
| `sequenceTask` | `schedule.SequenceTaskDefinition` |  | 否 |  |  |
| `parallelTask` | `schedule.ParallelTaskDefinition` |  | 否 |  |  |


## `schedule.SimpleTaskDefinition`

叶子任务；具体执行由调用方根据 type、entity_id、operation_name 分派。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|


## `schedule.SequenceTaskDefinition`

子节点可以继续嵌套串行或并行任务；items 必须非空且不包含空节点。
 沿用原 README 的顺序执行语义；首批任务只从第一个子节点递归取得。
 源库没有实现完成后的推进与失败处理，不在这里预设失败立即终止或继续执行。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `items` | `Array<schedule.WorkItemDefinition>` |  | 是 |  | 最小长度为 1 |


## `schedule.ParallelTaskDefinition`

首批任务从全部分支递归取得；沿用原 README 的任一分支成功即整体成功规则。
 成功状态的识别、全部失败或混合终态的汇总规则，须在 Go 实现阶段明确。
 后续编排采用以下补充约定（源库尚无执行实现）：提前成功不取消已启动分支，
 这些分支内的串行后续节点仍可执行，但结果不再改写已完成的并行节点。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `items` | `Array<schedule.WorkItemDefinition>` |  | 是 |  | 最小长度为 1 |
| `skipBranchError` | `boolean` |  | 否 |  | 保留原 SkipBranchError 数据；源实现没有消费此字段，暂不改变汇总规则。 |
| `master` | `string` |  | 否 |  | 保留原 Master 数据；源实现未定义其语义，暂不用于选择分支或决定成功。 |
