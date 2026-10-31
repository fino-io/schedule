## `schedule.Node`

静态组合树。每个节点代表一次执行位置，ID 在定义内唯一。
 Task 没有子项；Seq/All/Any 的子项非空，只有 Task 产生执行命令。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `id` | `string` |  | 否 |  |  |
| `name` | `string` |  | 否 |  |  |
| `kind` | [`schedule.NodeKind`](task.md#schedule-nodekind) |  | 否 |  |  |
| `children` | `Array<schedule.Node>` |  | 否 |  |  |
| `type` | `string` |  | 否 |  | 业务元数据由执行器解释，编排器不执行处理器或隐式传递输出。 |
| `entityId` | `string` |  | 否 |  |  |
| `operationName` | `string` |  | 否 |  |  |


<a id="schedule-nodekind"></a>

## `schedule.NodeKind`

| 枚举值 | 数值 | 说明 |
|---|---:|---|
| `NODE_KIND_UNSPECIFIED` | 0 |  |
| `NODE_KIND_TASK` | 1 |  |
| `NODE_KIND_SEQ` | 2 |  |
| `NODE_KIND_ALL` | 3 |  |
| `NODE_KIND_ANY` | 4 |  |
