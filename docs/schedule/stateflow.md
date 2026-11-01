## `schedule.StateflowDefinition`

独立的业务状态流，不参与工作流执行生命周期。允许自环、循环和单态图。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `init` | `string` |  | 否 |  |  |
| `steps` | `Array<schedule.StateflowStep>` |  | 否 |  |  |


## `schedule.StateflowStep`

通过目标状态选择转换；event 是转换输出标签，不是输入触发器。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `from` | `string` |  | 否 |  |  |
| `to` | `string` |  | 否 |  |  |
| `event` | `string` |  | 否 |  |  |
