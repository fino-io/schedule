// Package schedule compiles static Task/Seq/All/Any trees and reduces execution
// events into immutable snapshots and durable command intents. It owns neither
// persistence nor workers. The caller commits event receipts, snapshots and
// commands atomically, then delivers commands idempotently.
package schedule
