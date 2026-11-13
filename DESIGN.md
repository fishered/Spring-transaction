# Spring 集群 Saga 组件设计（当前实现）

## 目标与边界

本组件处理多个 Spring Boot 服务各自拥有数据库时的顺序业务流程。它通过持久化状态、参与方幂等、结果查询与补偿达到**可恢复的最终一致性**。HTTP 调用之间没有全局原子提交；业务数据可能在 Saga 运行期间短暂不一致。不可补偿的外部副作用必须由业务另行设计。

适合订单、库存等具有明确撤销动作的流程。需要同步原子提交且全部资源支持 XA 时，应评估 JTA/XA。Saga 不能承诺任何场景都自动成功或自动回滚。

## 运行结构

```text
发起服务                                   参与服务
SagaClient.start                          HTTP execute/query/compensate
    │                                             │
    ▼                                             ▼
saga_instance + saga_step                ParticipantExecutor
    │                                             │
    ▼                                             ▼
SagaCoordinator                           业务表 + participant_operation
    │                                     （同一数据库本地事务）
    └──────── HttpSagaAction ──────────────────────┘
```

应用注册版本化 `SagaDefinition`，每个 `SagaStep` 提供 `execute`、`query`、`compensate`。协调器每次只处理一个到期步骤；数据库事务不跨越远程 HTTP 调用。`start(workflowName, businessKey, payload)` 创建持久化实例并立即返回 ID；定时任务异步推进。相同业务键及相同请求复用原实例，不同请求被拒绝。

## 状态与故障处理

| 场景 | 协调器动作 |
| --- | --- |
| 执行明确成功 | 记录 `APPLIED`，推进下一步；最后一步后 `SUCCEEDED` |
| 执行明确拒绝 | 记录 `REJECTED`，从上一步开始逆序补偿；全部完成后 `COMPENSATED` |
| 执行请求异常或响应丢失 | 记录 `UNKNOWN`；恢复时先调用 `query`，根据参与方持久化结果决定推进、重试或人工处理 |
| 补偿请求异常或响应丢失 | 记录补偿 `UNKNOWN`；恢复时查询结果，已补偿则推进，仍已执行则重试补偿 |
| 结果无法判定或超过尝试上限 | 进入 `NEEDS_ATTENTION`，保留错误和步骤上下文；由业务排查后调用 `resume` |

每步 `operationId` 为 `sagaId:stepIndex`，在执行、查询和补偿中保持不变。参与方以该 ID 建唯一键；业务变更和 `participant_operation` 记录在同一个本地事务中。重复执行返回已保存结果。补偿先于执行到达时写入 `CANCELLED` 栅栏，后续执行不会产生副作用。补偿成功后返回 `COMPENSATED`。业务动作返回 `REJECTED` 前不得产生副作用；部分操作后抛异常才会回滚本地事务。

## 集群恢复

`saga_instance` 保存工作流版本、请求、当前步骤、租约、下次执行时间及错误。工作节点用条件更新领取租约，领取成功后执行单步；持久化进度时再次检查租约所有者和 epoch。节点崩溃或租约到期后，其他节点可接管。租约不能防止已经发出的 HTTP 请求重复，因此参与方操作必须幂等。数据库时间用于租约和扫描；HTTP 超时应短于租约。

升级工作流时，尚未结束的旧实例仍依赖原版本 `SagaDefinition`。旧版本定义必须一直注册到对应实例全部结束。人工恢复前先查看参与方真实状态，避免将未知的已执行动作当成未执行。

集群部署要求所有协调器副本共享同一状态库，所有相同参与服务副本共享其业务表与 `participant_operation`。领取租约的更新与实例读取处于同一事务，旧 epoch 的节点不能覆盖新节点进度。HTTP 调用可能在租约过期后继续运行，因而不能把租约当作恰好一次执行保证。

## 接入要求

- 协调器数据库应用 `db/cluster-tx/schema.sql`，每个参与方数据库至少建 `participant_operation`。表结构变更由应用的数据库迁移工具管理。
- 参与方业务 SQL 必须使用 `ParticipantExecutor` 所用的数据库和事务管理器。外部支付、消息发送等不受本地事务保护，需外部幂等键、结果查询及业务撤销。
- Servlet 参与方可将 `ParticipantAction` Bean 标注 `@SagaParticipant`，并设置 `cluster.tx.http.enabled=true` 自动注册执行、查询和补偿路由。HTTP 路由应实施服务间认证，并配置连接/读取超时。组件只定义传输协议，不替应用管理身份与权限。
- 应监控 `NEEDS_ATTENTION`、异常重试和长时间运行实例。当前版本没有内置指标或运维控制台。

仓库的 H2 集成测试覆盖独立数据库成功路径、明确拒绝后的补偿、响应丢失后的查询、补偿响应丢失、重复请求与租约竞争。`cluster-tx-sample` 提供可运行的 HTTP 双服务订单流程。PostgreSQL/MySQL 兼容性、真实宕机恢复及生产压测仍需针对部署环境验证。
