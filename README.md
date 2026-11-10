# Cluster Transaction

[![Maven build](https://github.com/fishered/Spring-transaction/actions/workflows/maven.yml/badge.svg)](https://github.com/fishered/Spring-transaction/actions/workflows/maven.yml)

[项目介绍页](https://fishered.github.io/Spring-transaction/) · [GitHub Actions](https://github.com/fishered/Spring-transaction/actions) · [设计说明](DESIGN.md)

适用于 Spring Boot 4 的顺序 Saga 组件。协调器把流程和步骤写入 JDBC 数据库；参与方在本地事务内提交业务变更及幂等记录。业务步骤明确拒绝时，协调器按相反顺序执行补偿。超时或响应丢失时，先用稳定的 `operationId` 查询参与方，再决定下一步。

**目前是可运行的 MVP。** 仓库提供真实 HTTP 双进程示例与 H2 集成测试。它提供可恢复的最终一致性，不提供跨服务的原子提交，也不能替业务自动生成补偿逻辑。生产接入前仍需在目标数据库、网络、部署与故障场景下验证。

## 已实现

| 模块 | 功能 |
| --- | --- |
| `cluster-tx-core` | 流程定义、操作契约、状态及客户端 API |
| `cluster-tx-jdbc` | Saga 状态表、租约、恢复扫描、参与方幂等与取消栅栏 |
| `cluster-tx-http` | 执行、查询和补偿的 JSON HTTP 适配器 |
| `cluster-tx-spring-boot-autoconfigure` | JDBC Bean、协调器及定时恢复任务的条件装配 |
| `cluster-tx-spring-boot-starter` | 业务应用统一依赖入口 |
| `cluster-tx-sample` | 可启动的订单协调器和库存参与方 |

协调器以 `(workflowName, businessKey)` 去重，持久化流程定义版本、请求内容、每一步结果及尝试次数。多个实例共享协调器数据库时，通过数据库租约争取处理权；租约过期后可由其他节点接管。已提交步骤在明确拒绝后逆序补偿。无法确认的结果与达到重试上限的步骤进入 `NEEDS_ATTENTION`，由业务排查后调用 `resume`。

## 运行示例

需要 Java 17+ 与 Maven。先在仓库根目录构建：

```bash
mvn -B -ntp clean verify
```

分别在两个终端启动库存服务和订单协调器。示例默认将两个 H2 数据库持久化在 `./data/`，进程重启后可继续处理未完成的 Saga。请在仓库根目录运行：

```bash
java -jar cluster-tx-sample/target/cluster-tx-sample-0.1.0-SNAPSHOT.jar --sample.role=participant --server.port=8081
```

```bash
java -jar cluster-tx-sample/target/cluster-tx-sample-0.1.0-SNAPSHOT.jar --sample.role=coordinator --server.port=8080 --sample.participant-url=http://localhost:8081
```

提交成功和拒绝订单：

```bash
curl -s -X POST http://localhost:8080/orders/demo-ok -H 'Content-Type: text/plain' -d ok
curl -s -X POST http://localhost:8080/orders/demo-reject -H 'Content-Type: text/plain' -d reject
curl -s http://localhost:8081/stock
```

`POST` 返回 `id`；用 `GET /sagas/{id}` 查询异步终态。正常订单应进入 `SUCCEEDED`，`GET /orders/demo-ok` 返回 `CONFIRMED`。拒绝订单应进入 `COMPENSATED`，`GET /orders/demo-reject` 返回 `CANCELLED`。库存只减少一件。重新提交相同订单号及相同正文将得到同一个 Saga ID；相同订单号、不同正文会被拒绝。

示例中的 `reject` 是明确的业务拒绝，用于展示补偿。协调器停止期间，新订单不会推进；重新启动后，恢复任务会继续扫描到期实例。

## 接入自己的服务

1. 在发起方和参与方应用中依赖 `cluster-tx-spring-boot-starter`。本仓库尚未发布 Maven Central；可先运行 `mvn install` 安装到本机 Maven 仓库。
2. 将 [`schema.sql`](cluster-tx-jdbc/src/main/resources/db/cluster-tx/schema.sql) 通过 Flyway 或 Liquibase 应用于协调器数据库。参与方数据库只需 `participant_operation` 表；它必须与参与方业务表处于同一数据库和事务管理器下。生产环境不要在每次启动时重建表。
3. 参与方实现 `ParticipantAction.execute/compensate`，交给 `ParticipantExecutor`；暴露执行、查询和补偿路由。可参考 [`SampleParticipant.java`](cluster-tx-sample/src/main/java/dev/cluster/tx/sample/SampleParticipant.java)。路由必须加服务间认证。
4. 发起方将 `SagaDefinition` 注册为 Spring Bean，使用 `HttpSagaAction` 与参与方通信，再注入 `SagaClient` 调用 `start/get/resume`。可参考 [`SampleCoordinator.java`](cluster-tx-sample/src/main/java/dev/cluster/tx/sample/SampleCoordinator.java)。HTTP 连接与读取超时需短于 `cluster.tx.lease-ms`。

示例的默认参数可通过 `cluster.tx.*` 覆盖：`scheduler-enabled`、`poll-delay-ms`、`lease-ms`、`retry-delay-ms`、`max-attempts`、`batch-size`。业务应监控 `NEEDS_ATTENTION`，确认外部结果后再选择 `RUNNING` 或 `COMPENSATING` 恢复方向。流程升级时必须保留未完成实例所使用的旧版本 `SagaDefinition`。

## 保证与限制

- 参与方业务变更与 `participant_operation` 必须使用同一事务管理器及数据库。`REJECTED` 必须没有业务副作用；已发生部分修改时应抛异常回滚。
- 执行与补偿通过 `operationId` 去重。补偿先到会写 `CANCELLED` 栅栏，迟到的执行请求不能再修改业务数据。
- 参与方自身调用支付网关、消息队列等外部资源时，需要外部系统提供幂等键、结果查询和可行的撤销机制；本地数据库事务不能覆盖这些资源。
- 租约只保证最终有节点接管，不保证远程调用只发生一次。业务动作与补偿必须可重复执行或由参与方幂等层保护。
- 当前数据库集成测试使用 H2；尚未完成 PostgreSQL/MySQL 的兼容性与并发压测，也没有内置鉴权、指标或运维后台。

GitHub Actions 在 Java 17 和 21 上运行 `mvn verify`。Pages 介绍页由 `site/` 自动部署。
