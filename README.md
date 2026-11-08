# Cluster Transaction

一个面向 Spring Boot 的跨服务事务组件设计与实现项目。目标是用持久化 Saga 流程管理跨服务操作，让超时、重复请求和节点宕机后的状态可以查询与恢复。

> **当前状态：设计与构建骨架。** 仓库目前只有全局状态机和 Starter 自动配置的初始代码；协调器、参与方持久化、网络传输及补偿执行尚未实现。请勿将它用于生产事务。

## 要解决的问题

本地 `@Transactional` 无法让多个独立服务的数据库自动同时提交。组件采用顺序 Saga：每个参与方提交自己的本地事务；发起方保存流程进度；失败时按业务定义补偿。它提供可恢复的**最终一致性**，不承诺跨 HTTP 调用的原子提交。完整边界、状态机与故障规则见 [设计文档](DESIGN.md)。

## 模块

| 模块 | 当前内容 | 计划职责 |
| --- | --- | --- |
| `cluster-tx-core` | 全局状态与转换规则 | 流程定义、动作契约、恢复策略 |
| `cluster-tx-jdbc` | Maven 模块骨架 | 协调器与参与方状态持久化 |
| `cluster-tx-spring-boot-autoconfigure` | 状态机 Bean 自动配置 | 条件装配与配置属性 |
| `cluster-tx-spring-boot-starter` | 依赖入口 | 为业务项目提供统一接入 |

## 构建

要求 Java 17+ 和 Maven 3.6.3+。在仓库根目录运行：

```bash
mvn -B -ntp verify
```

GitHub Actions 在 Java 17 和 21 上执行相同构建。构建通过只表示**当前骨架代码**编译和测试通过，不表示分布式事务功能已经完成。

## 开发路线

1. 状态机与 JDBC 仓储：持久化流程、步骤和审计记录。
2. 参与方执行器：业务变更与幂等记录同库同事务，处理迟到请求。
3. 集群调度：租约、版本检查、结果查询、补偿与恢复。
4. Spring Boot 接入与订单样例：故障注入、指标和运行文档。

项目介绍页源文件位于 [`site/`](site/)。推送到 `main` 后，Pages 工作流会发布该目录；首次发布需在仓库 Settings → Pages 将 Source 设为 **GitHub Actions**。

## 设计参考

- [Spring Boot Starter 自动配置](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html)
- [Spring Boot JTA 与 XA](https://docs.spring.io/spring-boot/reference/io/jta.html)
- [Seata Saga 机制与限制](https://seata.apache.org/docs/user/mode/saga/)
