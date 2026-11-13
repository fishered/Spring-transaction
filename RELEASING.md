# Maven Central 发布准备

项目的 Maven 坐标为 `io.github.fishered:cluster-tx-spring-boot-starter`。根 POM 在 `central-release` profile 中配置了源码包、Javadoc、GPG 签名和 Sonatype Central Publishing Maven Plugin。发布包包含父 POM 与五个库模块；`cluster-tx-sample` 只作为示例构建，不进入 Central 发布包。

## 发布前的一次性设置

1. 使用 GitHub 账号 `fishered` 登录 [Sonatype Central Portal](https://central.sonatype.com/)，确认 `io.github.fishered` 命名空间已验证。GitHub 仓库公开并不等于拥有 Central 发布权限。
2. 在 Portal 生成发布用的 User Token，把 token 的用户名、密码放在本机 Maven `settings.xml` 的 `<server><id>central</id>...</server>` 中。不要将 token 提交到仓库。
3. 创建并发布 GPG 公钥，在本机配置可用于批量构建的私钥和口令。不要将私钥、口令提交到仓库。

## 每次发布

1. 确认工作树干净，`mvn -B -ntp clean verify` 和 PostgreSQL 集群用例通过；确认 `consumer-smoke` 可在 `mvn install` 后独立编译。
2. 将父 POM 和所有子模块的版本从 `0.1.0-SNAPSHOT` 改为目标正式版本，并同步 README、`consumer-smoke` 中的示例版本。正式版本不能以 `-SNAPSHOT` 结尾。
3. 先运行 `mvn -B -ntp -Pcentral-release clean verify`，检查五个库模块都生成主 JAR、`-sources.jar`、`-javadoc.jar` 和 `.asc` 签名；示例模块无需发布。
4. 运行 `mvn -B -ntp -Pcentral-release deploy` 上传发布包。也可在 GitHub Actions 手动运行 **Stage Maven Central release**，输入已写入 POM 的正式版本。使用 Action 前需在仓库 Secrets 设置 `CENTRAL_USERNAME`、`CENTRAL_PASSWORD`、`GPG_PRIVATE_KEY`（ASCII armored 私钥）和 `GPG_PASSPHRASE`。配置中 `autoPublish=false`，因此上传后需登录 Portal 检查验证结果并手动发布。
5. 等 Central 可查询后，在全新的 Maven 本地仓库中编译 `consumer-smoke`，确认无需先安装本仓库；然后创建对应版本的 Git tag 和 GitHub Release。

发布前请检查版本与 API 兼容性。Maven Central 中已发布的版本不可原地替换；发现问题应发布新版本。
