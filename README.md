# 滚动新闻快照采集与归档

本服务面向新闻编辑部和数据运营人员：采集经常刷新的新闻目录页面，规范化标题与链接，并在本地 SQLite 中保留每次采集及文章出现历史。一次短暂的页面缺项不会立刻从归档删除新闻；操作者可以查看快照差异、采集健康、审核记录，并导出 CSV。

除 HTML 全量目录外，服务还支持第二类来源：本地可模拟的 JSON 增量接口。两类来源共存于同一归档，但按 feed 严格分开，互不影响。

## 环境

- Node.js `22.23.4`。
- 仅使用 Node 内置模块和 SQLite，无第三方包，不要求网络连接。

## 验收与演示

```sh
npm test
npm run build
npm run demo
npm run demo:json
```

HTML 演示读取本地合成页面并在临时目录建立数据库，输出一次采集摘要后清理临时文件。JSON 演示用合成增量接口跑通"初始同步 → 重投去重 → delete 证据 → 重对齐"的完整流程。生产数据库由 `ARCHIVE_DB` 指定，默认 `./var/archive.sqlite`。默认 HTTP 监听地址为 `127.0.0.1:8080`，可用 `HOST` 和 `PORT` 调整。

## 两类来源与全量/增量交接行为

注册 feed 时用 `kind` 区分来源类型：`html`（默认，既有行为不变）或 `json`。同一 feed 只能是其中一种；HTML 快照接口拒绝 JSON feed，事件与对齐接口拒绝 HTML feed。

JSON 来源的事件含稳定文章键、版本、`upsert`/`delete` 操作和下一游标。入库规则：

- **证据日志**：每个事件按 `(feed, 文章键, 版本, 操作)` 去重后写入 `feed_events`。重投的相同事件是幂等 `replay`，不改状态；同键同版本但操作不同的投递记为 `conflict` 证据。
- **版本不倒退**：文章键的已存版本只会变大。同键乱序到达的旧版本记为 `stale`，只留证据、不动归档；版本不高于本批次开始前游标水位的陌生键同样记为 `stale`，不会被旧事件"复活"。
- **游标推进**：游标只在事件确实入库的同一事务里推进，且只推进到本轮实际入库的最高版本对应的事件游标，绝不回退。纯重投批次不动游标；断线重放旧游标时，已读事件全部识别为 `replay`。
- **delete 保留证据**：`delete` 事件把文章转为 `quiet` 并在 `article_sources` 标记来源删除，文章行、审核状态和审核历史全部保留；之后出现更高版本的 `upsert` 会恢复 `visible`。
- **增量沉默 ≠ 缺失**：增量批次只处理被提到的事件，绝不因为某文章没出现就增加其缺失计数。

全量对齐（`POST /v1/feeds/:feedId/alignment` 或同步循环在来源要求时自动触发）是唯一的"全量观测"，目录带 `watermarkVersion` 与 `nextCursor`：

- 目录条目按版本守卫应用：条目版本高于已存版本才更新字段；否则只记一次全量观测（`seen_count`、缺失计数清零），不用旧目录覆盖新数据。
- 目录未列出且已存版本被水位覆盖的键才算"全量观测确实缺失"，缺失计数加一，达到阈值转 `quiet`；已存版本高于水位的键（对齐期间抵达的新增量）受保护，不计缺失、不被覆盖。
- 游标只在水位不低于已存最高版本时采用目录的 `nextCursor`；否则保留当前游标，避免对齐把游标拉回旧位置。
- 对齐与事件推送可以交错进行：对齐目录在途时到达的增量先入库，对齐提交时按上述版本守卫与水位规则合并，两边都不会丢。

`POST /v1/feeds/:feedId/sync` 从配置的 JSON 来源客户端拉取：每轮重新读取库内游标，来源要求重对齐（`resyncRequired`，例如游标被压缩淘汰）时先拉全量目录对齐再继续增量；同一 feed 的并发 sync 会合并为一次在途拉取。所有事件批次与对齐都写入 `sync_runs` 供健康与审计查询。

## HTTP 接口

- `POST /v1/feeds`：注册来源，提交 `feedId`、`baseUrl`、`allowedHosts`，以及可选的 `kind`（`html` 默认 / `json`）、`minimumItems`、`missingThreshold`。
- `POST /v1/feeds/:feedId/snapshots`：提交已保存的 HTML 页面；可用 `X-Captured-At` 指定采集时间。
- `POST /v1/feeds/:feedId/refresh`：手动抓取已注册来源；仅访问允许列表中的 DNS 主机。
- `POST /v1/feeds/:feedId/events`：提交一批 JSON 增量事件（数组或 `{events:[...]}`），返回逐事件结果与游标。
- `GET /v1/feeds/:feedId/events`：读取来源事件证据日志。
- `POST /v1/feeds/:feedId/alignment`：提交 JSON 全量目录（`items`、`watermarkVersion`、`nextCursor`）执行全量对齐。
- `POST /v1/feeds/:feedId/sync`：从配置的 JSON 来源客户端拉取事件，必要时自动全量对齐。
- `GET /v1/feeds/:feedId/syncs`：读取事件批次与全量对齐的运行记录。
- `GET /v1/feeds/:feedId`：查看 feed 状态、文章统计和采集健康；JSON feed 附带当前游标与最近同步记录。
- `GET /v1/feeds/:feedId/items`：分页读取文章；`visibility=visible|quiet` 可过滤状态；JSON 来源文章附带稳定文章键、来源版本和来源删除标记。
- `GET /v1/feeds/:feedId/runs`：读取近期页面快照接纳/拒绝记录。
- `GET /v1/feeds/:feedId/attempts`：读取抓取请求、重试、失败和响应审计。
- `GET /v1/feeds/:feedId/changes`：比较最近两次已接纳快照；未达到两次时返回 `409`。
- `GET /v1/feeds/:feedId/health`：读取 24 小时抓取/同步成功率、最近成功时间和异常提示。
- `GET /v1/feeds/:feedId/items.csv`：流式导出；限制输出行数并转义表格公式前缀。
- `PATCH /v1/feeds/:feedId/items/:articleId/review`：按修订号更新审核状态，状态为 `unreviewed`、`confirmed` 或 `withdrawn`。
- `GET /v1/feeds/:feedId/items/:articleId/review`：查看审核事件历史。
- `GET /v1/feeds/:feedId/items/:articleId/history`：查看文章在已接纳列表快照中的出现位置和间隔。
- `GET /health`：进程健康检查。

抓取失败、页面异常和正常列表变更有不同记录。相同页面指纹重复提交时不会重复增加文章观察次数。短暂缺失需达到来源配置的连续快照阈值后才转为 `quiet`，重新出现时恢复 `visible`。人工审核状态独立于可见性状态，旧修订号会得到冲突响应而不会覆盖新审核决定。

仓库中的 HTML 样本与合成 JSON 来源均为本地合成数据，仅用于离线演示和测试。实际部署前请配置准确来源域名，并遵守来源站点的使用条件。本服务不在后台隐式启动定时抓取或同步；由操作者或外部调度器显式触发。
