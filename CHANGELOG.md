# Changelog

All notable changes to this project will be documented in this file.

## [2.1.0] - 2026-07-22

### 订阅解析扩展

- **Clash YAML 格式支持** - 新增 `_is_clash_yaml` 检测、`_parse_clash_yaml` 解析和 `_clash_proxy_to_info` 转换，支持 vmess/vless/trojan/ss/hysteria2/socks5/http 7 种代理类型的 Clash YAML 订阅解析
- **解析错误行跳过** - 订阅源解析跳过 `#` 和 `//` 开头的注释行，解析失败的行在 debug 级别记录日志，不中断整体解析流程
- **行首特殊字符清理重试** - 解析每行时先尝试原行解析，失败后用 `re.sub(r'^[^a-zA-Z0-9]+', '', line)` 剻除 BOM、emoji、控制字符、空格、制表符、标点等行首特殊字符后重试

### 性能优化（后台）

- **SQLite WAL 模式 + PRAGMA 优化** - `journal_mode=WAL`、`synchronous=NORMAL`、`cache_size=-8000`（8MB）、`temp_store=MEMORY`，显著提升并发读写性能
- **复合索引** - 新增 `idx_proxies_sub_created(subscription_id, created_at)` 和 `idx_proxies_available(latency_ms, fail_count, subscription_id)`，加速分组查询和可用节点筛选
- **统计信息内存缓存** - `get_stats()` 使用 `_stats_cache` + `_stats_dirty` 脚标记机制，数据不变时直接返回缓存，避免重复 SQL 查询；所有修改数据的方法末尾调用 `_invalidate_stats()`
- **`get_stats` 合并查询** - 4 次 SQL 查询（subscriptions 计数 + proxies 可用数 + 平均延迟 + 协议分布）合并为 2 次（subscriptions 计数 + proxies `GROUP BY protocol` 一次获得可用数/加权平均延迟/协议分布）
- **共享 aiohttp session** - `ProxyChecker` 持有共享 `_session`，`_http_request_via_proxy` 复用而非每次检测创建新 `TCPConnector` + `ClientSession`；shutdown 时关闭 session
- **共享 checker** - scheduler 不再每次拉取/验证创建新 `ProxyChecker`，直接使用 `self.checker` 复用 HTTP session 和并发控制
- **批量插入节点** - `batch_insert_proxies` 使用 `executemany` + `INSERT OR IGNORE` 替代逐条 `execute` + `try/except IntegrityError`，单次 commit
- **socks4 过滤用 SQL 直接删除** - 新增 `delete_proxies_by_subscription_id_and_protocol`，不再全量获取再遍历过滤
- **N+1 查询优化** - `_fetch_single_subscription` 中用 `get_proxies_by_links`（`IN` 查询）替代逐个 `get_proxy_by_link`
- **`delete_subscription` 单次 commit** - 删除订阅源及其下所有节点合并为同一事务中的 2 条 DELETE，1 次 commit
- **合并元信息更新** - 新增 `batch_update_subscription_meta`（合并 total_count + fetch_status + reset_empty）和 `batch_update_instance_meta`（合并 total_count + fetch_status），单次 commit
- **`verify_stored_proxies` 分组查询** - 从 `get_all_proxies()` 全表扫描改为 `get_proxies_grouped_by_subscription`（利用复合索引），验证阈值放宽 2 倍避免误删
- **`get_proxies_grouped_by_subscription` 精简列** - `SELECT *` 12 列改为 `SELECT` 9 列（仅查模板需要的字段），减少数据传输和对象创建开销
- **Jinja2 模板缓存** - `cache_size` 从 0 改为 128，模板编译结果只生成一次后续直接复用

### 性能优化（前台）

- **JSON API 局部刷新** - `refreshPage()` 改为并行请求 `/api/stats` + `/api/proxies/grouped` + `/api/subscriptions` 三个 JSON API，局部更新统计数字、协议分布图、订阅源表格和当前 Tab 节点列表，替代整页 HTML → DOMParser 解析 → innerHTML 替换 → 正则提取 JS 数据
- **协议分布图动态更新** - 15 秒统计刷新时同步更新 Chart.js 饼图数据，使用 `update('none')` 无动画快速刷新
- **`subscriptions_json` 精简序列化** - 不再使用 `asdict()` 递归序列化所有 12 个字段，改为手动构造仅包含 JS 需要的 7~8 个字段的字典
- **`_proxy_to_dict` 精简** - 去掉不存在的 `status` 字段（修复 `AttributeError`）、前端不使用的 `last_check_time`/`last_success_time`，添加前端需要的 `created_at`
- **CDN 预连接** - `<link rel="dns-prefetch">` + `<link rel="preconnect">` 到 jsdelivr CDN，提前完成 DNS 解析和 TCP/TLS 连接建立
- **Chart.js 异步加载** - `<script async>` 加载不阻塞首屏渲染，饼图初始化改为 `initProtocolChart()` 函数带轮询等待（最多 3 秒）
- **Bootstrap JS `defer` 加载** - `<script defer>` 不阻塞 HTML 解析，`initTooltips()` 改为 `DOMContentLoaded` 回调中调用

---

## [2.0.0] - 2026-07-20

### 核心重构

- **节点-订阅关联重构** - 代理节点使用 `subscription_id`（整数）替代 `source`（字符串）绑定所属订阅源，已存在于其他订阅的节点不重复入库
- **纯文本订阅格式** - 对外订阅输出改为纯文本格式（每行一条原始代理 URI），参照 `subdom.txt` 样式；移除 base64 编码输出
- **内核转发检测** - 新增 Xray 内核转发检测能力，支持 HTTP/SOCKS 代理转发后检测连通性；TCP/TLS 直接检测作为回退
- **检测失败直接删除** - 取消 `fail_count` 累积逻辑，检测不通过的节点直接从数据库删除，不再保留
- **实例源节点不入库** - 服务实例获取的节点仅统计已连接数量，不再写入代理数据库
- **socks4 协议移除** - 不再支持 socks4/socks4a 协议，拉取订阅时自动移除已有的 socks4 节点

### 多协议解析扩展

- **socks5/http 代理支持** - 新增 socks5:// 和 http(s):// 格式的解析、检测和 Clash 配置生成
- **http/https 仅带 #fragment 时视为节点** - 避免 URL 误判为代理节点
- **链接规范化** - socks5/http 代理对外输出确保格式为 `protocol://host:port#host-port`，保留认证信息

### 检测优化

- **多目标 URL 轮询** - 新增 5 个检测目标（Google 204、Gstatic 204、Cloudflare、Apple、华为连通性检测）
- **响应体验证** - 新增 `_validate_check_response` 排除劫持页面和空响应
- **GET + 4KB body 读取** - 替代 HEAD 请求，提升服务器兼容性
- **检测重试机制** - `check_retries` 参数（默认 2），单次检测失败后自动重试
- **ConnectionRefusedError/ResetError** - 不再返回延迟值（视为不可用）
- **TLS 回退检测** - SSL 对象状态验证，`server_hostname` 使用原始域名
- **华为连通性检测** - 新增 `connectivitycheck.platform.hicloud.com/generate_204`

### 调度优化

- **CronTrigger 随机延迟** - 所有 crontab 任务加入 `jitter`（0~600 秒），避免多订阅源同时更新
- **订阅验证防重入** - `_verifying_subs` 集合跟踪正在验证的订阅 ID，防止并发重复验证
- **拉取后自动验证** - `_fetch_single_subscription` 完成后自动触发该订阅的已入库节点验证

### 时间与日志

- **UTC+8 统一** - 所有服务时间统一为东八区（UTC+8），包括数据库时间戳和日志时间
- **单文件日志** - 日志写入 `logs/proxy_pool.log` 单文件，不归档、不保留历史日志
- **内核日志合并** - 内核 stdout/stderr 合并写入 `logs/proxy-core.log`

### Web 界面

- **统计面板重构** - "总节点数"→"总订阅条目数"（显示订阅源数量），"可用数"→"可用节点数"，移除"不可用"统计卡片
- **分页显示** - 每个订阅源可用节点列表分页，每页 10 条
- **操作按钮更新** - "验证所有订阅"→"验证所有节点"，新增"清除所有节点"、"导出配置"、"导入配置"按钮
- **实例源表头** - "总数"→"已连接"，显示已连接节点数量而非数据库条目数

### 配置管理

- **一键导出/导入** - 导出订阅源和实例源配置为 JSON 文件（含时间戳），导入时自动去重
- **导出文件名时间戳** - 格式 `nethub_config_YYYYMMDD_HHMMSS.json`

### API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/proxies` | 可用节点列表 |
| GET | `/api/proxies/all` | 所有节点 |
| GET | `/api/proxies/grouped` | 按 subscription_id 分组的可用节点 |
| DELETE | `/api/proxies/{id}` | 删除节点 |
| DELETE | `/api/proxies` | 一键清除所有节点 |
| GET | `/api/subscription/plain` | 纯文本格式订阅 |
| GET | `/api/subscription/v2ray` | 纯文本格式订阅（同 plain） |
| GET | `/api/subscription/clash` | Clash 格式订阅（YAML） |
| POST | `/api/fetch` | 拉取所有订阅 |
| POST | `/api/fetch/{sub_id}` | 拉取指定订阅 |
| POST | `/api/verify` | 验证所有节点 |
| POST | `/api/verify/{sub_id}` | 验证指定订阅节点 |
| GET | `/api/stats` | 统计信息 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/subscriptions` | 订阅源列表 |
| POST | `/api/subscriptions` | 添加订阅源 |
| POST | `/api/subscriptions/auto` | 自动添加订阅源（仅 URL 必填） |
| PUT | `/api/subscriptions/{sub_id}` | 更新订阅源 |
| DELETE | `/api/subscriptions/{sub_id}` | 删除订阅源及其下所有节点 |
| GET | `/api/check-urls` | 检测目标 URL 列表 |
| POST | `/api/check-urls` | 添加检测目标 URL |
| DELETE | `/api/check-urls/{url_id}` | 删除检测目标 URL |
| GET | `/api/config/export` | 导出配置（JSON） |
| POST | `/api/config/import` | 导入配置（JSON） |
| GET | `/api/instance-sources` | 服务实例源列表 |
| POST | `/api/instance-sources` | 添加服务实例源 |
| PUT | `/api/instance-sources/{source_id}` | 更新服务实例源 |
| DELETE | `/api/instance-sources/{source_id}` | 删除服务实例源 |
| POST | `/api/instance-sources/{source_id}/fetch` | 获取实例源已连接节点数 |
| POST | `/api/instance-sources/{source_id}/import` | 导入实例源订阅 |

### Docker 构建

- **固定版本下载** - 内核数据文件使用固定版本标签，替代 `latest` 避免超时
- **curl 重试参数** - `--connect-timeout 30 --max-time 180 --retry 3 --retry-delay 5`

---

## [1.0.0] - 2026-07-18

### 核心功能

- **订阅源管理** - 数据库驱动的订阅源增删改查，支持独立配置 Crontab、延迟阈值、重试次数和最大并发数
- **Crontab 定时拉取** - 每个订阅源支持独立的 Crontab 表达式，通过 APScheduler CronTrigger 实现精准调度
- **HTTP 延迟检测** - 通过节点发起 HTTP 请求到多个目标 URL 检测延迟，取多目标最大延迟值作为结果
- **检测目标动态配置** - 检测目标 URL 存入数据库，页面可增删，修改后即时生效
- **多协议解析** - 支持 vmess / vless / trojan / ss / hysteria2 五种协议的订阅解析和 Clash 配置生成
- **对外订阅输出** - 提供核心（base64）和 Clash（YAML）格式订阅，仅包含当前可用节点
- **自动添加订阅 API** - `POST /api/subscriptions/auto` 接口支持仅传 URL 自动创建订阅并触发拉取验证

### 节点管理

- **独立延迟检测** - 每个节点独立测试延迟，报错自动跳过
- **延迟阈值过滤** - 延迟低于阈值自动入库，超标节点累加失败计数
- **连续失败清理** - 连续 3 次验证失败的节点自动移除
- **空订阅清理** - 连续 30 天节点数为 0 的订阅源自动删除
- **按订阅源分组** - 可用节点按订阅源分组，Tab 切换查看
- **延迟与入库时间排序** - 每个订阅源的节点列表支持延迟和入库时间升降序排序

### Web 界面

- **单页面管理** - 订阅源管理和可用节点列表合并为一个页面，无跳转
- **协议分布饼状图** - Chart.js 环形图动态展示各协议节点数量和百分比
- **订阅源状态** - 实时显示每个订阅源的拉取状态（更新中/成功/失败/待更新）
- **启用/禁用切换** - 状态列点击切换启用与禁用，启用后自动拉取并验证
- **编辑弹窗** - 操作列仅保留编辑按钮，删除功能移至编辑弹窗内
- **订阅地址提示** - 鼠标悬停订阅 ID 行或 Tab 标签时通过 Tooltip 显示订阅地址
- **可用节点 Tab 过滤** - 仅显示有可用节点的订阅源标签页

### 运维特性

- **日志按天归档** - TimedRotatingFileHandler 实现日志午夜自动轮转
- **日志自动清理** - 自动清理 7 天以前的归档日志
- **双输出日志** - 同时输出到控制台和文件
- **Docker Compose 部署** - 一键启动，数据持久化，时区配置

### API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/proxies` | 可用节点列表 |
| GET | `/api/proxies/all` | 所有节点 |
| DELETE | `/api/proxies/{id}` | 删除节点 |
| GET | `/api/subscription/v2ray` | 核心订阅（base64） |
| GET | `/api/subscription/clash` | Clash 订阅 |
| POST | `/api/fetch` | 拉取所有订阅 |
| POST | `/api/fetch/{sub_id}` | 拉取指定订阅 |
| POST | `/api/verify` | 验证所有节点 |
| POST | `/api/verify/{sub_id}` | 验证指定订阅节点 |
| GET | `/api/stats` | 统计信息 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/subscriptions` | 订阅源列表 |
| POST | `/api/subscriptions` | 添加订阅源 |
| POST | `/api/subscriptions/auto` | 自动添加订阅源（仅 URL 必填） |
| PUT | `/api/subscriptions/{sub_id}` | 更新订阅源 |
| DELETE | `/api/subscriptions/{sub_id}` | 删除订阅源 |
| GET | `/api/check-urls` | 检测目标 URL 列表 |
| POST | `/api/check-urls` | 添加检测目标 URL |
| DELETE | `/api/check-urls/{url_id}` | 删除检测目标 URL |
| GET | `/api/proxies/grouped` | 按订阅来源分组的可用节点 |
