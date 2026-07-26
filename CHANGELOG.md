# Changelog

All notable changes to this project will be documented in this file.

## [2.1.0] - 2026-07-26

### 实例源精准同步

- **实例节点身份标识** - `verified_proxies` 表新增 `instance_node_name` 和 `instance_node_address` 列，存储实例 API 返回的节点名称和地址，作为稳定的唯一身份标识
- **基于身份的精准同步** - `sync_verified_proxies` 替代原 `batch_insert_verified_proxies`，按 `(instance_source_id, instance_node_name, instance_node_address)` 三元组精准匹配：
  - 身份已存在且 link 相同 → 跳过
  - 身份已存在但 link 变化 → UPDATE link 及字段，重置延迟为 -1 待重新验证
  - 身份不存在 → INSERT 新节点
  - 不在当前已连接列表中的节点 → 保留不清理
- **UNIQUE 约束重构** - `verified_proxies` 表的唯一约束从 `link` 改为 `(instance_source_id, instance_node_name, instance_node_address)`，允许不同实例身份匹配到相同 link 时独立入库
- **自动迁移** - 启动时检测旧表约束，自动重建表并迁移数据
- **匹配结果附带身份** - `fetch_connected_proxies` 返回类型从 `list[ProxyInfo]` 改为 `list[tuple[ProxyInfo, str, str]]`，每个匹配结果附带实例节点的名称和地址

### 节点数限制重构

- **全局订阅节点限制** - `max_proxies` 仅限制 `proxies` 表（订阅入库节点总数），不再合并计算 `verified_proxies`
- **全局实例节点限制** - 新增 `max_instance_nodes` 配置项，独立限制 `verified_proxies` 表（所有实例已验证节点总数），超出按延迟最高+入库最久优先删除
- **移除订阅级节点限制** - 移除每个订阅条的 `max_nodes` 字段及相关代码（`enforce_max_subscription_proxies`、API 参数、前端输入框、表格列）
- **前端显示** - 订阅节点数卡片显示 `可用数/max_proxies`，实例节点数卡片显示 `已入库数/max_instance_nodes`

### 调度优化

- **移除定时验证任务** - 不再每半小时定时验证所有入库节点，节点验证改为订阅拉取和实例获取后自动执行
- **仅验证新增节点** - 实例获取后仅检测新增节点的延迟，而非全量验证该实例下所有节点
- **断开节点保留** - 实例更新时不在当前已连接列表中的节点保留不清理，等待下次验证自然淘汰

### Docker

- **资源限制** - docker-compose 添加内存限制（上限 512M、保底 128M），防止夯死服务器

### Bug 修复

- **实例节点误判新增** - 修复模糊匹配导致同一连接节点在不同次运行匹配到不同 link 时误判为"新增"的问题
- **UNIQUE 冲突崩溃** - 修复 link 变化时 UPDATE 与 `verified_proxies.link` UNIQUE 约束冲突导致 `IntegrityError` 的问题
- **link 重复丢失节点** - 修复不同实例身份匹配到相同 link 时 `INSERT OR IGNORE` 跳过导致节点丢失的问题

---

## [2.0.0] - 2026-07-20

### 核心重构

- **节点-订阅关联重构** - 代理节点使用 `subscription_id`（整数）替代 `source`（字符串）绑定所属订阅源，已存在于其他订阅的节点不重复入库
- **纯文本订阅格式** - 对外订阅输出改为纯文本格式（每行一条原始代理 URI）；移除 base64 编码输出
- **内核转发检测** - 新增 Xray 内核转发检测能力，支持 HTTP/SOCKS 代理转发后检测连通性；TCP/TLS 直接检测作为回退
- **检测失败直接删除** - 取消 `fail_count` 累积逻辑，检测不通过的节点直接从数据库删除
- **实例源节点入库** - 服务实例获取的已连接节点写入已验证库（`verified_proxies`），与订阅节点库分开管理
- **socks4 协议移除** - 不再支持 socks4/socks4a 协议，拉取订阅时自动移除已有的 socks4 节点

### 多协议解析扩展

- **socks5/http 代理支持** - 新增 socks5:// 和 http(s):// 格式的解析、检测和 Clash 配置生成
- **Clash YAML 订阅解析** - 新增 Clash YAML 格式订阅解析，支持 vmess/vless/trojan/ss/hysteria2/socks5/http 7 种代理类型
- **http/https 仅带 #fragment 时视为节点** - 避免 URL 误判为代理节点
- **链接规范化** - socks5/http 代理对外输出确保格式为 `protocol://host:port#host-port`，保留认证信息
- **智能解析容错** - 自动跳过注释行（`#`/`//`），解析失败行去除 BOM、emoji、控制字符等行首特殊字符后重试

### 检测优化

- **多目标 URL 轮询** - 新增 5 个检测目标（Google 204、Gstatic 204、Cloudflare、Apple、华为连通性检测）
- **响应体验证** - 新增 `_validate_check_response` 排除劫持页面和空响应
- **GET + 4KB body 读取** - 替代 HEAD 请求，提升服务器兼容性
- **检测重试机制** - `check_retries` 参数（默认 2），单次检测失败后自动重试
- **ConnectionRefusedError/ResetError** - 不再返回延迟值（视为不可用）
- **TLS 回退检测** - SSL 对象状态验证，`server_hostname` 使用原始域名

### 调度优化

- **CronTrigger 随机延迟** - 所有 crontab 任务加入 `jitter`（0~600 秒），避免多订阅源同时更新
- **订阅验证防重入** - `_verifying_subs` 集合跟踪正在验证的订阅 ID，防止并发重复验证
- **拉取后自动验证** - `_fetch_single_subscription` 完成后自动触发该订阅的已入库节点验证
- **独立队列并发** - 订阅源（5并发）和实例源（3并发）使用独立信号量并行拉取

### 时间与日志

- **UTC+8 统一** - 所有服务时间统一为东八区（UTC+8），包括数据库时间戳和日志时间
- **单文件日志** - 日志写入 `logs/proxy_pool.log` 单文件，不归档、不保留历史日志
- **内核日志合并** - 内核 stdout/stderr 合并写入 `logs/proxy-core.log`

### 性能优化

- **SQLite WAL + PRAGMA 优化** - WAL 模式、synchronous=NORMAL、8MB 缓存、temp_store=MEMORY
- **复合索引** - subscription_id + created_at、latency_ms + fail_count + subscription_id 双索引加速查询
- **统计信息内存缓存** - 数据不变时直接返回缓存，避免重复 SQL 查询
- **独立 HTTP 连接** - 每次检测创建独立连接（force_close=True），检测结束立即销毁，无连接池残留
- **批量数据库操作** - 逐条插入实时刷新、合并元信息更新为单次 commit
- **N+1 查询优化** - IN 批量查询替代逐个 get_proxy_by_link
- **前端 JSON API 局部刷新** - 并行请求 3 个 JSON API 局部更新页面，替代整页 DOMParser 解析
- **协议分布图动态更新** - 15 秒统计刷新同步更新 Chart.js 饼图
- **CDN 预连接 + async/defer** - dns-prefetch、preconnect 提前建立连接；Chart.js async、Bootstrap JS defer 不阻塞首屏

### Web 界面

- **统计面板** - 订阅节点数（可用数/全局限制）、实例节点数（已入库/全局限制）、平均延迟、协议分布
- **分页显示** - 每个订阅源可用节点列表分页，每页 10 条
- **实例源管理** - 显示每个实例的已连接数和已入库数，支持手工导入订阅
- **配置导出/导入** - 一键导出订阅源、实例源和节点限制配置为 JSON 文件（含时间戳），导入时自动去重

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
| PUT | `/api/config/max-proxies` | 更新全局订阅节点限制 |
| PUT | `/api/config/max-instance-nodes` | 更新全局实例节点限制 |
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
