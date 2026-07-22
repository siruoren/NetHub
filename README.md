# NetHub v2.1.0

自动获取、检测、维护节点池，提供 Web 管理界面和订阅链接输出。

## 功能特性

- **订阅源管理** - 数据库驱动的增删改查，每个订阅源独立配置 Crontab、延迟阈值、并发数；删除订阅源时自动清除其下所有节点
- **Crontab 定时拉取** - 每个订阅源支持 5 位 Crontab 表达式，内置随机延迟（0~10 分钟）避免多源同时更新
- **内核转发检测** - Xray 内核转发后检测连通性，TCP/TLS 直接检测作为回退；多目标 URL 轮询 + 响应体验证 + 检测重试
- **检测失败直接删除** - 取消失败计数累积，检测不通过的节点直接从数据库删除
- **节点-订阅绑定** - 每个节点绑定所属 `subscription_id`，已存在于其他订阅的节点不重复入库
- **纯文本订阅输出** - 每行一条原始代理 URI，参照 `subdom.txt` 格式；同时提供 Clash（YAML）格式
- **多协议支持** - vmess / vless / trojan / ss / hysteria2 / socks5 / http(s) 解析、检测与 Clash 配置生成
- **Clash YAML 订阅解析** - 支持解析 Clash 格式的 YAML 订阅源，自动识别并转换为内部节点格式
- **智能解析容错** - 自动跳过注释行（`#`/`//`），解析失败行去除 BOM、emoji、控制字符等行首特殊字符后重试
- **服务实例源** - 获取已连接节点数量统计（不入库），支持手工导入实例源中的订阅地址
- **配置导出/导入** - 一键导出订阅源和实例源配置为 JSON 文件（含时间戳），导入时自动去重
- **UTC+8 时区统一** - 所有服务时间统一为东八区
- **单文件日志** - 不归档、不保留历史日志
- **Docker 部署** - Docker Compose 一键启动

## 性能优化

- **SQLite WAL + PRAGMA 优化** - WAL 模式、synchronous=NORMAL、8MB 缓存、temp_store=MEMORY
- **复合索引** - subscription_id + created_at、latency_ms + fail_count + subscription_id 双索引加速查询
- **统计信息内存缓存** - 数据不变时直接返回缓存，避免重复 SQL 查询
- **共享 HTTP session** - ProxyChecker 复用 aiohttp session，避免每次检测创建新连接
- **批量数据库操作** - executemany 批量插入/更新/删除，合并元信息更新为单次 commit
- **N+1 查询优化** - IN 批量查询替代逐个 get_proxy_by_link
- **前端 JSON API 局部刷新** - 并行请求 3 个 JSON API 局部更新页面，替代整页 DOMParser 解析
- **协议分布图动态更新** - 15 秒统计刷新同步更新 Chart.js 饼图
- **CDN 预连接 + async/defer** - dns-prefetch、preconnect 提前建立连接；Chart.js async、Bootstrap JS defer 不阻塞首屏

## 快速开始

### Docker Compose（推荐）

```bash
git clone https://github.com/your-username/proxy_pool.git
cd proxy_pool

# 按需修改配置
vim config.yaml

# 启动服务
docker-compose up -d
```

访问 http://localhost:2020 查看 Web 界面。

### 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
python -m app.main
```

## 配置说明

编辑 `config.yaml`：

```yaml
server:
  host: "0.0.0.0"
  port: 8080
  debug: false

database:
  path: "data/proxy_pool.db"

check:
  timeout: 5.0                   # 检测超时（秒）
  max_concurrent: 50             # 全局并发检测数
  latency_threshold: 1500.0      # 全局延迟阈值（毫秒）
  check_mode: "auto"             # 检测模式: auto(优先内核转发回退TCP) / http(仅内核转发) / tcp(仅TCP/TLS)
  socks_port: 1080               # 本地 SOCKS 转发端口
  http_port: 1081                # 本地 HTTP 转发端口
  kernel_path: "xray"            # 内核可执行文件路径
  check_retries: 2               # 单次检测失败后重试次数

scheduler:
  fetch_interval: 3600           # 拉取订阅间隔（秒）
  verify_interval: 1800          # 验证节点间隔（秒）
  cleanup_interval: 7200         # 清理间隔（秒）
```

> 检测目标 URL 默认包含 Google 204、Gstatic 204、Cloudflare、Apple、华为连通性检测等，首次启动自动写入数据库，后续在页面「检测目标」中管理，修改即时生效。

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PROXY_POOL_PORT` | 服务端口 | 8080 |
| `PROXY_POOL_DB_PATH` | 数据库路径 | data/proxy_pool.db |

## API 接口

### 节点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/proxies` | 可用节点列表 |
| GET | `/api/proxies/all` | 所有节点 |
| GET | `/api/proxies/grouped` | 按 subscription_id 分组的可用节点 |
| DELETE | `/api/proxies/{id}` | 删除节点 |
| DELETE | `/api/proxies` | 一键清除所有节点 |

### 订阅输出

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/subscription/plain` | 纯文本格式（每行一条 URI） |
| GET | `/api/subscription/v2ray` | 纯文本格式（同 plain） |
| GET | `/api/subscription/clash` | Clash 格式（YAML） |

### 订阅源管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/subscriptions` | 订阅源列表 |
| POST | `/api/subscriptions` | 添加订阅源 |
| POST | `/api/subscriptions/auto` | 自动添加（仅 URL 必填，自动拉取验证） |
| PUT | `/api/subscriptions/{sub_id}` | 更新订阅源 |
| DELETE | `/api/subscriptions/{sub_id}` | 删除订阅源及其下所有节点 |

### 拉取与验证

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/fetch` | 拉取所有订阅 |
| POST | `/api/fetch/{sub_id}` | 拉取指定订阅 |
| POST | `/api/verify` | 验证所有节点 |
| POST | `/api/verify/{sub_id}` | 验证指定订阅节点 |

### 服务实例源

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/instance-sources` | 服务实例源列表 |
| POST | `/api/instance-sources` | 添加服务实例源 |
| PUT | `/api/instance-sources/{source_id}` | 更新服务实例源 |
| DELETE | `/api/instance-sources/{source_id}` | 删除服务实例源 |
| POST | `/api/instance-sources/{source_id}/fetch` | 获取实例源已连接节点数 |
| POST | `/api/instance-sources/{source_id}/import` | 导入实例源订阅 |

### 配置管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/config/export` | 导出配置（JSON） |
| POST | `/api/config/import` | 导入配置（JSON，自动去重） |

### 检测目标

| 方法 | 路径 | 说明 |
|------|------|--------|
| GET | `/api/check-urls` | 检测目标 URL 列表 |
| POST | `/api/check-urls` | 添加检测目标 URL |
| DELETE | `/api/check-urls/{url_id}` | 删除检测目标 URL |

### 其他

| 方法 | 路径 | 说明 |
|------|------|--------|
| GET | `/api/stats` | 统计信息（总订阅条目数、可用节点数、平均延迟、协议分布） |
| GET | `/api/health` | 健康检查 |

## 订阅链接使用

在节点客户端中添加以下订阅链接：

- **纯文本**: `http://your-server:2020/api/subscription/plain`
- **Clash**: `http://your-server:2020/api/subscription/clash`

> 订阅内容仅包含延迟低于阈值的可用节点，随节点池自动更新。

## 支持协议

| 协议 | 订阅解析 | Clash 生成 |
|------|----------|-----------|
| VMess | ✅ | ✅ |
| VLESS | ✅ | ✅ |
| Trojan | ✅ | ✅ |
| Shadowsocks | ✅ | ✅ |
| Hysteria2 | ✅ | ✅ |
| SOCKS5 | ✅ | ✅ |
| HTTP/HTTPS | ✅ | ✅ |

> socks4/socks4a 协议不再支持，拉取时自动移除。

## 延迟检测原理

延迟检测模拟真实上网场景：通过 Xray 内核转发访问目标检测 URL，测量完整请求延迟。

```
客户端 → Xray 内核转发 → 目标网站
         ├── DNS 解析
         ├── TCP 连接建立
         ├── TLS 握手（HTTPS 目标）
         └── HTTP 请求/响应
```

- 检测目标包含 Google 204、Gstatic 204、Cloudflare、Apple、华为连通性检测等
- 多个目标取最大延迟值，确保所有目标均可达
- 响应体验证排除劫持页面和空响应
- 单次检测失败后自动重试（默认 2 次）
- 检测失败节点直接删除，不保留

## 项目结构

```
proxy_pool/
├── app/
│   ├── __init__.py          # 应用工厂 & 全局单例
│   ├── main.py              # 启动入口
│   ├── config.py            # YAML 配置加载
│   ├── database.py          # aiosqlite 异步数据库操作（WAL + 缓存 + 批量操作）
│   ├── models.py            # 数据模型（ProxyInfo / ProxyDBRecord / SubscriptionRecord）
│   ├── parser.py            # 订阅拉取 & 解析（7 协议 + Clash YAML + 容错重试）
│   ├── checker.py           # 内核转发检测 + TCP/TLS 回退检测（共享 session）
│   ├── generator.py         # 纯文本 / Clash 订阅生成
│   ├── scheduler.py         # APScheduler 定时任务调度（共享 checker + N+1 优化）
│   ├── routers/
│   │   ├── api.py           # REST API 路由
│   │   └── web.py           # Web 页面路由（精简序列化）
│   └── templates/
│       ├── base.html        # 基础模板（CDN 预连接 + defer）
│       ├── index.html       # 主页面（JSON API 局部刷新 + 分页）
│       └── subscription.html # 订阅链接页
├── logs/                    # 日志目录（单文件，不归档）
├── data/                    # 数据库目录
├── config.yaml              # 配置文件
├── docker-compose.yaml      # Docker 编排
├── Dockerfile               # Docker 镜像
├── CHANGELOG.md             # 变更日志
└── requirements.txt         # Python 依赖
```

## License

MIT License
