# ProxyPool v1.0.0

自动获取、检测、维护代理节点池，提供 Web 管理界面和订阅链接输出。

## 功能特性

- **订阅源管理** - 数据库驱动的增删改查，每个订阅源独立配置 Crontab、延迟阈值、重试次数、并发数
- **Crontab 定时拉取** - 每个订阅源支持 5 位 Crontab 表达式精准调度
- **HTTP 延迟检测** - 模拟通过代理访问目标网站，测量完整请求延迟（DNS + TCP + TLS + HTTP），多目标取最大值
- **检测目标动态配置** - 检测目标 URL 存入数据库，页面可增删，修改即时生效，无需外部文件
- **多协议支持** - vmess / vless / trojan / ss / hysteria2 解析与 Clash 配置生成
- **自动清理** - 连续 3 次验证失败的代理自动移除；连续 30 天无代理的订阅源自动删除
- **单页面管理** - 订阅源管理 + 可用代理列表在同一页面，按订阅源 Tab 切换
- **协议分布图** - 饼状图动态展示各协议代理数量和百分比
- **订阅输出** - 仅输出当前可用代理，支持 V2Ray（base64）和 Clash（YAML）格式
- **日志归档** - 按天自动归档，自动清理 7 天前的日志
- **Docker 部署** - Docker Compose 一键启动

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

访问 http://localhost:8080 查看 Web 界面。

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

scheduler:
  fetch_interval: 3600           # 拉取订阅间隔（秒）
  verify_interval: 1800          # 验证代理间隔（秒）
  cleanup_interval: 7200         # 清理间隔（秒）
  max_fail_count: 3              # 最大连续失败次数
```

> 检测目标 URL 默认为 `https://www.google.com/generate_204` 和 `https://www.gstatic.com/generate_204`，首次启动自动写入数据库，后续在页面「检测目标」中管理，修改即时生效。

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PROXY_POOL_PORT` | 服务端口 | 8080 |
| `PROXY_POOL_DB_PATH` | 数据库路径 | data/proxy_pool.db |

## API 接口

### 代理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/proxies` | 可用代理列表 |
| GET | `/api/proxies/all` | 所有代理 |
| GET | `/api/proxies/grouped` | 按订阅来源分组的可用代理 |
| DELETE | `/api/proxies/{id}` | 删除代理 |

### 订阅输出

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/subscription/v2ray` | V2Ray 格式订阅（base64） |
| GET | `/api/subscription/clash` | Clash 格式订阅（YAML） |

### 订阅源管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/subscriptions` | 订阅源列表 |
| POST | `/api/subscriptions` | 添加订阅源 |
| POST | `/api/subscriptions/auto` | 自动添加（仅 URL 必填，自动拉取验证） |
| PUT | `/api/subscriptions/{sub_id}` | 更新订阅源 |
| DELETE | `/api/subscriptions/{sub_id}` | 删除订阅源 |

### 拉取与验证

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/fetch` | 拉取所有订阅 |
| POST | `/api/fetch/{sub_id}` | 拉取指定订阅 |
| POST | `/api/verify` | 验证所有代理 |
| POST | `/api/verify/{sub_id}` | 验证指定订阅代理 |

### 检测目标

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/check-urls` | 检测目标 URL 列表 |
| POST | `/api/check-urls` | 添加检测目标 URL |
| DELETE | `/api/check-urls/{url_id}` | 删除检测目标 URL |

### 其他

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/stats` | 统计信息 |
| GET | `/api/health` | 健康检查 |

## 订阅链接使用

在代理客户端中添加以下订阅链接：

- **V2Ray**: `http://your-server:8080/api/subscription/v2ray`
- **Clash**: `http://your-server:8080/api/subscription/clash`

> 订阅内容仅包含延迟低于阈值的可用代理，随代理池自动更新。

## 支持协议

| 协议 | 订阅解析 | Clash 生成 |
|------|----------|-----------|
| VMess | ✅ | ✅ |
| VLESS | ✅ | ✅ |
| Trojan | ✅ | ✅ |
| Shadowsocks | ✅ | ✅ |
| Hysteria2 | ✅ | ✅ |

## 延迟检测原理

延迟检测模拟真实上网场景：通过代理服务器访问目标检测 URL，测量完整请求延迟。

```
客户端 → 代理服务器 → 目标网站
         ├── DNS 解析
         ├── TCP 连接建立
         ├── TLS 握手（HTTPS 目标）
         └── HTTP 请求/响应
```

- 检测目标默认为 `https://www.google.com/generate_204` 和 `https://www.gstatic.com/generate_204`
- 多个目标取最大延迟值，确保所有目标均可达
- 相比仅测试 TCP/TLS 连通性，HTTP 请求延迟更贴近真实上网体验

## 项目结构

```
proxy_pool/
├── app/
│   ├── __init__.py          # 应用工厂 & 全局单例
│   ├── main.py              # 启动入口
│   ├── config.py            # YAML 配置加载
│   ├── database.py          # aiosqlite 异步数据库操作
│   ├── models.py            # 数据模型（ProxyInfo / ProxyDBRecord / SubscriptionRecord）
│   ├── parser.py            # 订阅拉取 & 解析（5 协议）
│   ├── checker.py           # HTTP 延迟检测（多目标取最大值）
│   ├── generator.py         # V2Ray / Clash 订阅生成
│   ├── scheduler.py         # APScheduler 定时任务调度
│   ├── routers/
│   │   ├── api.py           # REST API 路由
│   │   └── web.py           # Web 页面路由
│   └── templates/
│       ├── base.html        # 基础模板
│       ├── index.html       # 主页面（管理 + 代理列表）
│       └── subscription.html # 订阅链接页
├── logs/                    # 日志目录（自动归档）
├── config.yaml              # 配置文件
├── docker-compose.yaml      # Docker 编排
├── Dockerfile               # Docker 镜像
├── CHANGELOG.md             # 变更日志
└── requirements.txt         # Python 依赖
```

## License

MIT License
