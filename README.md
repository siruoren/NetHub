# ProxyPool - 代理池管理系统

自动获取、检测、维护代理节点池，提供 Web 界面和订阅链接。

## 功能特性

- 🔄 **定时拉取** - 自动从订阅源获取代理节点（vmess/vless/trojan/ss/hysteria2）
- ⚡ **延迟检测** - TCP/TLS 连通性检测，并发可配置
- 📊 **自动入库** - 延迟低于阈值的代理自动入库，超标自动删除
- ✅ **定时验证** - 定期验证已存代理可用性，连续失败自动清理
- 🌐 **Web 界面** - 展示代理列表、延迟、状态和统计信息
- 🔗 **订阅链接** - 提供 v2ray（base64）和 Clash（YAML）格式订阅
- 🐳 **一键部署** - Docker Compose 部署，数据持久化

## 快速开始

### Docker Compose（推荐）

```bash
# 克隆项目
git clone https://github.com/your-username/proxy_pool.git
cd proxy_pool

# 编辑订阅源
vim resources/Subscription.txt

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

# 编辑订阅源
vim resources/Subscription.txt

# 启动服务
python -m app.main
```

## 配置说明

编辑 `config.yaml` 进行配置：

```yaml
server:
  host: "0.0.0.0"
  port: 8080

check:
  timeout: 5.0           # 检测超时（秒）
  max_concurrent: 50     # 并发检测数
  latency_threshold: 3000.0  # 延迟阈值（毫秒）

scheduler:
  fetch_interval: 3600   # 拉取订阅间隔（秒）
  verify_interval: 1800  # 验证代理间隔（秒）
  cleanup_interval: 7200 # 清理间隔（秒）
  max_fail_count: 3      # 最大连续失败次数
```

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PROXY_POOL_PORT` | 服务端口 | 8080 |
| `PROXY_POOL_DB_PATH` | 数据库路径 | data/proxy_pool.db |

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/proxies` | 可用代理列表 |
| GET | `/api/proxies/all` | 所有代理 |
| DELETE | `/api/proxies/{id}` | 删除代理 |
| GET | `/api/subscription/v2ray` | V2Ray 订阅 |
| GET | `/api/subscription/clash` | Clash 订阅 |
| POST | `/api/fetch` | 手动拉取订阅 |
| POST | `/api/verify` | 手动验证代理 |
| GET | `/api/stats` | 统计信息 |
| GET | `/api/health` | 健康检查 |

## 订阅链接使用

在代理客户端中添加以下订阅链接：

- **V2Ray**: `http://your-server:8080/api/subscription/v2ray`
- **Clash**: `http://your-server:8080/api/subscription/clash`

## 支持协议

| 协议 | 订阅解析 | Clash 生成 |
|------|----------|-----------|
| VMess | ✅ | ✅ |
| VLESS | ✅ | ✅ |
| Trojan | ✅ | ✅ |
| Shadowsocks | ✅ | ✅ |
| Hysteria2 | ✅ | ✅ |

## 项目结构

```
proxy_pool/
├── app/
│   ├── __init__.py          # 应用工厂
│   ├── main.py              # 启动入口
│   ├── config.py            # 配置加载
│   ├── database.py          # 数据库操作
│   ├── models.py            # 数据模型
│   ├── parser.py            # 订阅解析
│   ├── checker.py           # 代理检测
│   ├── generator.py         # 订阅生成
│   ├── scheduler.py         # 定时任务
│   ├── routers/             # 路由
│   └── templates/           # 模板
├── resources/               # 资源文件
├── config.yaml              # 配置文件
├── docker-compose.yaml      # Docker 编排
└── Dockerfile               # Docker 镜像
```

## License

MIT License
