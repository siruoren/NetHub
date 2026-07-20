# Changelog

All notable changes to this project will be documented in this file.

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
| GET | `/api/subscription/v2ray` | 核心订阅 |
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
