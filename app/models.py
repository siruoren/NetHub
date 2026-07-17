"""ProxyPool 数据模型"""

from dataclasses import dataclass


@dataclass
class ProxyInfo:
    """从订阅解析出的代理信息"""

    protocol: str  # vmess / vless / trojan / ss / hysteria2
    name: str  # 节点名称
    address: str  # 服务器地址
    port: str  # 端口
    link: str  # 原始分享链接


@dataclass
class ProxyDBRecord:
    """数据库记录模型，与 proxies 表一一对应"""

    id: int
    protocol: str
    name: str
    address: str
    port: str
    link: str
    latency_ms: float  # 延迟毫秒，-1=未检测
    fail_count: int  # 连续失败次数
    source: str  # 来源订阅 URL
    last_check_time: str  # ISO8601
    last_success_time: str  # ISO8601
    created_at: str  # ISO8601

    @property
    def status(self) -> str:
        """代理状态：available / unavailable / unchecked"""
        if self.latency_ms < 0:
            return "unchecked"
        if self.fail_count > 0:
            return "unavailable"
        return "available"
