"""配置加载模块 - 从 YAML 文件加载配置"""

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False


@dataclass
class DatabaseConfig:
    path: str = "data/proxy_pool.db"


@dataclass
class CheckConfig:
    timeout: float = 5.0
    max_concurrent: int = 50
    latency_threshold: float = 1500.0
    check_mode: str = "auto"  # 检测模式: "auto" / "http" / "tcp"
    socks_port: int = 1080  # 本地转发端口（ConnectivityMonitor 使用）
    http_port: int = 1081  # 本地 HTTP 转发端口
    kernel_path: str = "v2ray"  # 内核可执行文件路径
    check_retries: int = 2  # 单次检测失败后重试次数


@dataclass
class SchedulerConfig:
    fetch_interval: int = 3600  # 拉取订阅间隔（秒）
    verify_interval: int = 1800  # 验证节点间隔（秒）
    max_proxies: int = 500  # 最大可用条目数，超出按入库时间最久+延迟最高优先删除
    max_instance_nodes: int = 0  # 所有实例已验证节点总数上限，0=不限制


@dataclass
class AppConfig:
    server: ServerConfig = field(default_factory=ServerConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    check: CheckConfig = field(default_factory=CheckConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)


def _deep_merge(base: dict, override: dict) -> dict:
    """深度合并字典，override 覆盖 base"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _dict_to_config(data: dict) -> AppConfig:
    """将字典转为 AppConfig dataclass"""
    server = ServerConfig(**data.get("server", {}))
    database = DatabaseConfig(**data.get("database", {}))
    check = CheckConfig(**data.get("check", {}))
    scheduler = SchedulerConfig(**data.get("scheduler", {}))
    return AppConfig(
        server=server,
        database=database,
        check=check,
        scheduler=scheduler,
    )


def load_config(path: str = "config.yaml") -> AppConfig:
    """从 YAML 文件加载配置，不存在则使用默认值"""
    defaults = {
        "server": {"host": "0.0.0.0", "port": 8080, "debug": False},
        "database": {"path": "data/proxy_pool.db"},
        "check": {
            "timeout": 5.0,
            "max_concurrent": 50,
            "latency_threshold": 1500.0,
            "check_mode": "auto",
            "socks_port": 1080,
            "http_port": 1081,
            "kernel_path": "v2ray",
            "check_retries": 2,
        },
        "scheduler": {
            "fetch_interval": 3600,
            "verify_interval": 1800,
            "max_proxies": 500,
            "max_instance_nodes": 0,
        },
    }

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            file_data = yaml.safe_load(f) or {}
        merged = _deep_merge(defaults, file_data)
    else:
        merged = defaults

    # 环境变量覆盖
    env_port = os.environ.get("PROXY_POOL_PORT")
    if env_port:
        merged["server"]["port"] = int(env_port)

    env_db_path = os.environ.get("PROXY_POOL_DB_PATH")
    if env_db_path:
        merged["database"]["path"] = env_db_path

    return _dict_to_config(merged)
