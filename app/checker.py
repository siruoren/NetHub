from __future__ import annotations
"""节点连通性检测模块 - 通过启动本地内核实例转发流量检测延迟

检测策略（优先级从高到低）：
1. 内核转发检测：为每条节点启动一个内核实例，通过本地 HTTP 转发端口
   发送 HTTP 请求到检测目标 URL，测量完整往返时间（含协议握手+传输），
   最贴近真实上网体验
2. TCP/TLS 直连检测：直接 TCP 连接到节点服务器地址，测量连接建立耗时；
   若协议使用 TLS，则额外完成 TLS 握手（作为回退方案）
3. DNS 预解析：非 IP 地址先做 DNS 解析，解析失败直接判定不可达

参照逻辑：
- 连接被拒绝（refused）仍返回延迟值（端口可达）
- 超时则返回 None 表示不可达
"""

import asyncio
import json
import logging
import os
import socket
import ssl
import tempfile
import time
import base64
import shutil
from dataclasses import dataclass
from urllib.parse import urlparse, unquote, parse_qs

import aiohttp

from app.models import ProxyInfo

logger = logging.getLogger(__name__)

# 默认检测目标 URL（数据库为空时使用）
DEFAULT_CHECK_URLS = [
    "https://www.google.com/generate_204",
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/",
    "https://www.apple.com/library/test/success.html",
    "https://connectivitycheck.platform.hicloud.com/generate_204",
]

# 检测 URL 预期响应验证规则
CHECK_URL_VALIDATORS = {
    "generate_204": lambda url, status, body: status == 204 or (status == 200 and len(body) == 0),
    "cp.cloudflare.com": lambda url, status, body: status == 200 and len(body) > 0,
    "success.html": lambda url, status, body: status == 200 and len(body) > 0,
    "connectivitycheck": lambda url, status, body: status == 204 or status == 200,
}


def _validate_check_response(url: str, status: int, body: bytes) -> bool:
    """验证检测响应是否为有效响应（排除劫持页面/认证门户）

    默认规则：2xx 状态码即通过
    特定 URL 有额外验证规则
    """
    if not (200 <= status < 300):
        return False
    # 逐条匹配验证规则
    for keyword, validator in CHECK_URL_VALIDATORS.items():
        if keyword in url:
            return validator(url, status, body)
    # 通用规则：2xx + 有响应体或 204
    return status == 204 or len(body) > 0

# 本地转发端口默认值
DEFAULT_SOCKS_PORT = 1080
DEFAULT_HTTP_PORT = 1081


@dataclass
class ProxyConnInfo:
    """节点服务器连接信息"""
    host: str
    port: int
    use_tls: bool
    protocol: str


class ProxyChecker:
    """基于 asyncio 的并发节点连通性检测器

    检测模式：
    - http: 为每条节点启动本地内核实例，通过转发端口发送 HTTP 请求（最真实）
    - tcp: 直接 TCP/TLS 连接到节点服务器地址（回退方案）
    - auto: 优先内核转发，失败则回退 TCP
    """

    def __init__(self, check_urls: list[str], timeout: float, max_concurrent: int,
                 socks_port: int = 0, http_port: int = 0,
                 check_mode: str = "auto", kernel_path: str = "xray",
                 check_retries: int = 2):
        """
        Args:
            check_urls: HTTP 检测目标 URL 列表
            timeout: 单次检测超时秒数
            max_concurrent: 最大并发数
            socks_port: 本地 SOCKS5 转发端口（ConnectivityMonitor 使用）
            http_port: 本地 HTTP 转发端口（ConnectivityMonitor 使用）
            check_mode: 检测模式 "http" / "tcp" / "auto"
            kernel_path: 内核可执行文件路径
            check_retries: 单次检测失败后重试次数（提升有效性）
        """
        self.check_urls = check_urls or DEFAULT_CHECK_URLS
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.socks_port = socks_port if socks_port > 0 else DEFAULT_SOCKS_PORT
        self.http_port = http_port if http_port > 0 else DEFAULT_HTTP_PORT
        self.check_mode = check_mode
        self.kernel_path = self._resolve_kernel_path(kernel_path)
        self.check_retries = max(check_retries, 0)

    @staticmethod
    def _resolve_kernel_path(kernel_path: str) -> str:
        """解析内核路径，返回绝对路径或空字符串"""
        resolved = shutil.which(kernel_path)
        if resolved:
            return resolved
        if os.path.isfile(kernel_path) and os.access(kernel_path, os.X_OK):
            return kernel_path
        logger.warning("内核可执行文件未找到: %s，内核转发检测不可用", kernel_path)
        return ""

    async def check_proxy(self, link: str) -> float | None:
        """检测单个节点的延迟

        根据 check_mode 选择检测方式：
        - auto: 优先内核转发，失败则回退 TCP/TLS
        - http: 仅通过内核转发检测
        - tcp: 仅直连 TCP/TLS 检测

        支持重试：首次失败后重试 check_retries 次，取最快成功的延迟值
        """
        async with self.semaphore:
            best_latency = None

            for attempt in range(1 + self.check_retries):
                latency = await self._check_once(link)
                if latency is not None:
                    # 成功则取最快延迟
                    if best_latency is None or latency < best_latency:
                        best_latency = latency
                    # 首次成功就不再重试
                    if attempt == 0:
                        break
                    # 重试成功也直接返回（已有可用延迟）
                    break
                # 首次失败，短暂等待后重试
                if attempt < self.check_retries:
                    await asyncio.sleep(0.5)

            return best_latency

    async def _check_once(self, link: str) -> float | None:
        """单次检测节点延迟（不含重试逻辑）"""
        if self.check_mode == "tcp":
            conn_info = self._parse_link(link)
            if not conn_info:
                return None
            return await self._check_tcp_tls(conn_info)

        # http / auto 模式：优先内核转发
        if self.kernel_path:
            latency = await self._check_via_kernel(link)
            if latency is not None:
                return latency

        # auto 回退或 http 模式内核不可用时回退 TCP
        if self.check_mode == "auto" or not self.kernel_path:
            conn_info = self._parse_link(link)
            if conn_info:
                return await self._check_tcp_tls(conn_info)

        return None

    # ---- 内核转发检测 ----

    async def _check_via_kernel(self, link: str) -> float | None:
        """为单条节点启动内核实例，通过本地 HTTP 转发端口检测延迟

        流程：
        1. 将分享链接转换为内核 outbound 配置
        2. 分配一个空闲端口作为 HTTP inbound
        3. 生成完整内核配置并写入临时文件
        4. 启动内核进程
        5. 等待端口就绪
        6. 通过本地 HTTP 转发端口发送 HTTP 请求
        7. 测量延迟
        8. 终止内核进程并清理临时文件
        """
        outbound = self._link_to_xray_outbound(link)
        if not outbound:
            return None

        port = self._find_free_port()
        config = {
            "log": {"loglevel": "warning"},
            "inbounds": [{
                "tag": "http-in",
                "protocol": "http",
                "port": port,
                "listen": "127.0.0.1",
            }],
            "outbounds": [outbound],
        }

        config_path = ""
        proc = None
        try:
            # 写入临时配置文件
            fd, config_path = tempfile.mkstemp(suffix=".json", prefix="nethub_")
            with os.fdopen(fd, "w") as f:
                json.dump(config, f, ensure_ascii=False)

            # 启动内核进程
            proc = await asyncio.create_subprocess_exec(
                self.kernel_path, "run", "-c", config_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # 等待端口就绪
            ready = await self._wait_for_port(port, timeout=5.0)
            if not ready:
                return None

            # 通过本地 HTTP 转发端口发送请求
            return await self._http_request_via_proxy(port)

        except Exception:
            return None
        finally:
            # 终止内核进程
            if proc and proc.returncode is None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            # 清理临时文件
            if config_path:
                try:
                    os.unlink(config_path)
                except Exception:
                    pass

    async def _http_request_via_proxy(self, port: int) -> float | None:
        """通过本地 HTTP 转发端口发送 HTTP 请求检测延迟

        轮询多个检测 URL，使用 GET 请求并验证响应内容；
        确保节点能真正访问目标页面，排除劫持页面和认证门户
        """
        urls = self.check_urls if self.check_urls else DEFAULT_CHECK_URLS
        proxy_url = f"http://127.0.0.1:{port}"

        for check_url in urls:
            start = time.monotonic()
            try:
                connector = aiohttp.TCPConnector(limit=1, force_close=True)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        check_url,
                        proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                        allow_redirects=True,
                        ssl=False,
                    ) as resp:
                        # 读取响应体（限制最大 4KB 防止下载大文件）
                        body = await resp.content.read(4096)
                        elapsed = (time.monotonic() - start) * 1000

                        if _validate_check_response(check_url, resp.status, body):
                            return round(elapsed, 1)
                        # 验证不通过，尝试下一个 URL
            except Exception:
                continue
        return None

    @staticmethod
    def _find_free_port() -> int:
        """查找一个空闲 TCP 端口"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    @staticmethod
    async def _wait_for_port(port: int, timeout: float = 5.0) -> bool:
        """等待端口可连接"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", port),
                    timeout=1.0,
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return True
            except Exception:
                await asyncio.sleep(0.1)
        return False

    # ---- 内核配置生成 ----

    @staticmethod
    def _link_to_xray_outbound(link: str) -> dict | None:
        """将分享链接转换为内核 outbound 配置"""
        try:
            if link.startswith("vmess://"):
                return ProxyChecker._vmess_to_xray(link)
            elif link.startswith("vless://"):
                return ProxyChecker._vless_to_xray(link)
            elif link.startswith("trojan://"):
                return ProxyChecker._trojan_to_xray(link)
            elif link.startswith("ss://"):
                return ProxyChecker._ss_to_xray(link)
            elif link.startswith("hysteria2://") or link.startswith("hy2://"):
                return ProxyChecker._hysteria2_to_xray(link)
            elif link.startswith(("socks5://", "socks4://", "socks4a://")):
                return ProxyChecker._socks_to_xray(link)
            elif link.startswith(("http://", "https://")) and "#" in link:
                return ProxyChecker._http_to_xray(link)
        except Exception:
            pass
        return None

    @staticmethod
    def _vmess_to_xray(link: str) -> dict | None:
        """vmess:// 分享链接转内核 outbound"""
        try:
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config = json.loads(base64.b64decode(config_b64).decode("utf-8"))

            address = config.get("add", "")
            port = int(config.get("port", 0))
            if not address or not port:
                return None

            outbound = {
                "protocol": "vmess",
                "settings": {
                    "vnext": [{
                        "address": address,
                        "port": port,
                        "users": [{
                            "id": config.get("id", ""),
                            "alterId": int(config.get("aid", 0)),
                            "security": config.get("scy", "auto"),
                        }]
                    }]
                }
            }

            network = config.get("net", "tcp")
            tls = config.get("tls", "")
            stream = {"network": network}

            if tls == "tls":
                stream["security"] = "tls"
                tls_settings = {}
                if config.get("sni"):
                    tls_settings["serverName"] = config["sni"]
                if config.get("alpn"):
                    tls_settings["alpn"] = config["alpn"].split(",")
                if config.get("fp"):
                    tls_settings["fingerprint"] = config["fp"]
                if tls_settings:
                    stream["tlsSettings"] = tls_settings
            elif tls == "reality":
                stream["security"] = "reality"
                reality_settings = {}
                if config.get("sni"):
                    reality_settings["serverName"] = config["sni"]
                if config.get("pbk"):
                    reality_settings["publicKey"] = config["pbk"]
                if config.get("sid"):
                    reality_settings["shortId"] = config["sid"]
                if config.get("fp"):
                    reality_settings["fingerprint"] = config["fp"]
                if reality_settings:
                    stream["realitySettings"] = reality_settings
            else:
                stream["security"] = "none"

            if network == "ws":
                ws_settings = {}
                if config.get("path"):
                    ws_settings["path"] = config["path"]
                if config.get("host"):
                    ws_settings["headers"] = {"Host": config["host"]}
                stream["wsSettings"] = ws_settings
            elif network == "grpc":
                grpc_settings = {}
                if config.get("path"):
                    grpc_settings["serviceName"] = config["path"]
                stream["grpcSettings"] = grpc_settings
            elif network in ("h2", "http"):
                h2_settings = {}
                if config.get("path"):
                    h2_settings["path"] = config["path"]
                if config.get("host"):
                    h2_settings["host"] = [config["host"]]
                stream["httpSettings"] = h2_settings

            outbound["streamSettings"] = stream
            return outbound
        except Exception:
            return None

    @staticmethod
    def _vless_to_xray(link: str) -> dict | None:
        """vless:// 分享链接转内核 outbound"""
        try:
            parsed = urlparse(link)
            query = parse_qs(parsed.query)

            uuid = unquote(parsed.username) if parsed.username else ""
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None

            users = [{"id": uuid, "encryption": "none"}]
            flow = query.get("flow", [None])[0]
            if flow:
                users[0]["flow"] = flow

            outbound = {
                "protocol": "vless",
                "settings": {
                    "vnext": [{
                        "address": address,
                        "port": port,
                        "users": users,
                    }]
                }
            }

            network = query.get("type", ["tcp"])[0]
            security = query.get("security", ["none"])[0]
            stream = {"network": network}

            if security == "tls":
                stream["security"] = "tls"
                tls_settings = {}
                sni = query.get("sni", [None])[0]
                if sni:
                    tls_settings["serverName"] = sni
                alpn = query.get("alpn", [None])[0]
                if alpn:
                    tls_settings["alpn"] = alpn.split(",")
                fp = query.get("fp", [None])[0]
                if fp:
                    tls_settings["fingerprint"] = fp
                if tls_settings:
                    stream["tlsSettings"] = tls_settings
            elif security == "reality":
                stream["security"] = "reality"
                reality_settings = {}
                sni = query.get("sni", [None])[0]
                if sni:
                    reality_settings["serverName"] = sni
                pbk = query.get("pbk", [None])[0]
                if pbk:
                    reality_settings["publicKey"] = pbk
                sid = query.get("sid", [None])[0]
                if sid:
                    reality_settings["shortId"] = sid
                fp = query.get("fp", [None])[0]
                if fp:
                    reality_settings["fingerprint"] = fp
                if reality_settings:
                    stream["realitySettings"] = reality_settings
            else:
                stream["security"] = "none"

            if network == "ws":
                ws_settings = {}
                path = query.get("path", [None])[0]
                if path:
                    ws_settings["path"] = path
                host = query.get("host", [None])[0]
                if host:
                    ws_settings["headers"] = {"Host": host}
                stream["wsSettings"] = ws_settings
            elif network == "grpc":
                grpc_settings = {}
                service_name = query.get("serviceName", [None])[0]
                if service_name:
                    grpc_settings["serviceName"] = service_name
                stream["grpcSettings"] = grpc_settings
            elif network in ("h2", "http"):
                h2_settings = {}
                path = query.get("path", [None])[0]
                if path:
                    h2_settings["path"] = path
                host = query.get("host", [None])[0]
                if host:
                    h2_settings["host"] = [host]
                stream["httpSettings"] = h2_settings

            outbound["streamSettings"] = stream
            return outbound
        except Exception:
            return None

    @staticmethod
    def _trojan_to_xray(link: str) -> dict | None:
        """trojan:// 分享链接转内核 outbound"""
        try:
            parsed = urlparse(link)
            query = parse_qs(parsed.query)

            password = unquote(parsed.username) if parsed.username else ""
            address = parsed.hostname or ""
            port = parsed.port or 443
            if not address or not port:
                return None

            outbound = {
                "protocol": "trojan",
                "settings": {
                    "servers": [{
                        "address": address,
                        "port": port,
                        "password": password,
                    }]
                }
            }

            network = query.get("type", ["tcp"])[0]
            security = query.get("security", ["tls"])[0]
            stream = {"network": network, "security": "tls"}

            tls_settings = {}
            sni = query.get("sni", [None])[0]
            if sni:
                tls_settings["serverName"] = sni
            alpn = query.get("alpn", [None])[0]
            if alpn:
                tls_settings["alpn"] = alpn.split(",")
            fp = query.get("fp", [None])[0]
            if fp:
                tls_settings["fingerprint"] = fp
            if tls_settings:
                stream["tlsSettings"] = tls_settings

            if network == "ws":
                ws_settings = {}
                path = query.get("path", [None])[0]
                if path:
                    ws_settings["path"] = path
                host = query.get("host", [None])[0]
                if host:
                    ws_settings["headers"] = {"Host": host}
                stream["wsSettings"] = ws_settings
            elif network == "grpc":
                grpc_settings = {}
                service_name = query.get("serviceName", [None])[0]
                if service_name:
                    grpc_settings["serviceName"] = service_name
                stream["grpcSettings"] = grpc_settings

            outbound["streamSettings"] = stream
            return outbound
        except Exception:
            return None

    @staticmethod
    def _ss_to_xray(link: str) -> dict | None:
        """ss:// 分享链接转内核 outbound"""
        try:
            line = link
            if "#" in line:
                line = line[:line.rindex("#")]

            ss_content = line[5:]
            cipher = ""
            password = ""
            address = ""
            port = 0

            if "@" in ss_content:
                at_idx = ss_content.rindex("@")
                addr_port = ss_content[at_idx + 1:]
                user_info_b64 = ss_content[:at_idx]

                try:
                    padding = 4 - len(user_info_b64) % 4
                    if padding != 4:
                        user_info_b64 += "=" * padding
                    decoded = base64.b64decode(user_info_b64).decode("utf-8")
                    cipher, password = decoded.split(":", 1)
                except Exception:
                    pass

                if addr_port.startswith("["):
                    bracket_end = addr_port.index("]")
                    address = addr_port[1:bracket_end]
                    port = int(addr_port[bracket_end + 2:]) if bracket_end + 2 < len(addr_port) else 0
                elif ":" in addr_port:
                    address, port_str = addr_port.rsplit(":", 1)
                    port = int(port_str)
            else:
                try:
                    padding = 4 - len(ss_content) % 4
                    if padding != 4:
                        ss_content += "=" * padding
                    decoded = base64.b64decode(ss_content).decode("utf-8")
                    user_info, addr_port = decoded.rsplit("@", 1)
                    cipher, password = user_info.split(":", 1)
                    if addr_port.startswith("["):
                        bracket_end = addr_port.index("]")
                        address = addr_port[1:bracket_end]
                        port = int(addr_port[bracket_end + 2:]) if bracket_end + 2 < len(addr_port) else 0
                    elif ":" in addr_port:
                        address, port_str = addr_port.rsplit(":", 1)
                        port = int(port_str)
                except Exception:
                    pass

            if not address or not port:
                return None

            return {
                "protocol": "shadowsocks",
                "settings": {
                    "servers": [{
                        "address": address,
                        "port": port,
                        "method": cipher,
                        "password": password,
                    }]
                }
            }
        except Exception:
            return None

    @staticmethod
    def _hysteria2_to_xray(link: str) -> dict | None:
        """hysteria2:// 分享链接转内核 outbound"""
        try:
            prefix_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
            rest = link[prefix_len:]

            if "#" in rest:
                rest = rest[:rest.rindex("#")]

            if "?" in rest:
                host_part, query_part = rest.split("?", 1)
                parsed = urlparse("http://" + host_part)
                query = parse_qs(query_part)
            else:
                parsed = urlparse("http://" + rest)
                query = {}

            password = unquote(parsed.username) if parsed.username else ""
            address = parsed.hostname or ""
            port = parsed.port or 443
            if not address or not port:
                return None

            outbound = {
                "protocol": "hysteria2",
                "settings": {
                    "servers": [{
                        "address": address,
                        "port": port,
                        "password": password,
                    }]
                }
            }

            stream = {"network": "h2", "security": "tls"}
            sni = query.get("sni", [None])[0]
            if sni:
                stream["tlsSettings"] = {"serverName": sni}

            outbound["streamSettings"] = stream
            return outbound
        except Exception:
            return None

    @staticmethod
    def _socks_to_xray(link: str) -> dict | None:
        """socks5:// / socks4:// / socks4a:// 分享链接转内核 outbound"""
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None

            servers = [{"address": address, "port": port}]
            user = unquote(parsed.username) if parsed.username else ""
            password = unquote(parsed.password) if parsed.password else ""
            if user:
                servers[0]["users"] = [{"user": user, "pass": password}]

            return {
                "protocol": "socks",
                "settings": {"servers": servers},
            }
        except Exception:
            return None

    @staticmethod
    def _http_to_xray(link: str) -> dict | None:
        """http:// / https:// 代理链接转内核 outbound"""
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None

            servers = [{"address": address, "port": port}]
            user = unquote(parsed.username) if parsed.username else ""
            password = unquote(parsed.password) if parsed.password else ""
            if user:
                servers[0]["users"] = [{"user": user, "pass": password}]

            return {
                "protocol": "http",
                "settings": {"servers": servers},
            }
        except Exception:
            return None

    # ---- TCP/TLS 直连检测（回退方案） ----

    async def _check_tcp_tls(self, info: ProxyConnInfo) -> float | None:
        """直接 TCP/TLS 连接到节点服务器检测延迟（回退方案）

        注意：此方法仅验证服务器端口可达和 TLS 握手成功，
        不代表节点能真正转发流量到目标网站。
        连接被拒绝视为不可用，不返回延迟值。
        """
        host = info.host
        if not self._is_ip(host):
            resolved = await self._resolve_host(host)
            if not resolved:
                logger.debug("DNS 解析失败: %s", host)
                return None
            host = resolved

        start = time.monotonic()
        try:
            if info.use_tls:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host, info.port, ssl=ctx, server_hostname=info.host
                    ),
                    timeout=self.timeout,
                )
                # 验证 TLS 握手完成：检查 SSL 对象状态
                ssl_obj = writer.get_extra_info("ssl_object")
                if ssl_obj is None:
                    writer.close()
                    return None
            else:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, info.port),
                    timeout=self.timeout,
                )

            elapsed = (time.monotonic() - start) * 1000
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return round(elapsed, 1)
        except ConnectionRefusedError:
            # 连接被拒绝 = 不可用，不返回延迟值
            return None
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            # 连接被重置/中断 = 不可用
            return None
        except Exception:
            return None

    @staticmethod
    def _is_ip(host: str) -> bool:
        """判断是否为 IP 地址"""
        try:
            socket.inet_pton(socket.AF_INET, host)
            return True
        except (OSError, socket.error):
            pass
        try:
            socket.inet_pton(socket.AF_INET6, host)
            return True
        except (OSError, socket.error):
            pass
        return False

    @staticmethod
    async def _resolve_host(host: str) -> str | None:
        """异步 DNS 解析，返回第一个 IPv4/IPv6 地址"""
        try:
            loop = asyncio.get_running_loop()
            addrs = await loop.getaddrinfo(host, None)
            for family, _, _, _, sockaddr in addrs:
                if family in (socket.AF_INET, socket.AF_INET6):
                    return sockaddr[0]
            return None
        except Exception:
            return None

    async def check_batch(self, links: list[str]) -> dict[str, float | None]:
        """批量并发检测，返回 {link: latency_ms | None}"""
        tasks = [self.check_proxy(link) for link in links]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output = {}
        for link, result in zip(links, results):
            if isinstance(result, Exception):
                logger.debug("检测异常 %s: %s", link[:50], result)
                output[link] = None
            else:
                output[link] = result
        return output

    async def check_proxy_infos(self, proxies: list[ProxyInfo]) -> dict[str, float | None]:
        """批量检测 ProxyInfo 列表，返回 {link: latency_ms | None}"""
        links = [p.link for p in proxies]
        return await self.check_batch(links)

    # ---- 协议解析（TCP/TLS 回退方案使用） ----

    def _parse_link(self, link: str) -> ProxyConnInfo | None:
        """解析分享链接，提取连接信息（地址、端口、是否 TLS）"""
        try:
            if link.startswith("vmess://"):
                return self._parse_vmess(link)
            elif link.startswith("vless://"):
                return self._parse_vless(link)
            elif link.startswith("trojan://"):
                return self._parse_trojan(link)
            elif link.startswith("ss://"):
                return self._parse_ss(link)
            elif link.startswith("hysteria2://") or link.startswith("hy2://"):
                return self._parse_hysteria2(link)
            elif link.startswith(("socks5://", "socks4://", "socks4a://")):
                return self._parse_socks(link)
            elif link.startswith(("http://", "https://")) and "#" in link:
                return self._parse_http_proxy(link)
        except Exception:
            pass
        return None

    def _parse_vmess(self, link: str) -> ProxyConnInfo | None:
        try:
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config = json.loads(base64.b64decode(config_b64).decode("utf-8"))
            address = config.get("add", "")
            port = config.get("port", 0)
            if not address or not port:
                return None
            use_tls = config.get("tls", "") in ("tls", "reality")
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=use_tls, protocol="vmess",
            )
        except Exception:
            return None

    def _parse_vless(self, link: str) -> ProxyConnInfo | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            params = dict(pair.split("=", 1) for pair in parsed.query.split("&") if "=" in pair)
            security = params.get("security", "none")
            use_tls = security in ("tls", "reality")
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=use_tls, protocol="vless",
            )
        except Exception:
            return None

    def _parse_trojan(self, link: str) -> ProxyConnInfo | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=True, protocol="trojan",
            )
        except Exception:
            return None

    def _parse_ss(self, link: str) -> ProxyConnInfo | None:
        try:
            line = link
            if "#" in line:
                line = line[:line.rindex("#")]

            ss_content = line[5:]
            address = ""
            port = 0

            if "@" in ss_content:
                at_idx = ss_content.rindex("@")
                addr_port = ss_content[at_idx + 1:]
                if addr_port.startswith("["):
                    bracket_end = addr_port.index("]")
                    address = addr_port[1:bracket_end]
                    port = int(addr_port[bracket_end + 2:]) if bracket_end + 2 < len(addr_port) else 0
                elif ":" in addr_port:
                    address, port_str = addr_port.rsplit(":", 1)
                    port = int(port_str)
            else:
                try:
                    padding = 4 - len(ss_content) % 4
                    if padding != 4:
                        ss_content += "=" * padding
                    decoded = base64.b64decode(ss_content).decode("utf-8")
                    if "@" in decoded:
                        _, addr_port = decoded.rsplit("@", 1)
                        if ":" in addr_port:
                            address, port_str = addr_port.rsplit(":", 1)
                            port = int(port_str)
                except Exception:
                    pass

            if not address or not port:
                return None
            return ProxyConnInfo(
                host=address, port=port,
                use_tls=False, protocol="ss",
            )
        except Exception:
            return None

    def _parse_hysteria2(self, link: str) -> ProxyConnInfo | None:
        try:
            prefix_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
            rest = link[prefix_len:]
            parsed = urlparse("http://" + rest)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=True, protocol="hysteria2",
            )
        except Exception:
            return None

    def _parse_socks(self, link: str) -> ProxyConnInfo | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            protocol = "socks5" if link.lower().startswith("socks5://") else "socks4"
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=False, protocol=protocol,
            )
        except Exception:
            return None

    def _parse_http_proxy(self, link: str) -> ProxyConnInfo | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            protocol = "https" if link.lower().startswith("https://") else "http"
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=(protocol == "https"), protocol=protocol,
            )
        except Exception:
            return None


class ConnectivityMonitor:
    """连通性监控器 - 定期检测本地转发端口是否可达"""

    def __init__(self, socks_port: int = DEFAULT_SOCKS_PORT,
                 check_interval: float = 15.0,
                 timeout: float = 5.0,
                 backoff_base: float = 30.0,
                 backoff_max: float = 120.0):
        self.socks_port = socks_port if socks_port > 0 else DEFAULT_SOCKS_PORT
        self.check_interval = check_interval
        self.timeout = timeout
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

    async def probe(self) -> bool:
        """检测本地转发端口是否可达"""
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self.socks_port),
                timeout=self.timeout,
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False

    def backoff_delay(self, consecutive_failures: int) -> float:
        """计算失败后的退避等待时间"""
        delay = self.backoff_base
        for _ in range(1, consecutive_failures):
            delay *= 2
            if delay >= self.backoff_max:
                return self.backoff_max
        return delay
