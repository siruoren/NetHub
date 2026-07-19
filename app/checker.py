from __future__ import annotations
"""节点连通性检测模块 - 参照服务实例项目的 HTTP 延迟检测逻辑重构

检测策略（优先级从高到低）：
1. HTTP 延迟检测：通过本地转发端口（SOCKS5/HTTP）发送 HTTP 请求到目标 URL，
   测量完整往返时间，最贴近真实上网体验
2. TCP/TLS 直连检测：直接 TCP 连接到节点服务器地址，测量连接建立耗时；
   若协议使用 TLS，则额外完成 TLS 握手
3. DNS 预解析：非 IP 地址先做 DNS 解析，解析失败直接判定不可达

参照逻辑：
- 服务实例项目的 Ping() 使用 net.DialTimeout("tcp") 测量直连延迟
- 服务实例项目的 connectivity 使用本地转发端口 TCP 拨号做连通性监控
- 服务实例项目的 HTTP 延迟通过转发端口发送 HTTP HEAD/GET 请求测真实延迟
- 连接被拒绝（refused）仍返回延迟值（端口可达）
- 超时则返回 None 表示不可达
"""

import asyncio
import json
import logging
import socket
import ssl
import time
import base64
from dataclasses import dataclass
from urllib.parse import urlparse, unquote

import aiohttp

from app.models import ProxyInfo

logger = logging.getLogger(__name__)

# 默认检测目标 URL（数据库为空时使用）
DEFAULT_CHECK_URLS = [
    "https://www.google.com/generate_204",
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/",
    "https://www.apple.com/library/test/success.html",
]

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
    - http: 通过本地转发端口发送 HTTP 请求（最真实，需要转发服务已启动）
    - tcp: 直接 TCP/TLS 连接到节点服务器地址
    - auto: 优先 HTTP，失败则回退 TCP
    """

    def __init__(self, check_urls: list[str], timeout: float, max_concurrent: int,
                 socks_port: int = 0, http_port: int = 0,
                 check_mode: str = "auto"):
        """
        Args:
            check_urls: HTTP 检测目标 URL 列表
            timeout: 单次检测超时秒数
            max_concurrent: 最大并发数
            socks_port: 本地 SOCKS5 转发端口（0=未启用）
            http_port: 本地 HTTP 转发端口（0=未启用）
            check_mode: 检测模式 "http" / "tcp" / "auto"
        """
        self.check_urls = check_urls or DEFAULT_CHECK_URLS
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.socks_port = socks_port if socks_port > 0 else DEFAULT_SOCKS_PORT
        self.http_port = http_port if http_port > 0 else DEFAULT_HTTP_PORT
        self.check_mode = check_mode

    async def check_proxy(self, link: str) -> float | None:
        """检测单个节点的延迟

        根据 check_mode 选择检测方式：
        - auto: 优先 HTTP，失败则回退 TCP/TLS
        - http: 仅通过本地转发端口检测
        - tcp: 仅直连 TCP/TLS 检测
        """
        conn_info = self._parse_link(link)
        if not conn_info:
            return None

        async with self.semaphore:
            if self.check_mode == "http":
                return await self._check_http_latency()
            elif self.check_mode == "tcp":
                return await self._check_tcp_tls(conn_info)
            else:
                # auto: 优先 HTTP，失败回退 TCP
                latency = await self._check_http_latency()
                if latency is not None:
                    return latency
                return await self._check_tcp_tls(conn_info)

    async def _check_http_latency(self) -> float | None:
        """通过本地转发端口发送 HTTP 请求检测延迟

        参照服务实例项目的 HTTP 延迟检测逻辑：
        通过本地 SOCKS5/HTTP 转发端口向检测目标 URL 发送 HTTP 请求，
        测量完整往返时间（含 DNS + TCP + TLS + HTTP），
        这与真实上网体验最接近。
        """
        check_url = self.check_urls[0] if self.check_urls else DEFAULT_CHECK_URLS[0]
        start = time.monotonic()
        try:
            # 优先使用 SOCKS5 转发端口
            connector = aiohttp.TCPConnector(
                limit=1, force_close=True,
            )
            proxy_url = f"http://127.0.0.1:{self.http_port}"
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.head(
                    check_url,
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    allow_redirects=True,
                    ssl=False,
                ) as resp:
                    # 204 / 200 / 301 等均视为连通
                    elapsed = (time.monotonic() - start) * 1000
                    if resp.status < 500:
                        return round(elapsed, 1)
                    return None
        except Exception:
            return None

    async def _check_tcp_tls(self, info: ProxyConnInfo) -> float | None:
        """直接 TCP/TLS 连接到节点服务器检测延迟

        参照服务实例项目的 Ping() 逻辑：
        1. DNS 预解析（非 IP 地址先解析）
        2. TCP DialTimeout 测量连接建立耗时
        3. TLS 协议额外完成 TLS 握手
        4. 连接被拒绝（refused）仍返回延迟（端口可达）
        5. 超时返回 None
        """
        # DNS 预解析
        host = info.host
        if not self._is_ip(host):
            resolved = await self._resolve_host(host)
            if not resolved:
                logger.debug("DNS 解析失败: %s", host)
                return None
            host = resolved

        # TCP/TLS 连接
        start = time.monotonic()
        try:
            if info.use_tls:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host, info.port, ssl=ctx, server_hostname=host
                    ),
                    timeout=self.timeout,
                )
            else:
                _, writer = await asyncio.wait_for(
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
            # 参照服务实例逻辑：连接被拒绝仍返回延迟（端口可达但拒绝连接）
            elapsed = (time.monotonic() - start) * 1000
            return round(elapsed, 1)
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
        """异步 DNS 解析，返回第一个 IPv4/IPv6 地址

        参照服务实例项目的 resolv.LookupHost() 逻辑
        """
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

    # ---- 协议解析 ----

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
        except Exception:
            pass
        return None

    def _parse_vmess(self, link: str) -> ProxyConnInfo | None:
        try:
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config_json = base64.b64decode(config_b64).decode("utf-8")
            config = json.loads(config_json)
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


class ConnectivityMonitor:
    """连通性监控器 - 参照服务实例项目的 connectivity 逻辑

    定期通过 TCP 拨号检测本地转发端口是否可达，
    用于判断转发服务是否正常运行。
    连续失败使用指数退避重试。
    """

    def __init__(self, socks_port: int = DEFAULT_SOCKS_PORT,
                 check_interval: float = 15.0,
                 timeout: float = 5.0,
                 backoff_base: float = 30.0,
                 backoff_max: float = 120.0):
        """
        Args:
            socks_port: 本地转发端口
            check_interval: 正常检测间隔（秒）
            timeout: 单次检测超时（秒）
            backoff_base: 失败退避基础延迟（秒）
            backoff_max: 失败退避最大延迟（秒）
        """
        self.socks_port = socks_port if socks_port > 0 else DEFAULT_SOCKS_PORT
        self.check_interval = check_interval
        self.timeout = timeout
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

    async def probe(self) -> bool:
        """检测本地转发端口是否可达

        参照服务实例项目的 probePhysicalConnectivity() 逻辑：
        TCP 拨号到本地转发端口，成功则可达。
        """
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
        """计算失败后的退避等待时间

        参照服务实例项目的 connectivityBackoffDelay() 逻辑：
        - 初始延迟 = backoff_base
        - 每次失败延迟翻倍
        - 最大不超过 backoff_max
        """
        delay = self.backoff_base
        for _ in range(1, consecutive_failures):
            delay *= 2
            if delay >= self.backoff_max:
                return self.backoff_max
        return delay
