from __future__ import annotations
"""代理延迟检测模块 - 通过 TCP/TLS 连接检测代理服务器响应速度

检测策略：
- 对代理服务器发起 TCP 连接，测量连接建立耗时
- 若协议使用 TLS（vmess+tls / vless+tls / trojan / hysteria2），
  则在 TCP 连接基础上完成 TLS 握手，测量总耗时
- TCP+TLS 延迟与实际上网体验高度相关：它反映到代理服务器的网络往返时间
  和服务端处理能力，是真实代理请求延迟的主要组成部分
- 使用 asyncio.Semaphore 控制并发数
- 检测目标 URL 存入数据库供页面展示和未来扩展
"""

import asyncio
import json
import logging
import ssl
import time
import base64
from dataclasses import dataclass
from urllib.parse import urlparse, unquote

from app.models import ProxyInfo

logger = logging.getLogger(__name__)

# 默认检测目标 URL（数据库为空时使用，页面展示用）
DEFAULT_CHECK_URLS = [
    "https://www.google.com/generate_204",
    "https://www.gstatic.com/generate_204",
]


@dataclass
class ProxyConnInfo:
    """代理服务器连接信息"""
    host: str
    port: int
    use_tls: bool
    protocol: str


class ProxyChecker:
    """基于 asyncio 的并发代理延迟检测器

    通过 TCP/TLS 连接测试代理服务器响应速度。
    延迟值 = TCP 连接耗时 (+ TLS 握手耗时)，单位毫秒。
    该值与实际通过代理浏览网页的延迟高度正相关。
    """

    def __init__(self, check_urls: list[str], timeout: float, max_concurrent: int):
        self.check_urls = check_urls or DEFAULT_CHECK_URLS
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def check_proxy(self, link: str) -> float | None:
        """检测单个代理的延迟

        通过 TCP/TLS 连接测试代理服务器响应速度。
        返回连接建立耗时（毫秒），失败返回 None。
        """
        conn_info = self._parse_link(link)
        if not conn_info:
            return None

        async with self.semaphore:
            return await self._check_connectivity(conn_info)

    async def _check_connectivity(self, info: ProxyConnInfo) -> float | None:
        """测试代理服务器 TCP/TLS 连通性并测量延迟"""
        start = time.monotonic()
        try:
            if info.use_tls:
                # TLS 协议：TCP 连接 + TLS 握手，更贴近真实代理请求体验
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        info.host, info.port, ssl=ctx, server_hostname=info.host
                    ),
                    timeout=self.timeout,
                )
            else:
                # 非 TLS 协议：仅 TCP 连接
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(info.host, info.port),
                    timeout=self.timeout,
                )

            elapsed = (time.monotonic() - start) * 1000
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return round(elapsed, 1)
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
            # vmess 的 tls 字段为 "tls" 或 "reality" 时使用 TLS
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
            # trojan 默认使用 TLS
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
            # 构造标准 URL 以便 urlparse 解析
            parsed = urlparse("http://" + rest)
            address = parsed.hostname or ""
            port = parsed.port or 0
            if not address or not port:
                return None
            # hysteria2 默认使用 TLS
            return ProxyConnInfo(
                host=address, port=int(port),
                use_tls=True, protocol="hysteria2",
            )
        except Exception:
            return None
