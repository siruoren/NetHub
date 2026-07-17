from __future__ import annotations
"""代理延迟检测模块 - 基于 asyncio TCP/TLS 连通性测试

检测策略：
- 使用 asyncio.open_connection 建立到 address:port 的 TCP 连接
- 若协议需要 TLS，再通过 asyncio.start_tls 完成 TLS 握手
- 总延迟 = TCP 时间 + (TLS 时间 if applicable)
- 使用 asyncio.Semaphore 控制并发数
"""

import asyncio
import json
import logging
import ssl
import time
import base64
from urllib.parse import urlparse, parse_qs

from app.models import ProxyInfo

logger = logging.getLogger(__name__)


class ProxyChecker:
    """基于 asyncio 的并发代理延迟检测器"""

    def __init__(self, check_urls: list[str], timeout: float, max_concurrent: int):
        self.check_urls = check_urls
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def check_proxy(self, link: str) -> float | None:
        """检测单个代理的延迟

        解析分享链接获取 address/port/tls 信息，
        使用 TCP/TLS 连通性测试测量延迟。
        返回延迟毫秒数，失败返回 None。
        """
        info = self._parse_link_for_check(link)
        if not info:
            return None

        address = info["address"]
        port = info["port"]
        use_tls = info["use_tls"]

        if not address or not port:
            return None

        async with self.semaphore:
            return await self._tcp_latency_check(address, int(port), use_tls, self.timeout)

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

    async def _tcp_latency_check(
        self, address: str, port: int, use_tls: bool, timeout: float
    ) -> float | None:
        """TCP 层延迟检测

        1. asyncio.open_connection 测 TCP 握手时间
        2. 若需要 TLS，asyncio.start_tls 测 TLS 握手时间
        3. 返回总延迟（毫秒）
        """
        start = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(address, port), timeout=timeout
            )

            if use_tls:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                # 使用 asyncio.get_event_loop().start_tls 进行 TLS 握手
                loop = asyncio.get_event_loop()
                transport = reader.transport
                try:
                    new_transport = await asyncio.wait_for(
                        loop.start_tls(transport, transport.get_protocol(), ssl_context),
                        timeout=timeout,
                    )
                    # 清理新 transport
                    new_transport.close()
                except Exception:
                    pass

            elapsed = (time.monotonic() - start) * 1000
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return round(elapsed, 1)
        except Exception:
            return None

    def _parse_link_for_check(self, link: str) -> dict | None:
        """从分享链接中提取检测所需信息：address, port, use_tls"""
        try:
            if link.startswith("vmess://"):
                return self._parse_vmess_for_check(link)
            elif link.startswith("vless://"):
                return self._parse_vless_for_check(link)
            elif link.startswith("trojan://"):
                return self._parse_trojan_for_check(link)
            elif link.startswith("ss://"):
                return self._parse_ss_for_check(link)
            elif link.startswith("hysteria2://") or link.startswith("hy2://"):
                return self._parse_hysteria2_for_check(link)
        except Exception:
            pass
        return None

    def _parse_vmess_for_check(self, link: str) -> dict | None:
        """解析 vmess 链接获取 address/port/tls"""
        try:
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config_json = base64.b64decode(config_b64).decode("utf-8")
            config = json.loads(config_json)
            use_tls = config.get("tls", "") == "tls"
            return {
                "address": config.get("add", ""),
                "port": str(config.get("port", "")),
                "use_tls": use_tls,
            }
        except Exception:
            return None

    def _parse_vless_for_check(self, link: str) -> dict | None:
        """解析 vless 链接获取 address/port/tls"""
        try:
            parsed = urlparse(link)
            query = parse_qs(parsed.query)
            security = query.get("security", ["none"])[0]
            use_tls = security in ("tls", "reality")
            return {
                "address": parsed.hostname or "",
                "port": str(parsed.port) if parsed.port else "",
                "use_tls": use_tls,
            }
        except Exception:
            return None

    def _parse_trojan_for_check(self, link: str) -> dict | None:
        """解析 trojan 链接 - 始终使用 TLS"""
        try:
            parsed = urlparse(link)
            return {
                "address": parsed.hostname or "",
                "port": str(parsed.port) if parsed.port else "",
                "use_tls": True,
            }
        except Exception:
            return None

    def _parse_ss_for_check(self, link: str) -> dict | None:
        """解析 ss 链接 - 通常不使用 TLS"""
        try:
            # 去掉 fragment
            line = link
            if "#" in line:
                line = line[: line.rindex("#")]

            ss_content = line[5:]  # 去掉 'ss://'
            address = ""
            port = ""

            if "@" in ss_content:
                at_idx = ss_content.rindex("@")
                addr_port = ss_content[at_idx + 1:]
                if addr_port.startswith("["):
                    bracket_end = addr_port.index("]")
                    address = addr_port[1:bracket_end]
                    port = addr_port[bracket_end + 2:] if bracket_end + 2 < len(addr_port) else ""
                elif ":" in addr_port:
                    address, port = addr_port.rsplit(":", 1)
            else:
                try:
                    padding = 4 - len(ss_content) % 4
                    if padding != 4:
                        ss_content += "=" * padding
                    decoded = base64.b64decode(ss_content).decode("utf-8")
                    if "@" in decoded:
                        _, addr_port = decoded.rsplit("@", 1)
                        if ":" in addr_port:
                            address, port = addr_port.rsplit(":", 1)
                except Exception:
                    pass

            return {
                "address": address,
                "port": port,
                "use_tls": False,
            }
        except Exception:
            return None

    def _parse_hysteria2_for_check(self, link: str) -> dict | None:
        """解析 hysteria2 链接 - 始终使用 TLS（QUIC）"""
        try:
            prefix_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
            rest = link[prefix_len:]
            parsed = urlparse("http://" + rest)
            return {
                "address": parsed.hostname or "",
                "port": str(parsed.port) if parsed.port else "",
                "use_tls": True,
            }
        except Exception:
            return None
