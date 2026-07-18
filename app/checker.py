from __future__ import annotations
"""代理延迟检测模块 - 模拟通过代理服务器访问目标 URL 的真实网络延迟

检测策略：
- 通过代理服务器发起 HTTP(S) 请求到多个目标检测 URL
- 测量完整的请求延迟（DNS 解析 + TCP 连接 + TLS 握手 + HTTP 请求/响应）
- 多个目标 URL 取最大延迟值，确保所有目标均可达
- 使用 aiohttp 的 HTTP CONNECT 隧道支持 HTTPS 目标检测
- 使用 asyncio.Semaphore 控制并发数
"""

import asyncio
import json
import logging
import time
import base64
from urllib.parse import urlparse

import aiohttp

from app.models import ProxyInfo

logger = logging.getLogger(__name__)

# 默认检测目标 URL（数据库为空时使用）
DEFAULT_CHECK_URLS = [
    "https://www.google.com/generate_204",
    "https://www.gstatic.com/generate_204",
]


class ProxyChecker:
    """基于 HTTP 请求的并发代理延迟检测器

    模拟真实上网场景：通过代理访问目标网站，测量完整请求延迟。
    这比单纯 TCP/TLS 连接检测更贴近实际使用体验。
    """

    def __init__(self, check_urls: list[str], timeout: float, max_concurrent: int):
        self.check_urls = check_urls or DEFAULT_CHECK_URLS
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def check_proxy(self, link: str) -> float | None:
        """检测单个代理的延迟

        通过代理请求所有目标 URL，返回最大延迟毫秒数。
        所有目标都失败则返回 None。
        """
        proxy_url = self._link_to_proxy_url(link)
        if not proxy_url:
            return None

        async with self.semaphore:
            return await self._check_multi_targets(proxy_url)

    async def _check_multi_targets(self, proxy_url: str) -> float | None:
        """通过代理请求所有目标 URL，取最大延迟"""
        if not self.check_urls:
            return None

        max_latency = None
        for target_url in self.check_urls:
            latency = await self._http_latency_check(proxy_url, target_url)
            if latency is not None:
                if max_latency is None or latency > max_latency:
                    max_latency = latency

        return max_latency

    async def _http_latency_check(self, proxy_url: str, target_url: str) -> float | None:
        """通过代理发送 HTTP(S) 请求检测延迟

        使用 aiohttp 的 HTTP CONNECT 隧道支持 HTTPS 目标：
        - 对于 HTTPS 目标：proxy_url 格式为 http://host:port，
          aiohttp 自动通过 CONNECT 方法建立 TLS 隧道
        - 对于 HTTP 目标：直接通过代理转发请求
        """
        start = time.monotonic()
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(target_url, proxy=proxy_url) as resp:
                    await resp.read()
                    elapsed = (time.monotonic() - start) * 1000
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

    # ---- 协议链接转代理 URL ----

    def _link_to_proxy_url(self, link: str) -> str | None:
        """将分享链接转换为 aiohttp 可用的代理 URL 格式

        aiohttp 代理格式: http://host:port
        无论是 HTTP 还是 HTTPS 协议的代理，代理 URL 统一使用 http://
        aiohttp 会自动通过 CONNECT 方法为 HTTPS 目标建立隧道
        """
        try:
            if link.startswith("vmess://"):
                return self._vmess_to_proxy_url(link)
            elif link.startswith("vless://"):
                return self._vless_to_proxy_url(link)
            elif link.startswith("trojan://"):
                return self._trojan_to_proxy_url(link)
            elif link.startswith("ss://"):
                return self._ss_to_proxy_url(link)
            elif link.startswith("hysteria2://") or link.startswith("hy2://"):
                return self._hysteria2_to_proxy_url(link)
        except Exception:
            pass
        return None

    def _vmess_to_proxy_url(self, link: str) -> str | None:
        try:
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config_json = base64.b64decode(config_b64).decode("utf-8")
            config = json.loads(config_json)
            address = config.get("add", "")
            port = str(config.get("port", ""))
            if not address or not port:
                return None
            return f"http://{address}:{port}"
        except Exception:
            return None

    def _vless_to_proxy_url(self, link: str) -> str | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = str(parsed.port) if parsed.port else ""
            if not address or not port:
                return None
            return f"http://{address}:{port}"
        except Exception:
            return None

    def _trojan_to_proxy_url(self, link: str) -> str | None:
        try:
            parsed = urlparse(link)
            address = parsed.hostname or ""
            port = str(parsed.port) if parsed.port else ""
            if not address or not port:
                return None
            return f"http://{address}:{port}"
        except Exception:
            return None

    def _ss_to_proxy_url(self, link: str) -> str | None:
        try:
            line = link
            if "#" in line:
                line = line[:line.rindex("#")]

            ss_content = line[5:]
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

            if not address or not port:
                return None
            return f"http://{address}:{port}"
        except Exception:
            return None

    def _hysteria2_to_proxy_url(self, link: str) -> str | None:
        try:
            prefix_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
            rest = link[prefix_len:]
            parsed = urlparse("http://" + rest)
            address = parsed.hostname or ""
            port = str(parsed.port) if parsed.port else ""
            if not address or not port:
                return None
            return f"http://{address}:{port}"
        except Exception:
            return None
