from __future__ import annotations
"""订阅解析模块 - 拉取和解析代理订阅链接

核心解析逻辑移植自 Proxy_List/get_connected_proxies/get_connected_proxies.py
支持 vmess / vless / trojan / ss / hysteria2 五种协议
"""

import base64
import json
import logging
from urllib.parse import urlparse, unquote

import aiohttp

from app.models import ProxyInfo

logger = logging.getLogger(__name__)


async def fetch_subscription(url: str, timeout: float = 15.0) -> str:
    """异步拉取订阅 URL 内容"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            resp.raise_for_status()
            return await resp.text()


def parse_subscription(content: str) -> list[ProxyInfo]:
    """解析订阅内容，返回 ProxyInfo 列表

    支持 vmess / vless / trojan / ss / hysteria2 协议
    自动检测 base64 编码的订阅内容并解码
    """
    share_links: list[ProxyInfo] = []
    lines = content.strip().split("\n")

    # 检测是否为 base64 编码的订阅内容
    has_protocol_prefix = any(
        line.strip().startswith((
            "vmess://", "vless://", "trojan://", "ss://",
            "hysteria2://", "hy2://",
        ))
        for line in lines if line.strip()
    )

    if not has_protocol_prefix:
        try:
            decoded = base64.b64decode(content.strip() + "===").decode("utf-8")
            lines = decoded.strip().split("\n")
            logger.info("检测到 base64 编码内容，已解码")
        except Exception:
            lines = content.strip().split("\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("vmess://"):
            info = _parse_vmess(line)
        elif line.startswith("vless://"):
            info = _parse_vless(line)
        elif line.startswith("trojan://"):
            info = _parse_trojan(line)
        elif line.startswith("ss://"):
            info = _parse_ss(line)
        elif line.startswith("hysteria2://") or line.startswith("hy2://"):
            info = _parse_hysteria2(line)
        else:
            info = None

        if info:
            share_links.append(info)

    return share_links


def _parse_vmess(line: str) -> ProxyInfo | None:
    """解析 vmess:// 链接"""
    try:
        config_b64 = line[8:]
        padding = 4 - len(config_b64) % 4
        if padding != 4:
            config_b64 += "=" * padding
        config_json = base64.b64decode(config_b64).decode("utf-8")
        config = json.loads(config_json)
        return ProxyInfo(
            protocol="vmess",
            name=config.get("ps", ""),
            address=config.get("add", ""),
            port=str(config.get("port", "")),
            link=line,
        )
    except Exception:
        return None


def _parse_vless(line: str) -> ProxyInfo | None:
    """解析 vless:// 链接"""
    try:
        parsed = urlparse(line)
        name = unquote(parsed.fragment) if parsed.fragment else ""
        address = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else ""
        return ProxyInfo(
            protocol="vless",
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


def _parse_trojan(line: str) -> ProxyInfo | None:
    """解析 trojan:// 链接"""
    try:
        parsed = urlparse(line)
        name = unquote(parsed.fragment) if parsed.fragment else ""
        address = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else ""
        return ProxyInfo(
            protocol="trojan",
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


def _parse_ss(line: str) -> ProxyInfo | None:
    """解析 ss:// 链接（SIP002 和传统格式）"""
    try:
        name = ""
        line_for_parse = line
        if "#" in line:
            frag_start = line.rindex("#")
            name = unquote(line[frag_start + 1:])
            line_for_parse = line[:frag_start]

        ss_content = line_for_parse[5:]  # 去掉 'ss://'
        address = ""
        port = ""

        if "@" in ss_content:
            # SIP002 格式: ss://base64(method:password)@address:port
            at_idx = ss_content.rindex("@")
            addr_port = ss_content[at_idx + 1:]
            if addr_port.startswith("["):
                bracket_end = addr_port.index("]")
                address = addr_port[1:bracket_end]
                port = addr_port[bracket_end + 2:] if bracket_end + 2 < len(addr_port) else ""
            elif ":" in addr_port:
                address, port = addr_port.rsplit(":", 1)
            else:
                address = addr_port
        else:
            # 传统格式: ss://base64(method:password@address:port)
            try:
                padding = 4 - len(ss_content) % 4
                if padding != 4:
                    ss_content_padded = ss_content + "=" * padding
                else:
                    ss_content_padded = ss_content
                decoded = base64.b64decode(ss_content_padded).decode("utf-8")
                if "@" in decoded:
                    _, addr_port = decoded.rsplit("@", 1)
                    if addr_port.startswith("["):
                        bracket_end = addr_port.index("]")
                        address = addr_port[1:bracket_end]
                        port = addr_port[bracket_end + 2:] if bracket_end + 2 < len(addr_port) else ""
                    elif ":" in addr_port:
                        address, port = addr_port.rsplit(":", 1)
                    else:
                        address = addr_port
            except Exception:
                pass

        return ProxyInfo(
            protocol="ss",
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


def _parse_hysteria2(line: str) -> ProxyInfo | None:
    """解析 hysteria2:// 或 hy2:// 链接"""
    try:
        prefix_len = len("hysteria2://") if line.startswith("hysteria2://") else len("hy2://")
        rest = line[prefix_len:]
        # 构造标准 URL 以便 urlparse 解析
        parsed = urlparse("http://" + rest)
        name = ""
        if "#" in rest:
            name = unquote(rest.split("#")[-1])
        address = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else ""
        return ProxyInfo(
            protocol="hysteria2",
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


async def load_subscription_urls(file_path: str) -> list[str]:
    """从 Subscription.txt 加载订阅 URL 列表

    支持两种格式：
    - 纯 URL 格式（每行一个 URL）
    - 行号\tURL 格式（忽略行号前缀）
    """
    urls = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # 支持 "行号\tURL" 格式
                if "\t" in line:
                    parts = line.split("\t", 1)
                    url = parts[-1].strip()
                else:
                    url = line
                if url.startswith("http://") or url.startswith("https://"):
                    urls.append(url)
    except FileNotFoundError:
        logger.warning("订阅文件不存在: %s", file_path)
    return urls


async def load_check_urls(file_path: str) -> list[str]:
    """从 domain_check.txt 加载检测 URL 列表"""
    urls = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http://") or line.startswith("https://"):
                    urls.append(line)
    except FileNotFoundError:
        logger.warning("检测 URL 文件不存在: %s，使用默认值", file_path)
        urls = ["http://www.google.com/generate_204"]
    return urls
