from __future__ import annotations
"""订阅解析模块 - 拉取和解析节点订阅链接

核心解析逻辑移植自 Proxy_List/get_connected_proxies/get_connected_proxies.py
支持 vmess / vless / trojan / ss / hysteria2 / socks5 / socks4 / http / https 协议
"""

import base64
import difflib
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

    支持 vmess / vless / trojan / ss / hysteria2 / socks5 / socks4 / http / https 协议
    自动检测 base64 编码的订阅内容并解码
    """
    share_links: list[ProxyInfo] = []
    lines = content.strip().split("\n")

    # 支持的协议前缀
    supported_prefixes = (
        "vmess://", "vless://", "trojan://", "ss://",
        "hysteria2://", "hy2://",
        "socks5://", "socks4://", "socks4a://",
        "http://", "https://",
    )

    # 检测是否为 base64 编码的订阅内容
    has_protocol_prefix = any(
        line.strip().startswith(supported_prefixes)
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
        elif line.startswith(("socks5://", "socks4://", "socks4a://")):
            info = _parse_socks(line)
        elif line.startswith(("http://", "https://")) and "#" in line:
            # 仅当 http/https 链接带 #fragment 时视为代理节点（避免误判普通 URL）
            info = _parse_http_proxy(line)
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


def _parse_socks(line: str) -> ProxyInfo | None:
    """解析 socks5:// 链接（socks4/socks4a 不再支持，直接跳过）

    格式: socks5://[user:pass@]host:port[#name]
    """
    try:
        # socks4/socks4a 协议不再支持，跳过
        if line.lower().startswith(("socks4://", "socks4a://")):
            return None
        parsed = urlparse(line)
        address = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else ""
        if not address or not port:
            return None
        name = unquote(parsed.fragment) if parsed.fragment else f"{address}:{port}"
        return ProxyInfo(
            protocol="socks5",
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


def _parse_http_proxy(line: str) -> ProxyInfo | None:
    """解析 http:// / https:// 代理链接

    格式: http://[user:pass@]host:port[#name]
    仅当带 #fragment 时视为代理节点（避免误判普通 URL）
    """
    try:
        parsed = urlparse(line)
        address = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else ""
        if not address or not port:
            return None
        name = unquote(parsed.fragment) if parsed.fragment else f"{address}:{port}"
        protocol = "https" if line.lower().startswith("https://") else "http"
        return ProxyInfo(
            protocol=protocol,
            name=name,
            address=address,
            port=port,
            link=line,
        )
    except Exception:
        return None


# ---- 服务实例 API 获取 ----

async def instance_login(base_url: str, username: str, password: str,
                         timeout: float = 10.0) -> tuple[aiohttp.ClientSession, dict]:
    """登录服务实例，返回 (session, headers)"""
    session = aiohttp.ClientSession()
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    try:
        async with session.post(
            f"{base_url}/api/login",
            json={"username": username, "password": password},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            data = await resp.json()
            if data.get("code") != "SUCCESS":
                await session.close()
                raise Exception(f"登录失败: {data}")
            token = data["data"]["token"]
            headers["Authorization"] = token
            return session, headers
    except Exception:
        await session.close()
        raise


async def get_instance_connected_nodes(
    session: aiohttp.ClientSession, headers: dict, base_url: str,
    timeout: float = 10.0,
) -> tuple[list[dict], list[dict]]:
    """获取服务实例已连接节点和订阅列表

    返回 (connected_nodes, subscriptions)
    connected_nodes: [{"id", "sub_index", "name", "address", "net", "outbound"}, ...]
    subscriptions: 原始订阅列表
    """
    async with session.get(
        f"{base_url}/api/touch", headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        data = await resp.json()

    connected = data["data"]["touch"]["connectedServer"]
    subscriptions = data["data"]["touch"]["subscriptions"]

    connected_nodes = []
    for conn in connected:
        node_id = conn["id"]
        sub_index = conn["sub"]
        outbound = conn.get("outbound", "proxy")

        if sub_index < 0:
            logger.debug("跳过手动添加的服务器 id=%d（无订阅源）", node_id)
            continue

        if sub_index < len(subscriptions):
            sub = subscriptions[sub_index]
            for server in sub.get("servers", []):
                if server["id"] == node_id:
                    connected_nodes.append({
                        "id": node_id,
                        "sub_index": sub_index,
                        "name": server.get("name"),
                        "address": server.get("address"),
                        "net": server.get("net"),
                        "outbound": outbound,
                    })
                    break
    return connected_nodes, subscriptions


def _normalize_string(s: str) -> str:
    """标准化字符串用于模糊匹配"""
    if not s:
        return ""
    return s.replace(" ", "").replace("-", "").replace("_", "").lower()


def _strings_similar(s1: str, s2: str, threshold: float = 0.5) -> bool:
    """判断两个字符串是否相似"""
    if not s1 or not s2:
        return False
    s1_norm = _normalize_string(s1)
    s2_norm = _normalize_string(s2)
    if not s1_norm or not s2_norm:
        return False
    if s1_norm in s2_norm or s2_norm in s1_norm:
        return True
    ratio = difflib.SequenceMatcher(None, s1_norm, s2_norm).ratio()
    return ratio >= threshold


async def fetch_connected_proxies(
    base_url: str, username: str, password: str,
) -> tuple[list[ProxyInfo], list[str]]:
    """从服务实例获取所有已连接节点的分享链接和订阅地址列表

    返回: (matched_proxies, subscription_urls)
    - matched_proxies: 匹配成功的 ProxyInfo 列表
    - subscription_urls: 服务实例中所有订阅源的地址 URL 列表

    流程：
    1. 登录服务实例
    2. 获取已连接节点列表和订阅列表
    3. 提取所有订阅源地址 URL
    4. 遍历每个订阅源，拉取并解析节点列表
    5. 将已连接节点与订阅中的节点进行匹配（名称+地址模糊匹配）
    """
    session, headers = await instance_login(base_url, username, password)
    try:
        connected_nodes, subscriptions = await get_instance_connected_nodes(
            session, headers, base_url,
        )
        logger.info("服务实例 %s: 已连接 %d 个节点, 共 %d 个订阅源",
                     base_url, len(connected_nodes), len(subscriptions))

        # 提取所有订阅源地址 URL
        subscription_urls = [
            sub.get("address", "")
            for sub in subscriptions
            if sub.get("address")
        ]

        matched_proxies: list[ProxyInfo] = []
        matched_node_ids: set[int] = set()

        for sub_index, sub in enumerate(subscriptions):
            sub_address = sub.get("address")
            if not sub_address:
                continue

            # 拉取订阅内容
            try:
                async with session.get(
                    sub_address, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15.0),
                ) as sub_resp:
                    content = await sub_resp.text()
                share_links = parse_subscription(content)
                logger.info("订阅源 %d: 解析到 %d 个节点", sub_index, len(share_links))
            except Exception as e:
                logger.warning("订阅源 %d 加载失败: %s", sub_index, e)
                continue

            # 该订阅源下的已连接节点
            sub_connected = [
                n for n in connected_nodes
                if n["sub_index"] == sub_index and n["id"] not in matched_node_ids
            ]

            # 第一次匹配：名称+地址模糊匹配
            unmatched = []
            for conn_node in sub_connected:
                if conn_node["id"] in matched_node_ids:
                    continue
                best_match = None
                best_quality = 0
                for link_info in share_links:
                    quality = 0
                    name_sim = _strings_similar(conn_node["name"], link_info.name)
                    addr_sim = _strings_similar(conn_node["address"], link_info.address)
                    if name_sim and addr_sim:
                        quality = 3
                    elif name_sim:
                        quality = 2
                    elif addr_sim:
                        quality = 1
                    if quality > best_quality:
                        best_quality = quality
                        best_match = link_info

                if best_match and best_quality >= 1:
                    matched_node_ids.add(conn_node["id"])
                    matched_proxies.append(best_match)
                else:
                    unmatched.append(conn_node)

            # 第二次匹配：地址精确匹配 或 地址模糊+端口匹配
            for conn_node in unmatched:
                if conn_node["id"] in matched_node_ids:
                    continue
                best_match = None
                best_quality = 0
                for link_info in share_links:
                    quality = 0
                    addr_exact = (conn_node["address"] and link_info.address
                                  and conn_node["address"] == link_info.address)
                    addr_fuzzy = _strings_similar(conn_node["address"], link_info.address, 0.7)
                    port_match = (conn_node.get("net") and link_info.port
                                  and str(conn_node["net"]) == str(link_info.port))
                    if addr_exact:
                        quality = 4
                    elif addr_fuzzy and port_match:
                        quality = 3
                    elif addr_fuzzy:
                        quality = 2
                    elif port_match and _strings_similar(conn_node["name"], link_info.name, 0.3):
                        quality = 1
                    if quality > best_quality:
                        best_quality = quality
                        best_match = link_info

                if best_match and best_quality >= 1:
                    matched_node_ids.add(conn_node["id"])
                    matched_proxies.append(best_match)

        logger.info("服务实例 %s: 共匹配到 %d 个已连接节点配置, 发现 %d 个订阅源",
                     base_url, len(matched_proxies), len(subscription_urls))
        return matched_proxies, subscription_urls
    finally:
        await session.close()
