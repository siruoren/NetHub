from __future__ import annotations
"""订阅解析模块 - 拉取和解析节点订阅链接

核心解析逻辑移植自 Proxy_List/get_connected_proxies/get_connected_proxies.py
支持 vmess / vless / trojan / ss / hysteria2 / socks5 / http / https 协议
支持 Clash YAML 格式订阅解析
"""

import asyncio
import base64
import difflib
import json
import logging
from urllib.parse import urlparse, unquote, parse_qs

import aiohttp
import yaml

from app.models import ProxyInfo

logger = logging.getLogger(__name__)


async def fetch_subscription(url: str, timeout: float = 60.0) -> str:
    """异步拉取订阅 URL 内容"""
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.text()
    except (asyncio.TimeoutError, TimeoutError) as e:
        logger.warning("拉取订阅超时 (%ss): %s", timeout, url[:80])
        raise
    except aiohttp.ClientError as e:
        logger.warning("拉取订阅失败: %s - %s", url[:80], e)
        raise


def parse_subscription(content: str) -> list[ProxyInfo]:
    """解析订阅内容，返回 ProxyInfo 列表

    支持格式：
    - 纯文本分享链接（vmess/vless/trojan/ss/hysteria2/socks5/http）
    - base64 编码的分享链接
    - Clash YAML 格式（proxies 列表）
    """
    stripped = content.strip()

    # 检测 Clash YAML 格式（含 proxies: 字段）
    if _is_clash_yaml(stripped):
        clash_proxies = _parse_clash_yaml(stripped)
        if clash_proxies:
            logger.info("检测到 Clash YAML 格式，解析到 %d 个节点", len(clash_proxies))
            return filter_invalid_proxies(clash_proxies)

    share_links: list[ProxyInfo] = []
    lines = stripped.split("\n")

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
            decoded = base64.b64decode(stripped + "===").decode("utf-8")
            lines = decoded.strip().split("\n")
            logger.info("检测到 base64 编码内容，已解码")
        except Exception:
            lines = stripped.split("\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 跳过注释行和常见非协议行
        if line.startswith("#") or line.startswith("//"):
            continue

        info = _try_parse_line(line)

        if info:
            share_links.append(info)
        else:
            logger.debug("忽略无法解析的行: %s", line[:80])

    return filter_invalid_proxies(share_links)


def filter_invalid_proxies(proxies) -> list:
    """过滤无效协议节点：vmess 地址/UUID为空、vless 含 raw/xhttp/reality 传输

    支持 ProxyInfo 和 ProxyDBRecord 对象（均含 protocol, address, link 字段）
    """
    _VLESS_SKIP_TYPES = ("type=raw", "type=xhttp", "security=reality")
    filtered = []
    vmess_empty = 0
    vless_skipped = 0
    for p in proxies:
        # vmess 地址为空或 UUID 为空（如 vmess() 等无效配置）
        if p.protocol == "vmess":
            if not p.address:
                vmess_empty += 1
                logger.debug("过滤无效 vmess 节点（地址为空）: %s", p.name[:50] if p.name else "")
                continue
            if not _vmess_has_uuid(p.link):
                vmess_empty += 1
                logger.debug("过滤无效 vmess 节点（UUID为空）: %s", p.name[:50] if p.name else "")
                continue
        # vless 协议包含 raw/xhttp/reality 传输
        if p.protocol == "vless" and any(t in p.link for t in _VLESS_SKIP_TYPES):
            vless_skipped += 1
            logger.debug("过滤 vless raw/xhttp/reality 节点: %s", p.name[:50] if p.name else "")
            continue
        filtered.append(p)
    removed = len(proxies) - len(filtered)
    if removed > 0:
        logger.info("过滤无效协议节点 %d 个（vmess 无效 %d, vless raw/xhttp/reality %d）",
                    removed, vmess_empty, vless_skipped)
    return filtered


def _vmess_has_uuid(link: str) -> bool:
    """检查 vmess 链接的 UUID 是否存在"""
    try:
        config_b64 = link[8:]
        if not config_b64.strip():
            return False
        padding = 4 - len(config_b64) % 4
        if padding != 4:
            config_b64 += "=" * padding
        config = json.loads(base64.b64decode(config_b64).decode("utf-8"))
        uuid_val = config.get("id", "").strip()
        return bool(uuid_val)
    except Exception:
        return False


def get_transport_type(link: str, protocol: str) -> str:
    """从节点链接中提取传输类型，用于显示（如 vmess(ws)）

    返回传输类型字符串，无传输类型时返回空字符串
    """
    try:
        if protocol == "vmess":
            config_b64 = link[8:]
            padding = 4 - len(config_b64) % 4
            if padding != 4:
                config_b64 += "=" * padding
            config = json.loads(base64.b64decode(config_b64).decode("utf-8"))
            net = config.get("net", "tcp")
            return net if net and net != "tcp" else ""
        elif protocol == "vless":
            parsed = urlparse(link)
            query = parse_qs(parsed.query)
            net = query.get("type", ["tcp"])[0]
            security = query.get("security", ["none"])[0]
            parts = []
            if net and net != "tcp":
                parts.append(net)
            if security and security != "none":
                parts.append(security)
            return "+".join(parts) if parts else ""
        elif protocol == "trojan":
            parsed = urlparse(link)
            query = parse_qs(parsed.query)
            net = query.get("type", ["tcp"])[0]
            return net if net and net != "tcp" else ""
        elif protocol in ("hysteria2", "hy2"):
            return "hysteria2"
    except Exception:
        pass
    return ""


def _try_parse_line(line: str) -> ProxyInfo | None:
    """尝试解析单行，失败后去除行首特殊字符重试"""
    info = _parse_line(line)
    if info is not None:
        return info

    # 去除行首特殊字符（非字母数字的不可见/控制字符），然后重试
    import re
    cleaned = re.sub(r'^[^a-zA-Z0-9]+', '', line)
    if cleaned != line and cleaned:
        info = _parse_line(cleaned)
        if info is not None:
            return info

    return None


def _parse_line(line: str) -> ProxyInfo | None:
    """解析单行分享链接"""
    if line.startswith("vmess://"):
        return _parse_vmess(line)
    elif line.startswith("vless://"):
        return _parse_vless(line)
    elif line.startswith("trojan://"):
        return _parse_trojan(line)
    elif line.startswith("ss://"):
        return _parse_ss(line)
    elif line.startswith("hysteria2://") or line.startswith("hy2://"):
        return _parse_hysteria2(line)
    elif line.startswith(("socks5://", "socks4://", "socks4a://")):
        return _parse_socks(line)
    elif line.startswith(("http://", "https://")) and "#" in line:
        return _parse_http_proxy(line)
    return None


def _is_clash_yaml(content: str) -> bool:
    """检测内容是否为 Clash YAML 格式"""
    # 快速检测：包含 proxies: 关键字
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("proxies:"):
            return True
        # 也匹配 "proxies :" 或缩进格式
        if stripped.replace(" ", "").startswith("proxies:"):
            return True
    return False


def _parse_clash_yaml(content: str) -> list[ProxyInfo]:
    """解析 Clash YAML 格式订阅，将每个代理转换为分享链接

    支持 type: vmess / vless / trojan / ss / hysteria2 / socks5 / http
    """
    try:
        data = yaml.safe_load(content)
    except Exception:
        return []

    if not isinstance(data, dict) or "proxies" not in data:
        return []

    proxies_data = data["proxies"]
    if not isinstance(proxies_data, list):
        return []

    results: list[ProxyInfo] = []
    for proxy in proxies_data:
        if not isinstance(proxy, dict):
            continue
        info = _clash_proxy_to_info(proxy)
        if info:
            results.append(info)

    return results


def _clash_proxy_to_info(proxy: dict) -> ProxyInfo | None:
    """将单个 Clash 代理配置转为 ProxyInfo + 分享链接"""
    ptype = proxy.get("type", "").lower()
    name = proxy.get("name", "")
    server = proxy.get("server", "")
    port = str(proxy.get("port", ""))

    if not server or not port:
        return None

    if ptype == "vmess":
        link = _clash_to_vmess_link(proxy, name, server, port)
    elif ptype == "vless":
        link = _clash_to_vless_link(proxy, name, server, port)
    elif ptype == "trojan":
        link = _clash_to_trojan_link(proxy, name, server, port)
    elif ptype in ("ss", "shadowsocks"):
        link = _clash_to_ss_link(proxy, name, server, port)
    elif ptype in ("hysteria2", "hy2"):
        link = _clash_to_hysteria2_link(proxy, name, server, port)
    elif ptype == "socks5":
        link = _clash_to_socks5_link(proxy, name, server, port)
    elif ptype in ("http", "https"):
        link = _clash_to_http_link(proxy, name, server, port, ptype)
    else:
        return None

    if not link:
        return None

    protocol = ptype if ptype != "shadowsocks" else "ss"
    return ProxyInfo(protocol=protocol, name=name, address=server, port=port, link=link)


def _clash_to_vmess_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash vmess → vmess:// 链接"""
    try:
        import uuid
        config = {
            "v": "2",
            "ps": name,
            "add": server,
            "port": int(port),
            "id": proxy.get("uuid", ""),
            "aid": proxy.get("alterId", 0),
            "net": proxy.get("network", "ws"),
            "type": "none",
            "host": "",
            "path": "",
            "tls": "tls" if proxy.get("tls") else "",
            "sni": proxy.get("servername", "") or proxy.get("sni", ""),
        }
        # 传输层
        network = config["net"]
        ws_opts = proxy.get("ws-opts", {})
        h2_opts = proxy.get("h2-opts", {})
        grpc_opts = proxy.get("grpc-opts", {})

        if network == "ws" and ws_opts:
            config["path"] = ws_opts.get("path", "/")
            if ws_opts.get("headers", {}).get("host"):
                config["host"] = ws_opts["headers"]["host"]
        elif network == "h2" and h2_opts:
            config["path"] = h2_opts.get("path", "/")
            hosts = h2_opts.get("host", [])
            if hosts:
                config["host"] = hosts[0]
        elif network == "grpc" and grpc_opts:
            config["path"] = grpc_opts.get("grpc-service-name", "")

        b64 = base64.b64encode(json.dumps(config, separators=(",", ":")).encode()).decode().rstrip("=")
        return f"vmess://{b64}"
    except Exception:
        return None


def _clash_to_vless_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash vless → vless:// 链接"""
    try:
        uuid = proxy.get("uuid", "")
        params = []
        # 传输层
        network = proxy.get("network", "ws")
        params.append(f"type={network}")
        # security
        if proxy.get("tls"):
            params.append("security=tls")
            sni = proxy.get("servername", "") or proxy.get("sni", "")
            if sni:
                params.append(f"sni={sni}")
            if proxy.get("alpn"):
                params.append(f"alpn={','.join(proxy['alpn'])}")
            fp = proxy.get("client-fingerprint", "")
            if fp:
                params.append(f"fp={fp}")
        # flow
        flow = proxy.get("flow", "")
        if flow:
            params.append(f"flow={flow}")
        # ws-opts
        ws_opts = proxy.get("ws-opts", {})
        if network == "ws" and ws_opts:
            if ws_opts.get("path"):
                params.append(f"path={ws_opts['path']}")
            if ws_opts.get("headers", {}).get("host"):
                params.append(f"host={ws_opts['headers']['host']}")
        # h2-opts
        h2_opts = proxy.get("h2-opts", {})
        if network == "h2" and h2_opts:
            if h2_opts.get("path"):
                params.append(f"path={h2_opts['path']}")
            hosts = h2_opts.get("host", [])
            if hosts:
                params.append(f"host={hosts[0]}")
        # grpc-opts
        grpc_opts = proxy.get("grpc-opts", {})
        if network == "grpc" and grpc_opts:
            if grpc_opts.get("grpc-service-name"):
                params.append(f"serviceName={grpc_opts['grpc-service-name']}")

        fragment = _quote(name)
        query = "&".join(params)
        return f"vless://{uuid}@{server}:{port}?{query}#{fragment}"
    except Exception:
        return None


def _clash_to_trojan_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash trojan → trojan:// 链接"""
    try:
        password = proxy.get("password", "")
        params = []
        sni = proxy.get("sni", "") or proxy.get("servername", "")
        if sni:
            params.append(f"sni={sni}")
        network = proxy.get("network", "ws")
        if network != "ws":
            params.append(f"type={network}")
        if proxy.get("alpn"):
            params.append(f"alpn={','.join(proxy['alpn'])}")
        # ws-opts
        ws_opts = proxy.get("ws-opts", {})
        if network == "ws" and ws_opts:
            if ws_opts.get("path"):
                params.append(f"path={ws_opts['path']}")
            if ws_opts.get("headers", {}).get("host"):
                params.append(f"host={ws_opts['headers']['host']}")
        # grpc-opts
        grpc_opts = proxy.get("grpc-opts", {})
        if network == "grpc" and grpc_opts:
            if grpc_opts.get("grpc-service-name"):
                params.append(f"serviceName={grpc_opts['grpc-service-name']}")

        fragment = _quote(name)
        query = "&".join(params)
        return f"trojan://{password}@{server}:{port}?{query}#{fragment}"
    except Exception:
        return None


def _clash_to_ss_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash ss → ss:// 链接"""
    try:
        cipher = proxy.get("cipher", "")
        password = proxy.get("password", "")
        userinfo = f"{cipher}:{password}"
        userinfo_b64 = base64.b64encode(userinfo.encode()).decode().rstrip("=")
        fragment = _quote(name)
        return f"ss://{userinfo_b64}@{server}:{port}#{fragment}"
    except Exception:
        return None


def _clash_to_hysteria2_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash hysteria2 → hysteria2:// 链接"""
    try:
        password = proxy.get("password", "") or proxy.get("auth", "")
        params = []
        sni = proxy.get("sni", "")
        if sni:
            params.append(f"sni={sni}")
        if proxy.get("alpn"):
            params.append(f"alpn={','.join(proxy['alpn'])}")
        insecure = proxy.get("skip-cert-verify", False)
        if insecure:
            params.append("insecure=1")

        fragment = _quote(name)
        query = "&".join(params)
        return f"hysteria2://{password}@{server}:{port}?{query}#{fragment}"
    except Exception:
        return None


def _clash_to_socks5_link(proxy: dict, name: str, server: str, port: str) -> str | None:
    """Clash socks5 → socks5:// 链接"""
    try:
        auth = ""
        username = proxy.get("username", "")
        password = proxy.get("password", "")
        if username:
            auth = f"{_quote(username)}:{_quote(password)}@"
        fragment = _quote(name)
        return f"socks5://{auth}{server}:{port}#{fragment}"
    except Exception:
        return None


def _clash_to_http_link(proxy: dict, name: str, server: str, port: str, ptype: str) -> str | None:
    """Clash http(s) → http(s):// 链接"""
    try:
        auth = ""
        username = proxy.get("username", "")
        password = proxy.get("password", "")
        if username:
            auth = f"{_quote(username)}:{_quote(password)}@"
        fragment = _quote(name)
        protocol = "https" if proxy.get("tls") or ptype == "https" else "http"
        return f"{protocol}://{auth}{server}:{port}#{fragment}"
    except Exception:
        return None


def _quote(s: str) -> str:
    """URL 编码（用于 #fragment 和参数值）"""
    from urllib.parse import quote
    return quote(s, safe="")


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
) -> tuple[list[tuple[ProxyInfo, str, str]], list[str], int]:
    """从服务实例获取所有已连接节点的分享链接和订阅地址列表

    返回: (matched_proxies_with_identity, subscription_urls, connected_node_count)
    - matched_proxies_with_identity: [(ProxyInfo, instance_node_name, instance_node_address), ...]
      每个匹配成功的节点附带实例 API 中的节点名称和地址，用于精准标识
    - subscription_urls: 服务实例中所有订阅源的地址 URL 列表
    - connected_node_count: 服务实例 API 返回的实际已连接节点数

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
        connected_node_count = len(connected_nodes)
        logger.info("服务实例 %s: 已连接 %d 个节点, 共 %d 个订阅源",
                     base_url, connected_node_count, len(subscriptions))

        # 提取所有订阅源地址 URL
        subscription_urls = [
            sub.get("address", "")
            for sub in subscriptions
            if sub.get("address")
        ]

        matched_proxies: list[tuple[ProxyInfo, str, str]] = []  # [(ProxyInfo, conn_node_name, conn_node_address), ...]
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
                    matched_proxies.append((best_match, conn_node["name"], conn_node["address"]))
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
                    matched_proxies.append((best_match, conn_node["name"], conn_node["address"]))

        logger.info("服务实例 %s: 共匹配到 %d 个已连接节点配置(实际已连接 %d), 发现 %d 个订阅源",
                     base_url, len(matched_proxies), connected_node_count, len(subscription_urls))
        return matched_proxies, subscription_urls, connected_node_count
    finally:
        await session.close()
