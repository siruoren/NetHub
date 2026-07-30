from __future__ import annotations
"""订阅链接生成模块 - 生成纯文本/Clash 格式订阅内容"""

import base64
import json
import logging
from urllib.parse import urlparse, unquote, parse_qs

import yaml

from app.models import ProxyDBRecord

logger = logging.getLogger(__name__)


def generate_plain_subscription(proxies: list[ProxyDBRecord]) -> str:
    """生成纯文本格式订阅内容（参照 subdom.txt 格式）

    每行一条原始代理 URI，socks/http 代理确保格式为 protocol://host:port#host-port
    如果条目中包含空格，自动移除空格及空格后续的内容
    """
    links = []
    for p in proxies:
        link = _normalize_link(p)
        # 移除空格及空格后续的内容（内核不支持含空格的链接）
        if " " in link:
            link = link[:link.index(" ")]
        links.append(link)
    return "\n".join(links)

def _normalize_link(proxy: ProxyDBRecord) -> str:
    """规范化分享链接，确保 socks/http 代理带有 #host-port 名称"""
    link = proxy.link
    if link.startswith(("socks5://", "socks4://", "socks4a://")):
        # socks4/socks4a 不再支持，跳过
        if link.lower().startswith(("socks4://", "socks4a://")):
            return link
        parsed = urlparse(link)
        host = parsed.hostname or proxy.address
        port = parsed.port or int(proxy.port) if proxy.port.isdigit() else 0
        if parsed.fragment:
            name = unquote(parsed.fragment)
        else:
            name = f"{host}-{port}"
        auth = ""
        if parsed.username:
            auth = unquote(parsed.username)
            if parsed.password:
                auth += f":{unquote(parsed.password)}"
            auth += "@"
        return f"socks5://{auth}{host}:{port}#{name}"
    elif link.startswith(("http://", "https://")) and ("#" in link or proxy.protocol in ("http", "https")):
        parsed = urlparse(link)
        host = parsed.hostname or proxy.address
        port = parsed.port or int(proxy.port) if proxy.port.isdigit() else 8080
        protocol = "https" if link.lower().startswith("https://") else "http"
        if parsed.fragment:
            name = unquote(parsed.fragment)
        else:
            name = f"{host}-{port}"
        auth = ""
        if parsed.username:
            auth = unquote(parsed.username)
            if parsed.password:
                auth += f":{unquote(parsed.password)}"
            auth += "@"
        return f"{protocol}://{auth}{host}:{port}#{name}"
    return link


def generate_v2ray_subscription(proxies: list[ProxyDBRecord]) -> str:
    """生成 v2ray 格式订阅内容（纯文本，每行一条原始链接）

    兼容旧名，实际输出与 generate_plain_subscription 一致
    """
    return generate_plain_subscription(proxies)


def generate_clash_subscription(proxies: list[ProxyDBRecord]) -> str:
    """生成 Clash 格式订阅内容

    生成完整 Clash 配置：proxies + proxy-groups
    """
    proxy_list = []
    proxy_names = []

    for proxy in proxies:
        clash_proxy = _link_to_clash_proxy(proxy.link, proxy.name)
        if clash_proxy:
            proxy_list.append(clash_proxy)
            proxy_names.append(clash_proxy.get("name", proxy.name))

    config = {
        "mixed-port": 7890,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "proxies": proxy_list,
        "proxy-groups": [
            {
                "name": "NetHub",
                "type": "url-test",
                "proxies": proxy_names if proxy_names else ["DIRECT"],
                "url": "http://www.gstatic.com/generate_204",
                "interval": 300,
            },
            {
                "name": "Proxy",
                "type": "select",
                "proxies": ["NetHub", "DIRECT"] + proxy_names,
            },
        ],
        "rules": [
            "MATCH,Proxy",
        ],
    }

    return yaml.dump(config, default_flow_style=False, allow_unicode=True)


def _link_to_clash_proxy(link: str, fallback_name: str) -> dict | None:
    """将分享链接转为 Clash proxy 字典"""
    try:
        if link.startswith("vmess://"):
            return _vmess_link_to_clash(link, fallback_name)
        elif link.startswith("vless://"):
            return _vless_link_to_clash(link, fallback_name)
        elif link.startswith("trojan://"):
            return _trojan_link_to_clash(link, fallback_name)
        elif link.startswith("ss://"):
            return _ss_link_to_clash(link, fallback_name)
        elif link.startswith("hysteria2://") or link.startswith("hy2://"):
            return _hysteria2_link_to_clash(link, fallback_name)
        elif link.startswith("socks5://"):
            return _socks_link_to_clash(link, fallback_name)
        elif link.startswith(("http://", "https://")) and "#" in link:
            return _http_link_to_clash(link, fallback_name)
    except Exception as e:
        logger.debug("转换 Clash 格式失败: %s - %s", link[:50], e)
    return None


def _socks_link_to_clash(link: str, fallback_name: str) -> dict:
    """socks5:// 转 Clash proxy"""
    parsed = urlparse(link)
    name = unquote(parsed.fragment) if parsed.fragment else fallback_name
    proxy = {
        "name": name or fallback_name,
        "type": "socks5",
        "server": parsed.hostname or "",
        "port": parsed.port or 1080,
        "udp": True,
    }
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def _http_link_to_clash(link: str, fallback_name: str) -> dict:
    """http:// / https:// 代理链接转 Clash proxy"""
    parsed = urlparse(link)
    name = unquote(parsed.fragment) if parsed.fragment else fallback_name
    proxy = {
        "name": name or fallback_name,
        "type": "http",
        "server": parsed.hostname or "",
        "port": parsed.port or 8080,
    }
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    if link.lower().startswith("https://"):
        proxy["tls"] = True
    return proxy


def _vmess_link_to_clash(link: str, fallback_name: str) -> dict:
    """vmess:// 转 Clash proxy"""
    config_b64 = link[8:]
    padding = 4 - len(config_b64) % 4
    if padding != 4:
        config_b64 += "=" * padding
    config = json.loads(base64.b64decode(config_b64).decode("utf-8"))

    proxy = {
        "name": config.get("ps", fallback_name) or fallback_name,
        "type": "vmess",
        "server": config.get("add", ""),
        "port": int(config.get("port", 443)),
        "uuid": config.get("id", ""),
        "alterId": int(config.get("aid", 0)),
        "cipher": config.get("scy", "auto"),
    }

    network = config.get("net", "tcp")
    if network == "ws":
        proxy["network"] = "ws"
        ws_opts = {}
        if config.get("path"):
            ws_opts["path"] = config["path"]
        if config.get("host"):
            ws_opts["headers"] = {"Host": config["host"]}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        proxy["network"] = "grpc"
        if config.get("path"):
            proxy["grpc-opts"] = {"grpc-service-name": config["path"]}
    elif network == "h2":
        proxy["network"] = "h2"
        h2_opts = {}
        if config.get("path"):
            h2_opts["path"] = config["path"]
        if config.get("host"):
            h2_opts["host"] = [config["host"]]
        if h2_opts:
            proxy["h2-opts"] = h2_opts

    if config.get("tls") == "tls":
        proxy["tls"] = True
        if config.get("sni"):
            proxy["servername"] = config["sni"]

    return proxy


def _vless_link_to_clash(link: str, fallback_name: str) -> dict:
    """vless:// 转 Clash proxy"""
    parsed = urlparse(link)
    query = parse_qs(parsed.query)

    name = unquote(parsed.fragment) if parsed.fragment else fallback_name
    uuid = unquote(parsed.username) if parsed.username else ""

    proxy = {
        "name": name or fallback_name,
        "type": "vless",
        "server": parsed.hostname or "",
        "port": parsed.port or 443,
        "uuid": uuid,
    }

    security = query.get("security", ["none"])[0]
    if security in ("tls", "reality"):
        proxy["tls"] = True
        sni = query.get("sni", [None])[0]
        if sni:
            proxy["servername"] = sni
        if security == "reality":
            proxy["reality-opts"] = {
                "public-key": query.get("pbk", [""])[0],
                "short-id": query.get("sid", [""])[0],
            }
            if query.get("fp", [None])[0]:
                proxy["client-fingerprint"] = query["fp"][0]

    network = query.get("type", ["tcp"])[0]
    if network == "ws":
        proxy["network"] = "ws"
        ws_opts = {}
        if query.get("path", [None])[0]:
            ws_opts["path"] = query["path"][0]
        if query.get("host", [None])[0]:
            ws_opts["headers"] = {"Host": query["host"][0]}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        proxy["network"] = "grpc"
        if query.get("serviceName", [None])[0]:
            proxy["grpc-opts"] = {"grpc-service-name": query["serviceName"][0]}

    flow = query.get("flow", [None])[0]
    if flow:
        proxy["flow"] = flow

    return proxy


def _trojan_link_to_clash(link: str, fallback_name: str) -> dict:
    """trojan:// 转 Clash proxy"""
    parsed = urlparse(link)
    query = parse_qs(parsed.query)

    name = unquote(parsed.fragment) if parsed.fragment else fallback_name
    password = unquote(parsed.username) if parsed.username else ""

    proxy = {
        "name": name or fallback_name,
        "type": "trojan",
        "server": parsed.hostname or "",
        "port": parsed.port or 443,
        "password": password,
    }

    sni = query.get("sni", [None])[0]
    if sni:
        proxy["sni"] = sni

    network = query.get("type", ["tcp"])[0]
    if network == "ws":
        proxy["network"] = "ws"
        ws_opts = {}
        if query.get("path", [None])[0]:
            ws_opts["path"] = query["path"][0]
        if query.get("host", [None])[0]:
            ws_opts["headers"] = {"Host": query["host"][0]}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        proxy["network"] = "grpc"
        if query.get("serviceName", [None])[0]:
            proxy["grpc-opts"] = {"grpc-service-name": query["serviceName"][0]}

    return proxy


def _ss_link_to_clash(link: str, fallback_name: str) -> dict:
    """ss:// 转 Clash proxy"""
    # 去掉 fragment
    line = link
    name = fallback_name
    if "#" in line:
        name = unquote(line[line.rindex("#") + 1:]) or fallback_name
        line = line[: line.rindex("#")]

    ss_content = line[5:]  # 去掉 'ss://'
    cipher = ""
    password = ""
    address = ""
    port = ""

    if "@" in ss_content:
        # SIP002 格式
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
            port = addr_port[bracket_end + 2:] if bracket_end + 2 < len(addr_port) else ""
        elif ":" in addr_port:
            address, port = addr_port.rsplit(":", 1)
    else:
        # 传统格式
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
                port = addr_port[bracket_end + 2:] if bracket_end + 2 < len(addr_port) else ""
            elif ":" in addr_port:
                address, port = addr_port.rsplit(":", 1)
        except Exception:
            pass

    return {
        "name": name,
        "type": "ss",
        "server": address,
        "port": int(port) if port else 443,
        "cipher": cipher,
        "password": password,
    }


def _hysteria2_link_to_clash(link: str, fallback_name: str) -> dict:
    """hysteria2:// 或 hy2:// 转 Clash proxy"""
    prefix_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
    rest = link[prefix_len:]

    name = fallback_name
    if "#" in rest:
        frag_start = rest.rindex("#")
        name = unquote(rest[frag_start + 1:]) or fallback_name
        rest = rest[:frag_start]

    # 构造标准 URL 以便解析
    if "?" in rest:
        host_part, query_part = rest.split("?", 1)
        parsed = urlparse("http://" + host_part)
        query = parse_qs(query_part)
    else:
        parsed = urlparse("http://" + rest)
        query = {}

    password = unquote(parsed.username) if parsed.username else ""

    proxy = {
        "name": name,
        "type": "hysteria2",
        "server": parsed.hostname or "",
        "port": parsed.port or 443,
    }

    if password:
        proxy["password"] = password

    sni = query.get("sni", [None])[0]
    if sni:
        proxy["sni"] = sni

    return proxy
