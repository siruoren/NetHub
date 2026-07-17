from __future__ import annotations
"""Web UI 页面路由 - Jinja2 模板渲染"""

import json
import logging
from dataclasses import asdict

from fastapi import APIRouter, Request

from app import get_config, get_db, get_scheduler, get_templates

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
async def index(request: Request):
    """代理列表主页"""
    db = get_db()
    config = get_config()
    templates = get_templates()
    scheduler = get_scheduler()

    proxies = await db.get_all_proxies()
    stats = await db.get_stats()
    subscriptions = await db.get_all_subscriptions()
    # 按来源分组的可用代理
    grouped = await db.get_proxies_grouped_by_source(config.check.latency_threshold)
    # 计算每个订阅源的可用代理数量
    sub_available_counts = {}
    for sub in subscriptions:
        sub_available_counts[sub.url] = len(grouped.get(sub.url, []))
    subscriptions_json = json.dumps(
        [dict(asdict(s), available_count=sub_available_counts.get(s.url, 0)) for s in subscriptions],
        ensure_ascii=False,
    )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "proxies": proxies,
            "stats": stats,
            "subscriptions": subscriptions,
            "subscriptions_json": subscriptions_json,
            "sub_available_counts": sub_available_counts,
            "grouped": grouped,
            "latency_threshold": config.check.latency_threshold,
            "last_fetch_time": scheduler.last_fetch_time,
            "last_verify_time": scheduler.last_verify_time,
        },
    )


@router.get("/subscription")
async def subscription_page(request: Request):
    """订阅链接页面"""
    templates = get_templates()

    # 基于请求 host 动态生成订阅链接
    host = request.headers.get("host", f"localhost:{get_config().server.port}")
    scheme = request.url.scheme

    v2ray_url = f"{scheme}://{host}/api/subscription/v2ray"
    clash_url = f"{scheme}://{host}/api/subscription/clash"

    return templates.TemplateResponse(
        "subscription.html",
        {
            "request": request,
            "v2ray_url": v2ray_url,
            "clash_url": clash_url,
        },
    )
