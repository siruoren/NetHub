from __future__ import annotations
"""Web UI 页面路由 - Jinja2 模板渲染"""

import asyncio
import json
import logging
from dataclasses import asdict

from fastapi import APIRouter, Request

from app import get_config, get_db, get_scheduler, get_templates

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
async def index(request: Request):
    """节点列表主页"""
    db = get_db()
    config = get_config()
    templates = get_templates()
    scheduler = get_scheduler()

    # 并行查询（移除 get_all_proxies，页面只使用 grouped 数据）
    results = await asyncio.gather(
        db.get_stats(),
        db.get_all_subscriptions(),
        db.get_proxies_grouped_by_subscription(config.check.latency_threshold),
        db.get_check_urls(),
        db.get_all_instance_sources(),
    )
    stats, subscriptions, grouped, check_urls, instance_sources = results

    # 过滤包含 nethub 的订阅源（内部地址不在管理界面展示）
    subscriptions = [s for s in subscriptions if "nethub" not in s.url.lower()]

    # 序列化
    instance_sources_json = json.dumps(
        [dict(asdict(s)) for s in instance_sources],
        ensure_ascii=False,
    )

    # 计算每个订阅源的可用节点数量（grouped 按 subscription_id 分组）
    sub_available_counts = {}
    for sub in subscriptions:
        sub_available_counts[sub.id] = len(grouped.get(sub.id, []))

    subscriptions_json = json.dumps(
        [dict(asdict(s), available_count=sub_available_counts.get(s.id, 0)) for s in subscriptions],
        ensure_ascii=False,
    )

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "stats": stats,
            "subscriptions": subscriptions,
            "subscriptions_json": subscriptions_json,
            "sub_available_counts": sub_available_counts,
            "check_urls": check_urls,
            "instance_sources": instance_sources,
            "instance_sources_json": instance_sources_json,
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

    plain_url = f"{scheme}://{host}/api/subscription/plain"
    v2ray_url = f"{scheme}://{host}/api/subscription/v2ray"
    clash_url = f"{scheme}://{host}/api/subscription/clash"

    return templates.TemplateResponse(
        request,
        "subscription.html",
        {
            "plain_url": plain_url,
            "v2ray_url": v2ray_url,
            "clash_url": clash_url,
        },
    )
