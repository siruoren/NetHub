"""REST API 路由 - JSON 格式的代理数据接口"""

import asyncio
import logging

from fastapi import APIRouter, HTTPException

from app import get_checker, get_config, get_db, get_scheduler
from app.generator import generate_clash_subscription, generate_v2ray_subscription

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


@router.get("/proxies")
async def get_available_proxies():
    """获取所有可用代理列表（latency_ms > 0 且 fail_count=0）"""
    db = get_db()
    config = get_config()
    proxies = await db.get_available_proxies(config.check.latency_threshold)
    return {
        "total": len(proxies),
        "proxies": [_proxy_to_dict(p) for p in proxies],
    }


@router.get("/proxies/all")
async def get_all_proxies():
    """获取所有代理（含不可用）"""
    db = get_db()
    proxies = await db.get_all_proxies()
    return {
        "total": len(proxies),
        "proxies": [_proxy_to_dict(p) for p in proxies],
    }


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int):
    """删除指定代理"""
    db = get_db()
    await db.delete_proxy(proxy_id)
    return {"message": "deleted"}


@router.get("/subscription/v2ray")
async def v2ray_subscription():
    """获取 v2ray 格式订阅（base64 编码）"""
    db = get_db()
    config = get_config()
    proxies = await db.get_available_proxies(config.check.latency_threshold)
    content = generate_v2ray_subscription(proxies)
    return _subscription_response(content, "text/plain")


@router.get("/subscription/clash")
async def clash_subscription():
    """获取 Clash 格式订阅（YAML）"""
    db = get_db()
    config = get_config()
    proxies = await db.get_available_proxies(config.check.latency_threshold)
    content = generate_clash_subscription(proxies)
    return _subscription_response(content, "text/yaml")


@router.post("/fetch")
async def manual_fetch():
    """手动触发拉取订阅"""
    scheduler = get_scheduler()
    if scheduler._fetching:
        return {"message": "拉取任务正在进行中"}
    asyncio.create_task(scheduler.fetch_and_check())
    return {"message": "已触发拉取任务"}


@router.post("/verify")
async def manual_verify():
    """手动触发验证代理"""
    scheduler = get_scheduler()
    if scheduler._verifying:
        return {"message": "验证任务正在进行中"}
    asyncio.create_task(scheduler.verify_stored_proxies())
    return {"message": "已触发验证任务"}


@router.get("/stats")
async def get_stats():
    """获取统计信息"""
    db = get_db()
    scheduler = get_scheduler()
    stats = await db.get_stats()
    stats["last_fetch_time"] = scheduler.last_fetch_time
    stats["last_verify_time"] = scheduler.last_verify_time
    return stats


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok"}


def _proxy_to_dict(proxy) -> dict:
    """将 ProxyDBRecord 转为 API 响应字典"""
    return {
        "id": proxy.id,
        "protocol": proxy.protocol,
        "name": proxy.name,
        "address": proxy.address,
        "port": proxy.port,
        "latency_ms": proxy.latency_ms,
        "fail_count": proxy.fail_count,
        "source": proxy.source,
        "last_check_time": proxy.last_check_time,
        "last_success_time": proxy.last_success_time,
        "status": proxy.status,
    }


def _subscription_response(content: str, content_type: str):
    """生成订阅响应，添加通用 headers"""
    from fastapi.responses import Response

    return Response(
        content=content,
        media_type=f"{content_type}; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="subscription"',
            "Profile-Update-Interval": "24",
            "Profile-Title": "ProxyPool",
        },
    )
