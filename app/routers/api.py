"""REST API 路由 - JSON 格式的代理数据接口"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

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
    """手动触发拉取所有订阅"""
    scheduler = get_scheduler()
    if scheduler._fetching:
        return {"message": "拉取任务正在进行中"}
    asyncio.create_task(scheduler.fetch_and_check())
    return {"message": "已触发拉取任务"}


@router.post("/fetch/{sub_id}")
async def manual_fetch_subscription(sub_id: int):
    """手动触发拉取指定订阅"""
    db = get_db()
    sub = await db.get_subscription_by_id(sub_id)
    if not sub:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="订阅不存在")
    scheduler = get_scheduler()
    asyncio.create_task(scheduler._fetch_single_subscription(sub_id))
    return {"message": f"已触发订阅 #{sub_id} 的拉取任务"}


@router.post("/verify")
async def manual_verify():
    """手动触发验证代理"""
    scheduler = get_scheduler()
    if scheduler._verifying:
        return {"message": "验证任务正在进行中"}
    asyncio.create_task(scheduler.verify_stored_proxies())
    return {"message": "已触发验证任务"}


@router.post("/verify/{sub_id}")
async def manual_verify_subscription(sub_id: int):
    """手动触发验证指定订阅源的代理"""
    db = get_db()
    sub = await db.get_subscription_by_id(sub_id)
    if not sub:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="订阅不存在")
    scheduler = get_scheduler()
    asyncio.create_task(scheduler.verify_subscription_proxies(sub_id))
    return {"message": f"已触发订阅 #{sub_id} 的代理验证"}


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


# ---- 订阅管理 ----

@router.get("/subscriptions")
async def get_subscriptions():
    """获取所有订阅源"""
    db = get_db()
    subs = await db.get_all_subscriptions()
    return {
        "total": len(subs),
        "subscriptions": [_subscription_to_dict(s) for s in subs],
    }


@router.post("/subscriptions")
async def add_subscription(url: str = "", crontab: str = "0 * * * *",
                           latency_threshold: float = 1500.0,
                           max_retries: int = 3, max_concurrent: int = 50,
                           enabled: bool = True):
    """添加订阅源"""
    from fastapi import HTTPException
    if not url:
        raise HTTPException(status_code=400, detail="URL 不能为空")
    db = get_db()
    sub = await db.add_subscription(url, crontab, latency_threshold, max_retries, max_concurrent, enabled)
    if not sub:
        raise HTTPException(status_code=409, detail="订阅地址已存在")
    # 注册定时任务
    scheduler = get_scheduler()
    if sub.enabled:
        scheduler._add_subscription_job(sub)
        # 自动触发该订阅的延迟检测
        asyncio.create_task(scheduler._fetch_single_subscription(sub.id))
    return _subscription_to_dict(sub)


class AutoSubRequest(BaseModel):
    """自动添加订阅请求体"""
    url: str
    crontab: Optional[str] = None
    latency_threshold: Optional[float] = None
    max_retries: Optional[int] = None
    max_concurrent: Optional[int] = None


@router.post("/subscriptions/auto")
async def auto_add_subscription(req: AutoSubRequest):
    """自动添加订阅源 - 仅 URL 必填，其余可选，支持去重，新增后自动拉取并验证"""
    if not req.url:
        raise HTTPException(status_code=400, detail="URL 不能为空")
    db = get_db()
    # 去重：检查是否已存在
    existing = await db.get_subscription_by_url(req.url)
    if existing:
        return {
            "status": "duplicate",
            "message": "订阅地址已存在",
            "subscription": _subscription_to_dict(existing),
        }
    # 创建新订阅，未填字段使用默认值
    sub = await db.add_subscription(
        url=req.url,
        crontab=req.crontab or "0 * * * *",
        latency_threshold=req.latency_threshold or 1500.0,
        max_retries=req.max_retries or 3,
        max_concurrent=req.max_concurrent or 50,
        enabled=True,
    )
    if not sub:
        raise HTTPException(status_code=500, detail="添加订阅失败")
    # 注册定时任务
    scheduler = get_scheduler()
    scheduler._add_subscription_job(sub)
    # 自动触发拉取，拉取完成后自动验证
    async def fetch_and_verify():
        await scheduler._fetch_single_subscription(sub.id)
        await scheduler.verify_subscription_proxies(sub.id)
    asyncio.create_task(fetch_and_verify())
    return {
        "status": "added",
        "message": "订阅已添加，正在拉取并验证代理",
        "subscription": _subscription_to_dict(sub),
    }


@router.put("/subscriptions/{sub_id}")
async def update_subscription(sub_id: int, url: str = None, crontab: str = None,
                               latency_threshold: float = None, max_retries: int = None,
                               max_concurrent: int = None, enabled: bool = None):
    """更新订阅源"""
    from fastapi import HTTPException
    db = get_db()
    kwargs = {}
    if url is not None:
        kwargs["url"] = url
    if crontab is not None:
        kwargs["crontab"] = crontab
    if latency_threshold is not None:
        kwargs["latency_threshold"] = latency_threshold
    if max_retries is not None:
        kwargs["max_retries"] = max_retries
    if max_concurrent is not None:
        kwargs["max_concurrent"] = max_concurrent
    if enabled is not None:
        kwargs["enabled"] = enabled

    success = await db.update_subscription(sub_id, **kwargs)
    if not success:
        raise HTTPException(status_code=404, detail="订阅不存在或无更新")
    # 刷新定时任务
    sub = await db.get_subscription_by_id(sub_id)
    scheduler = get_scheduler()
    scheduler.refresh_subscription_job(sub)

    if sub.enabled:
        # 启用时自动拉取并验证该订阅源
        async def fetch_and_verify():
            await scheduler._fetch_single_subscription(sub.id)
            await scheduler.verify_subscription_proxies(sub.id)
        asyncio.create_task(fetch_and_verify())
    else:
        # 禁用时重置拉取状态
        await db.update_fetch_status(sub_id, "idle")
    return _subscription_to_dict(sub)


@router.delete("/subscriptions/{sub_id}")
async def delete_subscription(sub_id: int):
    """删除订阅源"""
    db = get_db()
    success = await db.delete_subscription(sub_id)
    if not success:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="订阅不存在")
    # 移除定时任务
    scheduler = get_scheduler()
    scheduler.remove_subscription_job(sub_id)
    return {"message": "deleted"}


@router.get("/proxies/grouped")
async def get_proxies_grouped():
    """获取按订阅来源分组的可用代理"""
    db = get_db()
    config = get_config()
    grouped = await db.get_proxies_grouped_by_source(config.check.latency_threshold)
    result = {}
    for source, proxies in grouped.items():
        result[source] = [_proxy_to_dict(p) for p in proxies]
    return {"grouped": result}


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


def _subscription_to_dict(sub) -> dict:
    """将 SubscriptionRecord 转为 API 响应字典"""
    return {
        "id": sub.id,
        "url": sub.url,
        "crontab": sub.crontab,
        "latency_threshold": sub.latency_threshold,
        "max_retries": sub.max_retries,
        "max_concurrent": sub.max_concurrent,
        "enabled": sub.enabled,
        "created_at": sub.created_at,
        "empty_days": sub.empty_days,
        "total_count": sub.total_count,
        "fetch_status": sub.fetch_status,
    }


# ---- 检测目标 URL ----

@router.get("/check-urls")
async def get_check_urls():
    """获取所有检测目标 URL"""
    db = get_db()
    urls = await db.get_check_urls()
    return {"total": len(urls), "urls": urls}


@router.post("/check-urls")
async def add_check_url(url: str = ""):
    """添加检测目标 URL"""
    from fastapi import HTTPException
    if not url:
        raise HTTPException(status_code=400, detail="URL 不能为空")
    db = get_db()
    result = await db.add_check_url(url)
    if not result:
        raise HTTPException(status_code=409, detail="URL 已存在")
    # 更新 checker 的 check_urls
    checker = get_checker()
    if checker:
        checker.check_urls = [u["url"] for u in await db.get_check_urls()]
    return result


@router.delete("/check-urls/{url_id}")
async def delete_check_url(url_id: int):
    """删除检测目标 URL"""
    from fastapi import HTTPException
    db = get_db()
    success = await db.delete_check_url(url_id)
    if not success:
        raise HTTPException(status_code=404, detail="URL 不存在")
    # 更新 checker 的 check_urls
    checker = get_checker()
    if checker:
        checker.check_urls = [u["url"] for u in await db.get_check_urls()]
    return {"message": "deleted"}
