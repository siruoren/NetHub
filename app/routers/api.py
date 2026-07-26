"""REST API 路由 - JSON 格式的节点数据接口"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app import get_checker, get_config, get_db, get_scheduler
from app.generator import generate_clash_subscription, generate_plain_subscription, generate_v2ray_subscription

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


@router.get("/proxies")
async def get_available_proxies():
    """获取所有可用节点列表（latency_ms > 0 且 fail_count=0）"""
    db = get_db()
    config = get_config()
    proxies = await db.get_available_proxies(config.check.latency_threshold)
    return {
        "total": len(proxies),
        "proxies": [_proxy_to_dict(p) for p in proxies],
    }


@router.get("/proxies/all")
async def get_all_proxies():
    """获取所有节点（含不可用）"""
    db = get_db()
    proxies = await db.get_all_proxies()
    return {
        "total": len(proxies),
        "proxies": [_proxy_to_dict(p) for p in proxies],
    }



@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int):
    """删除指定节点"""
    db = get_db()
    await db.delete_proxy(proxy_id)
    return {"message": "deleted"}


@router.delete("/proxies")
async def delete_all_proxies():
    """一键清除数据库内所有节点"""
    db = get_db()
    count = await db.delete_all_proxies()
    return {"message": "deleted", "count": count}


@router.get("/subscription/v2ray")
async def v2ray_subscription():
    """获取纯文本格式订阅（每行一条原始代理 URI）

    输出所有延迟达标且未失败的节点（含已验证库节点，自动去重）
    """
    db = get_db()
    config = get_config()
    proxies = await db.get_subscription_output_proxies(config.check.latency_threshold)
    verified = await db.get_all_verified_proxies(config.check.latency_threshold)
    # 去重：已验证库中与订阅源重复的节点不输出（以协议+地址+端口去重）
    existing_keys = {(p.protocol, p.address, p.port) for p in proxies}
    for vp in verified:
        if (vp.protocol, vp.address, vp.port) not in existing_keys:
            proxies.append(vp)
    content = generate_v2ray_subscription(proxies)
    return _subscription_response(content, "text/plain")


@router.get("/subscription/plain")
async def plain_subscription():
    """获取纯文本格式订阅（每行一条原始代理 URI，参照 subdom.txt 格式）

    输出所有延迟达标且未失败的节点（含已验证库节点，自动去重）
    """
    db = get_db()
    config = get_config()
    proxies = await db.get_subscription_output_proxies(config.check.latency_threshold)
    verified = await db.get_all_verified_proxies(config.check.latency_threshold)
    # 去重：已验证库中与订阅源重复的节点不输出（以协议+地址+端口去重）
    existing_keys = {(p.protocol, p.address, p.port) for p in proxies}
    for vp in verified:
        if (vp.protocol, vp.address, vp.port) not in existing_keys:
            proxies.append(vp)
    content = generate_plain_subscription(proxies)
    return _subscription_response(content, "text/plain")


@router.get("/subscription/clash")
async def clash_subscription():
    """获取 Clash 格式订阅（YAML）

    输出所有延迟达标且未失败的节点（含已验证库节点，自动去重）
    """
    db = get_db()
    config = get_config()
    proxies = await db.get_subscription_output_proxies(config.check.latency_threshold)
    verified = await db.get_all_verified_proxies(config.check.latency_threshold)
    # 去重：已验证库中与订阅源重复的节点不输出（以协议+地址+端口去重）
    existing_keys = {(p.protocol, p.address, p.port) for p in proxies}
    for vp in verified:
        if (vp.protocol, vp.address, vp.port) not in existing_keys:
            proxies.append(vp)
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
    """手动触发验证节点"""
    scheduler = get_scheduler()
    if scheduler._verifying:
        return {"message": "验证任务正在进行中"}
    asyncio.create_task(scheduler.verify_stored_proxies())
    return {"message": "已触发验证任务"}


@router.post("/verify/{sub_id}")
async def manual_verify_subscription(sub_id: int):
    """手动触发验证指定订阅源的节点"""
    db = get_db()
    sub = await db.get_subscription_by_id(sub_id)
    if not sub:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="订阅不存在")
    scheduler = get_scheduler()
    if sub_id in scheduler._verifying_subs:
        return {"message": f"订阅 #{sub_id} 正在验证中，请勿重复操作"}
    asyncio.create_task(scheduler.verify_subscription_proxies(sub_id))
    return {"message": f"已触发订阅 #{sub_id} 的节点验证"}


@router.get("/stats")
async def get_stats():
    """获取统计信息"""
    db = get_db()
    scheduler = get_scheduler()
    config = get_config()
    stats = await db.get_stats()
    stats["last_fetch_time"] = scheduler.last_fetch_time
    stats["last_verify_time"] = scheduler.last_verify_time
    stats["max_proxies"] = config.scheduler.max_proxies

    # 添加每个实例源的节点数和限制信息
    instance_sources = await db.get_all_instance_sources()

    # 全局实例节点限制（来自配置/数据库设置）
    stats["max_instance_nodes"] = config.scheduler.max_instance_nodes

    instance_node_info = {}
    for inst in instance_sources:
        verified_count = await db.get_verified_count_by_instance_id(inst.id)
        instance_node_info[inst.id] = {
            "total_count": verified_count,
            "connected_count": inst.connected_count,
        }
    stats["instance_node_info"] = instance_node_info

    # 添加每个订阅源的节点数和限制信息
    subscriptions = await db.get_all_subscriptions()
    sub_node_info = {}
    for sub in subscriptions:
        count = await db.get_proxy_count_by_subscription_id(sub.id)
        sub_node_info[sub.id] = {
            "node_count": count,
        }
    stats["sub_node_info"] = sub_node_info

    return stats


class MaxProxiesBody(BaseModel):
    max_proxies: int


class MaxInstanceNodesBody(BaseModel):
    max_instance_nodes: int


@router.put("/config/max-proxies")
async def update_max_proxies(body: MaxProxiesBody):
    """更新最大可用条目数（保存到数据库）"""
    if body.max_proxies < 1:
        return {"error": "max_proxies 必须为正整数"}
    max_proxies = body.max_proxies
    config = get_config()
    config.scheduler.max_proxies = max_proxies
    db = get_db()
    await db.set_setting("max_proxies", str(max_proxies))
    # 立即执行一次限制检查（订阅+已验证总数不超限）
    deleted = await db.enforce_max_proxies_with_verified(max_proxies)
    return {"max_proxies": max_proxies, "deleted": deleted}


@router.put("/config/max-instance-nodes")
async def update_max_instance_nodes(body: MaxInstanceNodesBody):
    """更新所有实例已验证节点总数上限（保存到数据库），0=不限制"""
    max_instance_nodes = body.max_instance_nodes
    if max_instance_nodes < 0:
        return {"error": "max_instance_nodes 不能为负数"}
    config = get_config()
    config.scheduler.max_instance_nodes = max_instance_nodes
    db = get_db()
    await db.set_setting("max_instance_nodes", str(max_instance_nodes))
    # 立即执行一次限制检查
    deleted = 0
    if max_instance_nodes > 0:
        deleted = await db.enforce_max_all_verified_proxies(max_instance_nodes)
    return {"max_instance_nodes": max_instance_nodes, "deleted": deleted}


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok"}


# ---- 订阅管理 ----

@router.get("/subscriptions")
async def get_subscriptions():
    """获取所有订阅源（过滤 nethub 内部地址）"""
    db = get_db()
    subs = await db.get_all_subscriptions()
    subs = [s for s in subs if "nethub" not in s.url.lower()]
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
    # 自动触发拉取（拉取完成后会自动验证该订阅下所有已入库节点）
    asyncio.create_task(scheduler._fetch_single_subscription(sub.id))
    return {
        "status": "added",
        "message": "订阅已添加，正在拉取并验证节点",
        "subscription": _subscription_to_dict(sub),
    }


@router.put("/subscriptions/{sub_id}")
async def update_subscription(sub_id: int, url: Optional[str] = None,
                               crontab: Optional[str] = None,
                               latency_threshold: Optional[float] = None,
                               max_retries: Optional[int] = None,
                               max_concurrent: Optional[int] = None,
                               enabled: Optional[bool] = None):
    """更新订阅源"""
    from fastapi import HTTPException
    db = get_db()
    kwargs = {}
    if url is not None:
        # 检查新 URL 是否已被其他订阅使用
        existing = await db.get_subscription_by_url(url)
        if existing and existing.id != sub_id:
            raise HTTPException(status_code=409, detail="订阅地址已被其他订阅使用")
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
        # 启用时自动拉取（拉取完成后会自动验证该订阅下所有已入库节点）
        asyncio.create_task(scheduler._fetch_single_subscription(sub.id))
    else:
        # 禁用时重置拉取状态
        await db.batch_update_subscription_meta(sub_id, fetch_status="idle")
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
    """获取按订阅来源分组的所有可用节点（含延迟-1待检测）"""
    db = get_db()
    instance_sources = await db.get_all_instance_sources()
    subscriptions = await db.get_all_subscriptions()
    subscriptions = [s for s in subscriptions if "nethub" not in s.url.lower()]
    result = {}
    for sub in subscriptions:
        proxies = await db.get_proxies_by_subscription_id(sub.id)
        if proxies:
            result[str(sub.id)] = [_proxy_to_dict(p) for p in proxies]
    return {"grouped": result}


# ---- 已验证库节点 ----

@router.get("/verified-proxies/grouped")
async def get_verified_proxies_grouped():
    """获取按实例源分组的已验证节点（包含所有节点，含延迟-1待检测）"""
    db = get_db()
    instance_sources = await db.get_all_instance_sources()
    result = {}
    for inst in instance_sources:
        vp = await db.get_verified_by_instance_id(inst.id)
        if vp:
            result[str(inst.id)] = [_proxy_to_dict(p) for p in vp]
    return {"verified_grouped": result}



@router.delete("/verified-proxies/{proxy_id}")
async def delete_verified_proxy(proxy_id: int):
    """删除指定已验证节点"""
    db = get_db()
    await db.batch_delete_verified([proxy_id])
    return {"message": "deleted"}


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
        "subscription_id": proxy.subscription_id,
        "created_at": proxy.created_at,
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
            "Profile-Title": "NetHub",
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


# ---- 服务实例源管理 ----

@router.get("/instance-sources")
async def get_instance_sources():
    """获取所有服务实例源"""
    db = get_db()
    sources = await db.get_all_instance_sources()
    return {
        "total": len(sources),
        "sources": [_instance_source_to_dict(s) for s in sources],
    }


class InstanceSourceCreate(BaseModel):
    """添加服务实例源请求体"""
    base_url: str
    username: str
    password: str
    crontab: Optional[str] = "*/10 * * * *"
    latency_threshold: Optional[float] = 1500.0
    max_concurrent: Optional[int] = 50


@router.post("/instance-sources")
async def add_instance_source(req: InstanceSourceCreate):
    """添加服务实例源"""
    from fastapi import HTTPException
    if not req.base_url:
        raise HTTPException(status_code=400, detail="服务实例地址不能为空")
    db = get_db()
    source = await db.add_instance_source(
        base_url=req.base_url,
        username=req.username,
        password=req.password,
        crontab=req.crontab or "*/10 * * * *",
        latency_threshold=req.latency_threshold or 1500.0,
        max_concurrent=req.max_concurrent or 50,
        enabled=True,
    )
    if not source:
        raise HTTPException(status_code=409, detail="服务实例地址已存在")
    # 注册定时任务
    scheduler = get_scheduler()
    scheduler._add_instance_source_job(source)
    # 自动触发首次获取
    asyncio.create_task(scheduler._fetch_single_instance_source(source.id))
    return _instance_source_to_dict(source)


@router.put("/instance-sources/{source_id}")
async def update_instance_source(source_id: int, base_url: Optional[str] = None,
                                  username: Optional[str] = None,
                                  password: Optional[str] = None,
                                  crontab: Optional[str] = None,
                                  latency_threshold: Optional[float] = None,
                                  max_concurrent: Optional[int] = None,
                                  enabled: Optional[bool] = None):
    """更新服务实例源"""
    from fastapi import HTTPException
    db = get_db()
    kwargs = {}
    if base_url is not None:
        kwargs["base_url"] = base_url
    if username is not None:
        kwargs["username"] = username
    if password is not None:
        kwargs["password"] = password
    if crontab is not None:
        kwargs["crontab"] = crontab
    if latency_threshold is not None:
        kwargs["latency_threshold"] = latency_threshold
    if max_concurrent is not None:
        kwargs["max_concurrent"] = max_concurrent
    if enabled is not None:
        kwargs["enabled"] = enabled

    success = await db.update_instance_source(source_id, **kwargs)
    if not success:
        raise HTTPException(status_code=404, detail="服务实例源不存在或无更新")
    # 刷新定时任务
    source = await db.get_instance_source_by_id(source_id)
    scheduler = get_scheduler()
    scheduler.refresh_instance_source_job(source)

    if source.enabled:
        # 启用时自动获取
        asyncio.create_task(scheduler._fetch_single_instance_source(source.id))
    else:
        await db.batch_update_instance_meta(source_id, fetch_status="idle")
    return _instance_source_to_dict(source)


@router.delete("/instance-sources/{source_id}")
async def delete_instance_source(source_id: int):
    """删除服务实例源"""
    from fastapi import HTTPException
    db = get_db()
    success = await db.delete_instance_source(source_id)
    if not success:
        raise HTTPException(status_code=404, detail="服务实例源不存在")
    # 移除定时任务
    scheduler = get_scheduler()
    scheduler.remove_instance_source_job(source_id)
    return {"message": "deleted"}


@router.post("/instance-sources/{source_id}/fetch")
async def manual_fetch_instance_source(source_id: int):
    """手动触发获取指定服务实例源的已连接节点"""
    db = get_db()
    source = await db.get_instance_source_by_id(source_id)
    if not source:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="服务实例源不存在")
    scheduler = get_scheduler()
    asyncio.create_task(scheduler._fetch_single_instance_source(source_id))
    return {"message": f"已触发实例源 #{source_id} 的获取任务"}


@router.post("/instance-sources/{source_id}/import-subs")
async def import_instance_subscriptions(source_id: int):
    """手工导入服务实例中的订阅源到本地订阅源表"""
    db = get_db()
    source = await db.get_instance_source_by_id(source_id)
    if not source:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="服务实例源不存在")
    scheduler = get_scheduler()
    count = await scheduler.import_instance_subscriptions(source_id)
    return {"message": f"已导入 {count} 个订阅源", "imported": count}


def _instance_source_to_dict(source) -> dict:
    """将 InstanceSourceRecord 转为 API 响应字典"""
    return {
        "id": source.id,
        "base_url": source.base_url,
        "username": source.username,
        "password": source.password,
        "crontab": source.crontab,
        "latency_threshold": source.latency_threshold,
        "max_concurrent": source.max_concurrent,
        "enabled": source.enabled,
        "created_at": source.created_at,
        "total_count": source.total_count,
        "fetch_status": source.fetch_status,
        "connected_count": source.connected_count,
    }


# ---- 配置导出/导入 ----

@router.get("/config/export")
async def export_config():
    """导出订阅源和服务实例源配置为 JSON 文件"""
    db = get_db()
    subs = await db.get_all_subscriptions()
    sources = await db.get_all_instance_sources()

    config = {
        "version": 1,
        "max_proxies": get_config().scheduler.max_proxies,
        "max_instance_nodes": get_config().scheduler.max_instance_nodes,
        "subscriptions": [
            {
                "url": s.url,
                "crontab": s.crontab,
                "latency_threshold": s.latency_threshold,
                "max_retries": s.max_retries,
                "max_concurrent": s.max_concurrent,
                "enabled": s.enabled,
            }
            for s in subs
        ],
        "instance_sources": [
            {
                "base_url": s.base_url,
                "username": s.username,
                "password": s.password,
                "crontab": s.crontab,
                "latency_threshold": s.latency_threshold,
                "max_concurrent": s.max_concurrent,
                "enabled": s.enabled,
            }
            for s in sources
        ],
    }

    from fastapi.responses import Response
    from datetime import datetime, timedelta, timezone
    import json
    content = json.dumps(config, ensure_ascii=False, indent=2)
    ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S")
    return Response(
        content=content,
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="nethub_config_{ts}.json"',
        },
    )


class ConfigImportRequest(BaseModel):
    """配置导入请求体"""
    config: dict


@router.post("/config/import")
async def import_config(req: ConfigImportRequest):
    """从 JSON 导入订阅源和服务实例源配置（去重，不覆盖已有）"""
    db = get_db()
    scheduler = get_scheduler()

    config = req.config
    sub_added = 0
    sub_dup = 0
    inst_added = 0
    inst_dup = 0

    # 导入 max_proxies 设置
    max_proxies = config.get("max_proxies")
    if max_proxies and isinstance(max_proxies, int) and max_proxies > 0:
        get_config().scheduler.max_proxies = max_proxies
        await db.set_setting("max_proxies", str(max_proxies))

    # 导入 max_instance_nodes 设置
    max_instance_nodes = config.get("max_instance_nodes")
    if max_instance_nodes and isinstance(max_instance_nodes, int) and max_instance_nodes >= 0:
        get_config().scheduler.max_instance_nodes = max_instance_nodes
        await db.set_setting("max_instance_nodes", str(max_instance_nodes))

    # 导入订阅源
    for item in config.get("subscriptions", []):
        url = item.get("url", "").strip()
        if not url:
            continue
        existing = await db.get_subscription_by_url(url)
        if existing:
            sub_dup += 1
            continue
        sub = await db.add_subscription(
            url=url,
            crontab=item.get("crontab", "0 * * * *"),
            latency_threshold=item.get("latency_threshold", 1500.0),
            max_retries=item.get("max_retries", 3),
            max_concurrent=item.get("max_concurrent", 50),
            enabled=item.get("enabled", True),
        )
        if sub:
            scheduler._add_subscription_job(sub)
            sub_added += 1

    # 导入服务实例源
    for item in config.get("instance_sources", []):
        base_url = item.get("base_url", "").strip()
        if not base_url:
            continue
        existing = await db.get_instance_source_by_url(base_url)
        if existing:
            inst_dup += 1
            continue
        source = await db.add_instance_source(
            base_url=base_url,
            username=item.get("username", ""),
            password=item.get("password", ""),
            crontab=item.get("crontab", "*/10 * * * *"),
            latency_threshold=item.get("latency_threshold", 1500.0),
            max_concurrent=item.get("max_concurrent", 50),
            enabled=item.get("enabled", True),
        )
        if source:
            scheduler._add_instance_source_job(source)
            inst_added += 1

    return {
        "message": f"导入完成: 订阅源新增 {sub_added} 个(跳过 {sub_dup} 个重复), 实例源新增 {inst_added} 个(跳过 {inst_dup} 个重复)",
        "subscription_added": sub_added,
        "subscription_duplicate": sub_dup,
        "instance_added": inst_added,
        "instance_duplicate": inst_dup,
    }
