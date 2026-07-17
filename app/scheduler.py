from __future__ import annotations
"""定时任务调度模块 - 使用 APScheduler 管理拉取/验证/清理任务，支持 crontab 配置"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.checker import ProxyChecker
from app.config import AppConfig
from app.database import ProxyDatabase
from app.parser import fetch_subscription, parse_subscription

logger = logging.getLogger(__name__)


class TaskScheduler:
    """代理池定时任务调度器"""

    def __init__(self, config: AppConfig, db: ProxyDatabase, checker: ProxyChecker):
        self.config = config
        self.db = db
        self.checker = checker
        self.scheduler = AsyncIOScheduler()
        self._last_fetch_time = ""
        self._last_verify_time = ""
        self._fetching = False
        self._verifying = False

    def start(self) -> None:
        """注册默认定时任务并启动调度器"""
        cfg = self.config.scheduler

        # 默认验证和清理任务（使用全局间隔配置）
        self.scheduler.add_job(
            self.verify_stored_proxies,
            "interval",
            seconds=cfg.verify_interval,
            id="verify_proxies",
            name="验证已存代理",
        )
        self.scheduler.add_job(
            self.cleanup_proxies,
            "interval",
            seconds=cfg.cleanup_interval,
            id="cleanup_proxies",
            name="清理不合格代理",
        )
        self.scheduler.start()
        logger.info("调度器已启动: verify=%ds, cleanup=%ds",
                     cfg.verify_interval, cfg.cleanup_interval)

        # 启动后注册订阅任务
        asyncio.create_task(self._register_subscription_jobs())

    async def _register_subscription_jobs(self) -> None:
        """从数据库加载订阅源并注册 crontab 定时任务"""
        subs = await self.db.get_enabled_subscriptions()
        for sub in subs:
            self._add_subscription_job(sub)
        logger.info("已注册 %d 个订阅拉取任务", len(subs))

    def _add_subscription_job(self, sub) -> None:
        """为单个订阅注册 crontab 定时任务"""
        job_id = f"fetch_sub_{sub.id}"
        try:
            # 移除已有同 ID 任务
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            parts = sub.crontab.strip().split()
            if len(parts) != 5:
                logger.error("订阅 %d crontab 格式错误: %s", sub.id, sub.crontab)
                return

            trigger = CronTrigger(
                minute=parts[0], hour=parts[1], day=parts[2],
                month=parts[3], day_of_week=parts[4],
            )
            self.scheduler.add_job(
                self._fetch_single_subscription,
                trigger=trigger,
                id=job_id,
                name=f"拉取订阅 #{sub.id}",
                args=[sub.id],
            )
            logger.info("注册订阅任务 #%d: crontab=%s", sub.id, sub.crontab)
        except Exception as e:
            logger.error("注册订阅任务 #%d 失败: %s", sub.id, e)

    def remove_subscription_job(self, sub_id: int) -> None:
        """移除订阅定时任务"""
        job_id = f"fetch_sub_{sub_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

    def refresh_subscription_job(self, sub) -> None:
        """刷新订阅定时任务（更新 crontab 后调用）"""
        self.remove_subscription_job(sub.id)
        if sub.enabled:
            self._add_subscription_job(sub)

    def shutdown(self) -> None:
        """关闭调度器"""
        self.scheduler.shutdown(wait=False)
        logger.info("调度器已关闭")

    async def _fetch_single_subscription(self, sub_id: int) -> None:
        """拉取单个订阅并检测入库"""
        sub = await self.db.get_subscription_by_id(sub_id)
        if not sub or not sub.enabled:
            return

        try:
            logger.info("开始拉取订阅 #%d: %s", sub.id, sub.url[:50])
            await self.db.update_fetch_status(sub_id, "updating")
            content = await fetch_subscription(sub.url)
            proxies = parse_subscription(content)
            if not proxies:
                logger.warning("订阅 #%d 未解析到任何节点", sub.id)
                await self.db.increment_empty_days(sub_id)
                await self.db.update_total_count(sub_id, 0)
                await self.db.update_fetch_status(sub_id, "success")
                return

            logger.info("订阅 #%d: 解析到 %d 个节点，开始逐个检测...", sub.id, len(proxies))

            # 创建该订阅专用的 checker（使用订阅自己的并发数和超时）
            sub_checker = ProxyChecker(
                check_urls=self.checker.check_urls,
                timeout=self.config.check.timeout,
                max_concurrent=sub.max_concurrent,
            )

            threshold = sub.latency_threshold
            added = 0
            updated = 0
            skipped = 0

            for proxy in proxies:
                # 每个代理独立检测延迟，报错自动跳过
                try:
                    latency = await sub_checker.check_proxy(proxy.link)
                except Exception as e:
                    logger.debug("检测代理 %s 异常，跳过: %s", proxy.name[:30], e)
                    skipped += 1
                    continue

                if latency is None:
                    # 检测失败，跳过
                    skipped += 1
                    existing = await self.db.get_proxy_by_link(proxy.link)
                    if existing:
                        await self.db.increment_fail(existing.id)
                    continue

                if latency <= threshold:
                    # 延迟达标
                    existing = await self.db.get_proxy_by_link(proxy.link)
                    if existing:
                        await self.db.update_latency(existing.id, latency)
                        updated += 1
                    else:
                        success = await self.db.insert_proxy(proxy, latency, sub.url)
                        if success:
                            added += 1
                else:
                    # 延迟超标
                    existing = await self.db.get_proxy_by_link(proxy.link)
                    if existing:
                        await self.db.increment_fail(existing.id)
                    skipped += 1

            # 有代理入库则重置空天数
            if added > 0:
                await self.db.reset_empty_days(sub_id)

            # 更新订阅源的代理总数
            await self.db.update_total_count(sub_id, len(proxies))
            await self.db.update_fetch_status(sub_id, "success")

            self._last_fetch_time = datetime.now(timezone.utc).isoformat()
            logger.info("订阅 #%d 拉取完成: 新增 %d, 更新 %d, 跳过 %d, 总解析 %d",
                        sub.id, added, updated, skipped, len(proxies))
        except Exception as e:
            await self.db.update_fetch_status(sub_id, "failed")
            logger.error("拉取订阅 #%d 异常: %s", sub.id, e, exc_info=True)

    async def fetch_and_check(self) -> None:
        """手动触发：拉取所有启用的订阅并检测入库"""
        if self._fetching:
            logger.info("上一次拉取任务尚未完成，跳过本次")
            return

        self._fetching = True
        try:
            subs = await self.db.get_enabled_subscriptions()
            if not subs:
                logger.warning("没有启用的订阅源")
                return

            for sub in subs:
                await self._fetch_single_subscription(sub.id)
        except Exception as e:
            logger.error("拉取任务异常: %s", e, exc_info=True)
        finally:
            self._fetching = False

    async def verify_stored_proxies(self) -> None:
        """验证已存代理可用性 - 失败则累加 fail_count，连续3次失败由清理任务移除"""
        if self._verifying:
            logger.info("上一次验证任务尚未完成，跳过本次")
            return

        self._verifying = True
        try:
            proxies = await self.db.get_all_proxies()
            if not proxies:
                logger.info("数据库中没有代理，跳过验证")
                return

            logger.info("开始验证 %d 个代理...", len(proxies))
            links = [p.link for p in proxies]
            results = await self.checker.check_batch(links)

            success_count = 0
            fail_count = 0

            for proxy in proxies:
                latency = results.get(proxy.link)
                if latency is not None:
                    await self.db.update_latency(proxy.id, latency)
                    success_count += 1
                else:
                    await self.db.increment_fail(proxy.id)
                    fail_count += 1

            self._last_verify_time = datetime.now(timezone.utc).isoformat()
            logger.info("验证完成: 成功 %d, 失败 %d", success_count, fail_count)
        except Exception as e:
            logger.error("验证任务异常: %s", e, exc_info=True)
        finally:
            self._verifying = False

    async def verify_subscription_proxies(self, sub_id: int) -> None:
        """验证指定订阅源的代理可用性 - 失败则累加 fail_count，连续3次失败由清理任务移除"""
        sub = await self.db.get_subscription_by_id(sub_id)
        if not sub:
            logger.warning("订阅 #%d 不存在", sub_id)
            return

        proxies = await self.db.get_proxies_by_source(sub.url)
        if not proxies:
            logger.info("订阅 #%d 没有代理，跳过验证", sub_id)
            return

        logger.info("开始验证订阅 #%d 的 %d 个代理...", sub_id, len(proxies))

        sub_checker = ProxyChecker(
            check_urls=self.checker.check_urls,
            timeout=self.config.check.timeout,
            max_concurrent=sub.max_concurrent,
        )
        links = [p.link for p in proxies]
        results = await sub_checker.check_batch(links)

        success_count = 0
        fail_count = 0

        for proxy in proxies:
            latency = results.get(proxy.link)
            if latency is not None:
                await self.db.update_latency(proxy.id, latency)
                success_count += 1
            else:
                await self.db.increment_fail(proxy.id)
                fail_count += 1

        self._last_verify_time = datetime.now(timezone.utc).isoformat()
        logger.info("订阅 #%d 验证完成: 成功 %d, 失败 %d", sub_id, success_count, fail_count)

    async def cleanup_proxies(self) -> None:
        """清理不合格代理和空订阅源"""
        # 清理连续3次验证失败的代理
        deleted = await self.db.delete_proxies_by_fail_count(3)
        if deleted > 0:
            logger.info("清理代理: 删除 %d 个连续3次不可用的代理", deleted)

        # 清理连续30天代理数为0的订阅源
        empty_subs = await self.db.get_subscriptions_with_empty_days(30)
        for sub in empty_subs:
            # 再次确认该订阅源下确实没有代理
            count = await self.db.get_proxy_count_by_source(sub.url)
            if count == 0:
                await self.db.delete_subscription(sub.id)
                self.remove_subscription_job(sub.id)
                logger.info("清理订阅: 删除 #%d 连续30天无代理 (%s)", sub.id, sub.url[:50])

    @property
    def last_fetch_time(self) -> str:
        return self._last_fetch_time

    @property
    def last_verify_time(self) -> str:
        return self._last_verify_time
