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
from app.parser import fetch_subscription, parse_subscription, fetch_connected_proxies

logger = logging.getLogger(__name__)


class TaskScheduler:
    """节点池定时任务调度器"""

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
        asyncio.create_task(self._register_instance_source_jobs())

    async def _register_subscription_jobs(self) -> None:
        """从数据库加载订阅源并注册 crontab 定时任务"""
        subs = await self.db.get_enabled_subscriptions()
        for sub in subs:
            self._add_subscription_job(sub)
        logger.info("已注册 %d 个订阅拉取任务", len(subs))

    async def _register_instance_source_jobs(self) -> None:
        """从数据库加载服务实例源并注册 crontab 定时任务"""
        sources = await self.db.get_enabled_instance_sources()
        for source in sources:
            self._add_instance_source_job(source)
        logger.info("已注册 %d 个实例源获取任务", len(sources))

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

            # 同时获取服务实例源
            await self.fetch_all_instance_sources()
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

    # ---- 服务实例源管理 ----

    def _add_instance_source_job(self, source) -> None:
        """为单个服务实例源注册 crontab 定时任务"""
        job_id = f"fetch_instance_{source.id}"
        try:
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            parts = source.crontab.strip().split()
            if len(parts) != 5:
                logger.error("服务实例源 %d crontab 格式错误: %s", source.id, source.crontab)
                return

            trigger = CronTrigger(
                minute=parts[0], hour=parts[1], day=parts[2],
                month=parts[3], day_of_week=parts[4],
            )
            self.scheduler.add_job(
                self._fetch_single_instance_source,
                trigger=trigger,
                id=job_id,
                name=f"获取实例源 #{source.id}",
                args=[source.id],
            )
            logger.info("注册实例源任务 #%d: crontab=%s", source.id, source.crontab)
        except Exception as e:
            logger.error("注册实例源任务 #%d 失败: %s", source.id, e)

    def remove_instance_source_job(self, source_id: int) -> None:
        """移除服务实例源定时任务"""
        job_id = f"fetch_instance_{source_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

    def refresh_instance_source_job(self, source) -> None:
        """刷新服务实例源定时任务（更新 crontab 后调用）"""
        self.remove_instance_source_job(source.id)
        if source.enabled:
            self._add_instance_source_job(source)

    async def _fetch_single_instance_source(self, source_id: int) -> None:
        """从服务实例获取已连接节点配置，先全部入库再检测延迟

        实例源的节点：先入库（延迟=-1），再逐个检测更新延迟。
        检测不通过也保留在库中，仅累加 fail_count，由定时清理任务
        在连续 7 天无成功后才移除。
        """
        source = await self.db.get_instance_source_by_id(source_id)
        if not source or not source.enabled:
            return

        # 实例源的 source 标识
        source_tag = f"instance:{source.base_url}"

        try:
            logger.info("开始获取实例源 #%d: %s", source.id, source.base_url)
            await self.db.update_instance_fetch_status(source_id, "updating")

            proxies = await fetch_connected_proxies(
                source.base_url, source.username, source.password,
            )

            if not proxies:
                logger.warning("实例源 #%d 未获取到任何已连接节点", source.id)
                await self.db.update_instance_total_count(source_id, 0)
                await self.db.update_instance_fetch_status(source_id, "success")
                return

            logger.info("实例源 #%d: 获取到 %d 个节点，先入库再检测...",
                        source.id, len(proxies))

            # ---- 第一步：全部入库（延迟=-1 表示未检测） ----
            added = 0
            existed = 0
            for proxy in proxies:
                existing = await self.db.get_proxy_by_link(proxy.link)
                if existing:
                    existed += 1
                else:
                    # 入库时 latency_ms=-1（未检测），不影响后续检测
                    success = await self.db.insert_proxy(proxy, -1, source_tag)
                    if success:
                        added += 1

            logger.info("实例源 #%d: 入库完成, 新增 %d, 已存在 %d, 开始检测延迟...",
                        source.id, added, existed)

            # ---- 第二步：逐个检测延迟并更新 ----
            sub_checker = ProxyChecker(
                check_urls=self.checker.check_urls,
                timeout=self.config.check.timeout,
                max_concurrent=source.max_concurrent,
            )

            checked_ok = 0
            checked_fail = 0
            for proxy in proxies:
                db_record = await self.db.get_proxy_by_link(proxy.link)
                if not db_record:
                    continue

                try:
                    latency = await sub_checker.check_proxy(proxy.link)
                except Exception as e:
                    logger.debug("检测节点 %s 异常: %s", proxy.name[:30], e)
                    latency = None

                if latency is not None and latency > 0:
                    await self.db.update_latency(db_record.id, latency)
                    checked_ok += 1
                else:
                    # 检测不通过也保留，仅累加 fail_count
                    await self.db.increment_fail(db_record.id)
                    checked_fail += 1

            await self.db.update_instance_total_count(source_id, len(proxies))
            await self.db.update_instance_fetch_status(source_id, "success")

            self._last_fetch_time = datetime.now(timezone.utc).isoformat()
            logger.info("实例源 #%d 获取完成: 入库 %d(新增%d), 检测成功 %d, 失败 %d, 总 %d",
                        source.id, added + existed, added, checked_ok, checked_fail, len(proxies))
        except Exception as e:
            await self.db.update_instance_fetch_status(source_id, "failed")
            logger.error("获取实例源 #%d 异常: %s", source.id, e, exc_info=True)

    async def fetch_all_instance_sources(self) -> None:
        """手动触发：获取所有启用的服务实例源的已连接节点"""
        sources = await self.db.get_enabled_instance_sources()
        if not sources:
            logger.warning("没有启用的服务实例源")
            return
        for source in sources:
            await self._fetch_single_instance_source(source.id)

    async def cleanup_proxies(self) -> None:
        """清理不合格代理和空订阅源

        - 订阅源代理：连续 3 次验证失败则移除（原逻辑不变）
        - 实例源代理：last_success_time 为空或距今超过 7 天且 fail_count > 0 才移除
        """
        # 清理订阅源代理：连续3次验证失败
        deleted = await self.db.delete_sub_proxies_by_fail_count(3)
        if deleted > 0:
            logger.info("清理订阅源代理: 删除 %d 个连续3次不可用的代理", deleted)

        # 清理实例源代理：连续7天无成功
        deleted_inst = await self.db.delete_instance_proxies_stale(7)
        if deleted_inst > 0:
            logger.info("清理实例源代理: 删除 %d 个连续7天无成功的代理", deleted_inst)

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
