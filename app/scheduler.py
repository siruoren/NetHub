from __future__ import annotations
"""定时任务调度模块 - 使用 APScheduler 管理拉取/验证/清理任务，支持 crontab 配置"""

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

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
        self._verifying_subs: set[int] = set()  # 正在验证的订阅 ID 集合，防止并发
        self._last_instance_sub_urls: list[str] = []  # 最近一次实例源获取的订阅地址缓存

    def start(self) -> None:
        """注册默认定时任务并启动调度器"""
        cfg = self.config.scheduler

        # 默认验证和清理任务（使用全局间隔配置）
        self.scheduler.add_job(
            self.verify_stored_proxies,
            "interval",
            seconds=cfg.verify_interval,
            id="verify_proxies",
            name="验证已存节点",
        )
        self.scheduler.add_job(
            self.cleanup_proxies,
            "interval",
            seconds=cfg.cleanup_interval,
            id="cleanup_proxies",
            name="清理不合格节点",
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
                jitter=random.randint(0, 600),  # 随机延迟0~600秒(10分钟内)
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
        """拉取单个订阅并检测入库（并发检测 + 批量数据库写入）

        节点绑定 subscription_id，已存在于其他订阅则跳过不更新。
        拉取完成后同时验证该订阅下所有已入库节点。
        """
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

            logger.info("订阅 #%d: 解析到 %d 个节点，并发检测中...", sub.id, len(proxies))

            # 自动移除该订阅下已有的 socks4 节点（不再支持）
            existing_proxies = await self.db.get_proxies_by_subscription_id(sub_id)
            socks4_ids = [p.id for p in existing_proxies if p.protocol == "socks4"]
            if socks4_ids:
                await self.db.batch_delete_proxies(socks4_ids)
                logger.info("订阅 #%d: 自动移除 %d 个 socks4 节点", sub_id, len(socks4_ids))

            # 并发检测所有节点延迟
            sub_checker = ProxyChecker(
                check_urls=self.checker.check_urls,
                timeout=self.config.check.timeout,
                max_concurrent=sub.max_concurrent,
                socks_port=self.checker.socks_port,
                http_port=self.checker.http_port,
                check_mode=self.checker.check_mode,
                kernel_path=self.checker.kernel_path,
                check_retries=self.config.check.check_retries,
            )
            links = [p.link for p in proxies]
            results = await sub_checker.check_batch(links)

            # 批量处理检测结果
            threshold = sub.latency_threshold
            latency_updates = []   # [(proxy_id, latency), ...]
            delete_ids = []        # [proxy_id, ...]  已入库但延迟超标需删除的节点
            added = 0
            skipped = 0

            # 批量查询所有解析节点的 link 是否已存在于数据库，避免 N+1 查询
            existing_map = await self.db.get_proxies_by_links(links)

            for proxy in proxies:
                latency = results.get(proxy.link)

                if latency is None:
                    # 检测失败 - 不入库，跳过
                    skipped += 1
                    continue

                existing = existing_map.get(proxy.link)
                if existing:
                    # 节点已存在于数据库中（可能属于其他订阅）
                    # 不更新、不转移，仅更新延迟
                    if latency <= threshold:
                        latency_updates.append((existing.id, latency))
                    else:
                        # 延迟超标 - 如果属于当前订阅则删除，否则仅跳过
                        if existing.subscription_id == sub_id:
                            delete_ids.append(existing.id)
                        skipped += 1
                else:
                    # 新节点
                    if latency <= threshold:
                        success = await self.db.insert_proxy(proxy, latency, sub_id)
                        if success:
                            added += 1
                    else:
                        skipped += 1

            # 批量写入数据库
            if latency_updates:
                await self.db.batch_update_latency(latency_updates)
            if delete_ids:
                await self.db.batch_delete_proxies(delete_ids)

            # 有节点入库则重置空天数
            if added > 0:
                await self.db.reset_empty_days(sub_id)

            # 更新订阅源的节点总数
            await self.db.update_total_count(sub_id, len(proxies))
            await self.db.update_fetch_status(sub_id, "success")

            self._last_fetch_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
            logger.info("订阅 #%d 拉取完成: 新增 %d, 更新延迟 %d, 跳过 %d, 总解析 %d",
                        sub.id, added, len(latency_updates), skipped, len(proxies))

            # 拉取完成后，同时验证该订阅下所有已入库节点
            await self.verify_subscription_proxies(sub_id)

        except Exception as e:
            await self.db.update_fetch_status(sub_id, "failed")
            logger.error("拉取订阅 #%d 异常: %s", sub.id, e, exc_info=True)

    async def fetch_and_check(self) -> None:
        """手动触发：并行拉取所有启用的订阅和实例源并检测入库"""
        if self._fetching:
            logger.info("上一次拉取任务尚未完成，跳过本次")
            return

        self._fetching = True
        try:
            subs = await self.db.get_enabled_subscriptions()
            sources = await self.db.get_enabled_instance_sources()

            if not subs and not sources:
                logger.warning("没有启用的订阅源或实例源")
                return

            # 并行拉取所有订阅和实例源
            tasks = []
            for sub in subs:
                tasks.append(self._fetch_single_subscription(sub.id))
            for source in sources:
                tasks.append(self._fetch_single_instance_source(source.id))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("并行拉取任务 %d 异常: %s", i, r)
        except Exception as e:
            logger.error("拉取任务异常: %s", e, exc_info=True)
        finally:
            self._fetching = False

    async def verify_stored_proxies(self) -> None:
        """验证已存节点可用性（并发检测 + 批量数据库写入）"""
        if self._verifying:
            logger.info("上一次验证任务尚未完成，跳过本次")
            return

        self._verifying = True
        try:
            proxies = await self.db.get_all_proxies()
            if not proxies:
                logger.info("数据库中没有节点，跳过验证")
                return

            logger.info("开始验证 %d 个节点...", len(proxies))
            links = [p.link for p in proxies]
            results = await self.checker.check_batch(links)

            latency_updates = []
            delete_ids = []

            for proxy in proxies:
                latency = results.get(proxy.link)
                if latency is not None:
                    latency_updates.append((proxy.id, latency))
                else:
                    # 检测失败直接删除
                    delete_ids.append(proxy.id)

            # 批量写入
            if latency_updates:
                await self.db.batch_update_latency(latency_updates)
            if delete_ids:
                await self.db.batch_delete_proxies(delete_ids)

            self._last_verify_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
            logger.info("验证完成: 成功 %d, 删除 %d", len(latency_updates), len(delete_ids))
        except Exception as e:
            logger.error("验证任务异常: %s", e, exc_info=True)
        finally:
            self._verifying = False

    async def verify_subscription_proxies(self, sub_id: int) -> None:
        """验证指定订阅源 ID 下所有已入库节点的可用性"""
        if sub_id in self._verifying_subs:
            logger.info("订阅 #%d 正在验证中，跳过本次", sub_id)
            return

        self._verifying_subs.add(sub_id)
        try:
            sub = await self.db.get_subscription_by_id(sub_id)
            if not sub:
                logger.warning("订阅 #%d 不存在", sub_id)
                return

            # 按 subscription_id 查询该订阅下所有已入库节点
            proxies = await self.db.get_proxies_by_subscription_id(sub_id)
            if not proxies:
                logger.info("订阅 #%d 没有已入库节点，跳过验证", sub_id)
                return

            logger.info("开始验证订阅 #%d 的 %d 个已入库节点...", sub_id, len(proxies))

            sub_checker = ProxyChecker(
                check_urls=self.checker.check_urls,
                timeout=self.config.check.timeout,
                max_concurrent=sub.max_concurrent,
                socks_port=self.checker.socks_port,
                http_port=self.checker.http_port,
                check_mode=self.checker.check_mode,
                kernel_path=self.checker.kernel_path,
                check_retries=self.config.check.check_retries,
            )
            links = [p.link for p in proxies]
            results = await sub_checker.check_batch(links)

            latency_updates = []
            delete_ids = []

            for proxy in proxies:
                latency = results.get(proxy.link)
                if latency is not None:
                    latency_updates.append((proxy.id, latency))
                else:
                    # 检测失败直接删除
                    delete_ids.append(proxy.id)

            if latency_updates:
                await self.db.batch_update_latency(latency_updates)
            if delete_ids:
                await self.db.batch_delete_proxies(delete_ids)

            self._last_verify_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
            logger.info("订阅 #%d 验证完成: 成功 %d, 删除 %d", sub_id, len(latency_updates), len(delete_ids))
        except Exception as e:
            logger.error("订阅 #%d 验证异常: %s", sub_id, e, exc_info=True)
        finally:
            self._verifying_subs.discard(sub_id)

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
                jitter=random.randint(0, 600),  # 随机延迟0~600秒(10分钟内)
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
        """从服务实例获取已连接节点数（仅更新统计，不写入数据库）"""
        source = await self.db.get_instance_source_by_id(source_id)
        if not source or not source.enabled:
            return

        try:
            logger.info("开始获取实例源 #%d: %s", source.id, source.base_url)
            await self.db.update_instance_fetch_status(source_id, "updating")

            proxies, subscription_urls = await fetch_connected_proxies(
                source.base_url, source.username, source.password,
            )

            # 缓存订阅地址列表，供手工导入时使用
            self._last_instance_sub_urls = subscription_urls

            connected_count = len(proxies)
            logger.info("实例源 #%d: 已连接 %d 个节点, 发现 %d 个订阅源",
                        source.id, connected_count, len(subscription_urls))

            # 仅更新统计信息，节点不写入数据库
            await self.db.update_instance_total_count(source_id, connected_count)
            await self.db.update_instance_fetch_status(source_id, "success")

            self._last_fetch_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
        except Exception as e:
            await self.db.update_instance_fetch_status(source_id, "failed")
            logger.error("获取实例源 #%d 异常: %s", source.id, e, exc_info=True)

    async def fetch_all_instance_sources(self) -> None:
        """手动触发：获取所有启用的服务实例源的已连接节点数"""
        sources = await self.db.get_enabled_instance_sources()
        if not sources:
            logger.warning("没有启用的服务实例源")
            return
        for source in sources:
            await self._fetch_single_instance_source(source.id)

    async def import_instance_subscriptions(self, source_id: int) -> int:
        """手工导入服务实例中的订阅源到本地订阅源表

        先重新获取实例源以拿到最新订阅地址列表，然后逐个新增。
        返回新增数量。
        """
        source = await self.db.get_instance_source_by_id(source_id)
        if not source or not source.enabled:
            return 0

        # 获取实例源的订阅地址列表
        _, subscription_urls = await fetch_connected_proxies(
            source.base_url, source.username, source.password,
        )

        if not subscription_urls:
            logger.info("实例源 #%d 没有订阅源可导入", source_id)
            return 0

        new_sub_count = 0
        for sub_url in subscription_urls:
            existing = await self.db.get_subscription_by_url(sub_url)
            if not existing:
                sub_record = await self.db.add_subscription(
                    url=sub_url,
                    crontab="0 * * * *",
                    latency_threshold=source.latency_threshold,
                    max_retries=3,
                    max_concurrent=source.max_concurrent,
                    enabled=True,
                )
                if sub_record:
                    self._add_subscription_job(sub_record)
                    asyncio.create_task(self._fetch_single_subscription(sub_record.id))
                    new_sub_count += 1
                    logger.info("导入订阅源: %s (来自实例源 #%d)", sub_url[:50], source_id)

        logger.info("实例源 #%d: 导入完成, 新增 %d 个订阅源", source_id, new_sub_count)
        return new_sub_count

    async def cleanup_proxies(self) -> None:
        """清理空订阅源

        检测失败的节点已在验证时直接删除，无需按 fail_count 清理。
        仅清理连续7天节点数为0的订阅源。
        """
        empty_subs = await self.db.get_subscriptions_with_empty_days(7)
        for sub in empty_subs:
            # 再次确认该订阅源下确实没有节点
            count = await self.db.get_proxy_count_by_subscription_id(sub.id)
            if count == 0:
                await self.db.delete_subscription(sub.id)
                self.remove_subscription_job(sub.id)
                logger.info("清理订阅: 删除 #%d 连续7天无节点 (%s)", sub.id, sub.url[:50])

    @property
    def last_fetch_time(self) -> str:
        return self._last_fetch_time

    @property
    def last_verify_time(self) -> str:
        return self._last_verify_time
