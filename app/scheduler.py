from __future__ import annotations
"""定时任务调度模块 - 使用 APScheduler 管理拉取/验证/清理任务，支持 crontab 配置"""

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

import aiohttp
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
        self._fetching_instances: set[int] = set()  # 正在获取的实例源 ID 集合，防止并发

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
        """拉取单个订阅并检测入库

        节点绑定 subscription_id，已存在于其他订阅则跳过不更新。
        拉取完成后快速验证该订阅下已入库节点。
        """
        sub = await self.db.get_subscription_by_id(sub_id)
        if not sub or not sub.enabled:
            return

        try:
            logger.info("订阅 #%d: 开始拉取 %s", sub.id, sub.url[:50])
            await self.db.batch_update_subscription_meta(sub_id, fetch_status="updating")
            content = await fetch_subscription(sub.url)
            logger.info("订阅 #%d: 拉取成功，开始解析", sub.id)
            proxies = parse_subscription(content)
            if not proxies:
                logger.warning("订阅 #%d: 未解析到任何节点", sub.id)
                await self.db.increment_empty_days(sub_id)
                await self.db.batch_update_subscription_meta(sub_id, total_count=0, fetch_status="success")
                return

            logger.info("订阅 #%d: 解析到 %d 个节点，开始并发检测...", sub.id, len(proxies))

            # 自动移除该订阅下已有的 socks4 节点（不再支持）
            socks4_count = await self.db.delete_proxies_by_subscription_id_and_protocol(sub_id, "socks4")
            if socks4_count:
                logger.info("订阅 #%d: 自动移除 %d 个 socks4 节点", sub_id, socks4_count)

            # 使用共享 checker 检测所有节点延迟
            links = [p.link for p in proxies]
            logger.info("订阅 #%d: 开始检测 %d 个节点延迟...", sub.id, len(links))
            results = await self.checker.check_batch(links)
            logger.info("订阅 #%d: 延迟检测完成，开始处理结果...", sub.id)

            # 批量处理检测结果
            threshold = sub.latency_threshold
            latency_updates = []
            delete_ids = []
            new_proxies = []
            skipped = 0
            verified_skipped = 0
            threshold_skipped = 0  # 超过阈值的节点数

            existing_map = await self.db.get_proxies_by_links(links)
            # 查询已验证库中存在的 link，这些节点不再重复插入订阅库
            verified_links = await self.db.get_verified_links_set(links)

            for proxy in proxies:
                latency = results.get(proxy.link)

                if latency is None:
                    skipped += 1
                    continue

                # 已验证库中存在的节点不再重复插入订阅库
                if proxy.link in verified_links:
                    verified_skipped += 1
                    continue

                existing = existing_map.get(proxy.link)
                if existing:
                    if latency <= threshold:
                        latency_updates.append((existing.id, latency))
                    else:
                        if existing.subscription_id == sub_id:
                            delete_ids.append(existing.id)
                        threshold_skipped += 1
                else:
                    if latency <= threshold:
                        new_proxies.append((proxy, latency, sub_id))
                    else:
                        threshold_skipped += 1

            logger.info("订阅 #%d: 检测结果 - 新增 %d, 更新 %d, 删除 %d, 跳过 %d(检测失败 %d, 超阈值 %d, 已验证库 %d)",
                        sub.id, len(new_proxies), len(latency_updates), len(delete_ids), 
                        skipped + threshold_skipped, skipped, threshold_skipped, verified_skipped)

            # 批量写入数据库
            added = await self.db.batch_insert_proxies(new_proxies)
            if latency_updates:
                await self.db.batch_update_latency(latency_updates)
            if delete_ids:
                await self.db.batch_delete_proxies(delete_ids)

            # 合并更新订阅源元信息
            await self.db.batch_update_subscription_meta(
                sub_id,
                total_count=len(proxies),
                fetch_status="success",
                reset_empty=(added > 0),
            )

            self._last_fetch_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
            logger.info("订阅 #%d: 拉取完成 - 新增 %d, 更新延迟 %d, 跳过 %d, 总解析 %d",
                        sub.id, added, len(latency_updates), skipped, len(proxies))

            # 强制执行全局节点限制（订阅+已验证总数不超限，超限优先删订阅源节点）
            max_proxies = self.config.scheduler.max_proxies
            if max_proxies > 0:
                deleted = await self.db.enforce_max_proxies_with_verified(max_proxies)
                if deleted:
                    logger.info("超出全局节点限制 %d，优先删除 %d 个订阅源节点", max_proxies, deleted)

        except Exception as e:
            await self.db.batch_update_subscription_meta(sub_id, fetch_status="failed")
            if isinstance(e, (asyncio.TimeoutError, TimeoutError, aiohttp.ClientError)):
                logger.warning("拉取订阅 #%d 失败: %s", sub.id, e)
            else:
                logger.error("拉取订阅 #%d 异常: %s", sub.id, e, exc_info=True)

    async def fetch_and_check(self) -> None:
        """手动触发：订阅源和实例源分别使用独立队列并发拉取

        订阅源队列：5 并发
        实例源队列：3 并发
        两个队列互不影响，独立运行
        """
        if self._fetching:
            logger.info("上一次拉取任务尚未完成，跳过本次")
            return

        self._fetching = True
        sub_sem = asyncio.Semaphore(5)  # 订阅源最多 5 个并发
        inst_sem = asyncio.Semaphore(3)  # 实例源最多 3 个并发

        try:
            subs = await self.db.get_enabled_subscriptions()
            sources = await self.db.get_enabled_instance_sources()

            if not subs and not sources:
                logger.warning("没有启用的订阅源或实例源")
                return

            logger.info("开始独立队列拉取: %d 个订阅源(5并发), %d 个实例源(3并发)",
                        len(subs), len(sources))

            # 将所有待处理订阅源状态设为 pending
            for sub in subs:
                await self.db.batch_update_subscription_meta(sub.id, fetch_status="pending")

            # 订阅源队列任务
            async def run_sub_task(index: int, sub_id: int):
                async with sub_sem:
                    logger.info("[订阅源 %d/%d] 开始处理 #%d", index, len(subs), sub_id)
                    try:
                        await self._fetch_single_subscription(sub_id)
                        logger.info("[订阅源 %d/%d] 完成 #%d", index, len(subs), sub_id)
                    except Exception as e:
                        logger.error("[订阅源 %d/%d] #%d 异常: %s", index, len(subs), sub_id, e)

            # 实例源队列任务
            async def run_inst_task(index: int, source_id: int):
                async with inst_sem:
                    logger.info("[实例源 %d/%d] 开始处理 #%d", index, len(sources), source_id)
                    try:
                        await self._fetch_single_instance_source(source_id)
                        logger.info("[实例源 %d/%d] 完成 #%d", index, len(sources), source_id)
                    except Exception as e:
                        logger.error("[实例源 %d/%d] #%d 异常: %s", index, len(sources), source_id, e)

            # 创建两个独立的任务组
            sub_tasks = []
            for i, sub in enumerate(subs):
                sub_tasks.append(asyncio.create_task(run_sub_task(i + 1, sub.id)))

            inst_tasks = []
            for i, source in enumerate(sources):
                inst_tasks.append(asyncio.create_task(run_inst_task(i + 1, source.id)))

            # 并行执行两个队列
            await asyncio.gather(
                asyncio.gather(*sub_tasks),
                asyncio.gather(*inst_tasks)
            )

            logger.info("独立队列拉取完成")

            # 所有订阅拉取完成后，验证已入库节点可用性
            if subs:
                logger.info("开始验证所有已入库节点可用性...")
                await self.verify_stored_proxies()
        except Exception as e:
            logger.error("拉取任务异常: %s", e, exc_info=True)
        finally:
            self._fetching = False

    async def verify_stored_proxies(self) -> None:
        """验证已存节点可用性（按订阅源分组批量检测 + 批量数据库写入）

        使用 grouped 查询替代全量查询，利用复合索引加速
        """
        if self._verifying:
            logger.info("上一次验证任务尚未完成，跳过本次")
            return

        self._verifying = True
        try:
            grouped = await self.db.get_proxies_grouped_by_subscription(
                self.config.check.latency_threshold * 2  # 验证时阈值放宽2倍
            )
            if not grouped:
                logger.info("数据库中没有可用节点，跳过验证")
                return

            total = sum(len(ps) for ps in grouped.values())
            logger.info("开始验证 %d 个节点（%d 个订阅源）...", total, len(grouped))

            all_links = []
            link_to_proxy = {}  # link → ProxyDBRecord
            for sub_id, proxies in grouped.items():
                for proxy in proxies:
                    all_links.append(proxy.link)
                    link_to_proxy[proxy.link] = proxy

            results = await self.checker.check_batch(all_links)

            latency_updates = []
            delete_ids = []

            for link, proxy in link_to_proxy.items():
                latency = results.get(link)
                if latency is not None:
                    latency_updates.append((proxy.id, latency))
                else:
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

            # 使用共享 checker 检测（复用 HTTP session）
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
        """从服务实例获取已连接节点，增量入已验证库，再全量验证延迟

        流程：
        1. 登录实例获取已连接节点和订阅地址列表
        2. 增量插入：已有节点跳过（INSERT OR IGNORE），新节点入已验证库（延迟默认-1）
        3. 全量验证：检测已验证库中该实例下所有节点的延迟
        4. 检测失败的删除，可达的更新延迟
        5. 全局节点限制检查（订阅+已验证总数不超限，超限优先删订阅源节点）
        """
        # 防止并发获取同一实例源
        if source_id in self._fetching_instances:
            logger.info("实例源 #%d 正在获取中，跳过本次", source_id)
            return

        source = await self.db.get_instance_source_by_id(source_id)
        if not source or not source.enabled:
            return

        self._fetching_instances.add(source_id)
        try:
            logger.info("开始获取实例源 #%d: %s", source.id, source.base_url)
            await self.db.batch_update_instance_meta(source_id, fetch_status="updating")

            # Add overall timeout to prevent hanging
            try:
                proxies, subscription_urls = await asyncio.wait_for(
                    fetch_connected_proxies(
                        source.base_url, source.username, source.password,
                    ),
                    timeout=300.0  # 5 minutes total timeout
                )
            except asyncio.TimeoutError:
                logger.error("实例源 #%d: 获取超时（5分钟），可能网络或服务实例响应慢", source.id)
                await self.db.batch_update_instance_meta(source_id, fetch_status="failed")
                return

            # 缓存订阅地址列表，供手工导入时使用
            self._last_instance_sub_urls = subscription_urls

            connected_count = len(proxies)
            logger.info("实例源 #%d: 已连接 %d 个节点, 发现 %d 个订阅源",
                        source.id, connected_count, len(subscription_urls))

            if connected_count > 0:
                # 检查这些节点是否在订阅节点库中存在，如果存在则从订阅库删除
                proxy_links = [p.link for p in proxies]
                existing_in_proxies = await self.db.get_proxy_links_set(proxy_links)
                if existing_in_proxies:
                    deleted_from_proxies = await self.db.delete_proxies_by_links(list(existing_in_proxies))
                    logger.info("实例源 #%d: 从订阅节点库中移除 %d 个重复节点", source.id, deleted_from_proxies)

                # 增量插入：已有节点跳过，新节点入已验证库（延迟默认-1）
                verified_items = [(proxy, -1.0, source_id) for proxy in proxies]
                added = await self.db.batch_insert_verified_proxies(verified_items)
                logger.info("实例源 #%d: 新增入库 %d 个节点（跳过已存在 %d），开始全量验证...",
                            source.id, added, connected_count - added)

                # 全量验证已验证库中该实例下所有节点的可用性
                try:
                    await self._verify_instance_verified(source_id)
                except Exception as verify_error:
                    logger.error("实例源 #%d: 验证过程异常: %s", source.id, verify_error)
                    # 验证失败也继续更新状态，避免卡在updating
            else:
                # 无已连接节点，清空该实例的已验证记录
                await self.db.delete_verified_by_instance_id(source_id)

            # 全局节点限制检查（订阅+已验证总数不超限，超限优先删订阅源节点）
            max_proxies = self.config.scheduler.max_proxies
            if max_proxies > 0:
                deleted = await self.db.enforce_max_proxies_with_verified(max_proxies)
                if deleted:
                    logger.info("全局节点限制 %d，优先删除 %d 个订阅源节点", max_proxies, deleted)

            # 更新元信息：已入库数 = 已验证库中该实例下的总量
            verified = await self.db.get_verified_by_instance_id(source_id)
            await self.db.batch_update_instance_meta(
                source_id,
                total_count=len(verified),
                fetch_status="success",
            )

            self._last_fetch_time = datetime.now(timezone(timedelta(hours=8))).isoformat()
        except Exception as e:
            logger.error("实例源 #%d: 获取过程异常: %s", source_id, e, exc_info=True)
            await self.db.batch_update_instance_meta(source_id, fetch_status="failed")
        finally:
            self._fetching_instances.discard(source_id)

    async def _verify_instance_verified(self, source_id: int) -> None:
        """验证已验证库中指定实例下所有节点的可用性，检测失败则删除

        使用实例源的延迟阈值过滤：延迟超过阈值的节点也会被删除
        """
        try:
            source = await self.db.get_instance_source_by_id(source_id)
            if not source:
                return

            proxies = await self.db.get_verified_by_instance_id(source_id)
            if not proxies:
                return

            max_latency = source.latency_threshold
            logger.info("实例源 #%d: 检测 %d 个已验证节点可用性（延迟阈值 %.1fms）...", 
                       source_id, len(proxies), max_latency)
            links = [p.link for p in proxies]
            results = await self.checker.check_batch(links)

            latency_updates = []
            delete_ids = []
            for proxy in proxies:
                latency = results.get(proxy.link)
                if latency is not None:
                    if latency <= max_latency:
                        logger.debug("实例源 #%d: 节点 %s 检测成功，延迟 %dms（达标）", 
                                   source_id, proxy.name[:30], latency)
                        latency_updates.append((proxy.id, latency))
                    else:
                        logger.debug("实例源 #%d: 节点 %s 延迟 %dms 超过阈值 %.1fms，将删除", 
                                   source_id, proxy.name[:30], latency, max_latency)
                        delete_ids.append(proxy.id)
                else:
                    logger.debug("实例源 #%d: 节点 %s 检测失败，将删除", source_id, proxy.name[:30])
                    # 检测失败直接删除
                    delete_ids.append(proxy.id)

            if latency_updates:
                await self.db.batch_update_verified_latency(latency_updates)
                # Log latency distribution for debugging
                latencies = [lat for _, lat in latency_updates]
                if latencies:
                    avg_latency = sum(latencies) / len(latencies)
                    min_latency = min(latencies)
                    max_latency_actual = max(latencies)
                    logger.info("实例源 #%d: 延迟分布 - 平均 %.1fms, 最小 %.1fms, 最大 %.1fms",
                                source_id, avg_latency, min_latency, max_latency_actual)
            if delete_ids:
                await self.db.batch_delete_verified(delete_ids)
            logger.info("实例源 #%d: 验证完成 - 成功 %d, 删除 %d",
                        source_id, len(latency_updates), len(delete_ids))
        except Exception as e:
            logger.error("实例源 #%d: 验证异常: %s", source_id, e, exc_info=True)

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
