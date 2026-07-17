"""定时任务调度模块 - 使用 APScheduler 管理拉取/验证/清理任务"""

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.checker import ProxyChecker
from app.config import AppConfig
from app.database import ProxyDatabase
from app.parser import fetch_subscription, parse_subscription, load_subscription_urls

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
        """注册所有定时任务并启动调度器"""
        cfg = self.config.scheduler

        self.scheduler.add_job(
            self.fetch_and_check,
            "interval",
            seconds=cfg.fetch_interval,
            id="fetch_subscriptions",
            name="拉取订阅并检测",
        )
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
        logger.info("调度器已启动: fetch=%ds, verify=%ds, cleanup=%ds",
                     cfg.fetch_interval, cfg.verify_interval, cfg.cleanup_interval)

    def shutdown(self) -> None:
        """关闭调度器"""
        self.scheduler.shutdown(wait=False)
        logger.info("调度器已关闭")

    async def fetch_and_check(self) -> None:
        """任务1：拉取订阅 + 检测 + 入库

        1. 从 Subscription.txt 读取所有订阅 URL
        2. 逐个 URL 拉取内容并解析为 ProxyInfo 列表
        3. 对所有代理并发检测延迟
        4. 延迟低于 threshold 的代理插入/更新到数据库
        """
        if self._fetching:
            logger.info("上一次拉取任务尚未完成，跳过本次")
            return

        self._fetching = True
        try:
            logger.info("开始拉取订阅...")
            urls = await load_subscription_urls(self.config.resources.subscription_file)
            if not urls:
                logger.warning("未找到订阅 URL")
                return

            all_proxies = []
            for url in urls:
                try:
                    content = await fetch_subscription(url)
                    proxies = parse_subscription(content)
                    logger.info("订阅 %s: 解析到 %d 个节点", url[:50], len(proxies))
                    # 标记来源
                    for p in proxies:
                        all_proxies.append((p, url))
                except Exception as e:
                    logger.error("拉取订阅失败 %s: %s", url[:50], e)

            if not all_proxies:
                logger.warning("未解析到任何代理节点")
                return

            logger.info("共解析到 %d 个代理节点，开始并发检测...", len(all_proxies))

            # 并发检测延迟
            links = [p.link for p, _ in all_proxies]
            results = await self.checker.check_batch(links)

            # 处理检测结果
            added = 0
            updated = 0
            threshold = self.config.check.latency_threshold

            for proxy, source in all_proxies:
                latency = results.get(proxy.link)
                if latency is not None and latency <= threshold:
                    # 延迟达标，尝试入库
                    existing = await self.db.get_proxy_by_link(proxy.link)
                    if existing:
                        await self.db.update_latency(existing.id, latency)
                        updated += 1
                    else:
                        success = await self.db.insert_proxy(proxy, latency, source)
                        if success:
                            added += 1
                elif latency is not None and latency > threshold:
                    # 延迟超标，如果已在库中则增加失败计数
                    existing = await self.db.get_proxy_by_link(proxy.link)
                    if existing:
                        await self.db.increment_fail(existing.id)

            self._last_fetch_time = datetime.now(timezone.utc).isoformat()
            logger.info("拉取完成: 新增 %d, 更新 %d, 总解析 %d", added, updated, len(all_proxies))
        except Exception as e:
            logger.error("拉取任务异常: %s", e, exc_info=True)
        finally:
            self._fetching = False

    async def verify_stored_proxies(self) -> None:
        """任务2：验证已存代理可用性

        1. 从数据库获取所有代理
        2. 并发检测延迟
        3. 成功的更新 latency + 重置 fail_count
        4. 失败的 fail_count + 1
        """
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

    async def cleanup_proxies(self) -> None:
        """任务3：清理不合格代理

        删除 fail_count >= max_fail_count 的代理
        """
        max_fail = self.config.scheduler.max_fail_count
        deleted = await self.db.delete_proxies_by_fail_count(max_fail)
        if deleted > 0:
            logger.info("清理完成: 删除 %d 个不合格代理 (fail_count >= %d)", deleted, max_fail)

    @property
    def last_fetch_time(self) -> str:
        return self._last_fetch_time

    @property
    def last_verify_time(self) -> str:
        return self._last_verify_time
