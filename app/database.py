from __future__ import annotations
"""数据库操作层 - aiosqlite 异步封装"""

import os
from datetime import datetime, timedelta, timezone

import aiosqlite

from app.models import ProxyDBRecord, ProxyInfo, SubscriptionRecord, InstanceSourceRecord


class ProxyDatabase:
    """节点数据库异步操作层"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._stats_cache: dict | None = None     # 统计信息内存缓存
        self._stats_dirty: bool = True             # 缓存是否需要刷新

    async def init(self) -> None:
        """初始化数据库连接和表结构"""
        # 确保数据目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        # SQLite 性能优化
        await self._db.execute("PRAGMA journal_mode=WAL")       # WAL 模式，读写并发
        await self._db.execute("PRAGMA synchronous=NORMAL")      # 减少 fsync 次数
        await self._db.execute("PRAGMA cache_size=-8000")        # 8MB 缓存
        await self._db.execute("PRAGMA temp_store=MEMORY")       # 临时表内存存储

        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS proxies (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                protocol         TEXT    NOT NULL,
                name             TEXT    NOT NULL DEFAULT '',
                address          TEXT    NOT NULL DEFAULT '',
                port             TEXT    NOT NULL DEFAULT '',
                link             TEXT    NOT NULL UNIQUE,
                latency_ms       REAL    DEFAULT -1,
                fail_count       INTEGER DEFAULT 0,
                subscription_id  INTEGER NOT NULL DEFAULT 0,
                last_check_time  TEXT    DEFAULT '',
                last_success_time TEXT   DEFAULT '',
                created_at       TEXT    DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_proxies_protocol ON proxies(protocol);
            CREATE INDEX IF NOT EXISTS idx_proxies_latency ON proxies(latency_ms);
            CREATE INDEX IF NOT EXISTS idx_proxies_link ON proxies(link);
            CREATE INDEX IF NOT EXISTS idx_proxies_sub_id ON proxies(subscription_id);
            CREATE INDEX IF NOT EXISTS idx_proxies_sub_created ON proxies(subscription_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_proxies_available ON proxies(latency_ms, fail_count, subscription_id);

            CREATE TABLE IF NOT EXISTS subscriptions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                url               TEXT    NOT NULL UNIQUE,
                crontab           TEXT    NOT NULL DEFAULT '0 * * * *',
                latency_threshold REAL    DEFAULT 1500.0,
                max_retries       INTEGER DEFAULT 3,
                max_concurrent    INTEGER DEFAULT 50,
                enabled           INTEGER DEFAULT 1,
                created_at        TEXT    DEFAULT '',
                empty_days        INTEGER DEFAULT 0,
                total_count       INTEGER DEFAULT 0,
                fetch_status      TEXT    DEFAULT 'idle'
            );

            CREATE INDEX IF NOT EXISTS idx_subscriptions_url ON subscriptions(url);

            CREATE TABLE IF NOT EXISTS check_urls (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                url   TEXT    NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS instance_sources (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                base_url          TEXT    NOT NULL UNIQUE,
                username          TEXT    NOT NULL DEFAULT '',
                password          TEXT    NOT NULL DEFAULT '',
                crontab           TEXT    NOT NULL DEFAULT '*/10 * * * *',
                latency_threshold REAL    DEFAULT 1500.0,
                max_concurrent    INTEGER DEFAULT 50,
                enabled           INTEGER DEFAULT 1,
                created_at        TEXT    DEFAULT '',
                total_count       INTEGER DEFAULT 0,
                fetch_status      TEXT    DEFAULT 'idle'
            );

            CREATE INDEX IF NOT EXISTS idx_instance_sources_base_url ON instance_sources(base_url);
        """)
        await self._db.commit()

        # 迁移：为旧表添加 subscription_id 列（如不存在）
        try:
            await self._db.execute("ALTER TABLE proxies ADD COLUMN subscription_id INTEGER NOT NULL DEFAULT 0")
            await self._db.commit()
        except Exception:
            pass

        # 兼容旧表：添加 check_urls 表（如不存在）
        try:
            await self._db.execute(
                "CREATE TABLE IF NOT EXISTS check_urls (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL UNIQUE)"
            )
            await self._db.commit()
        except Exception:
            pass

        # 迁移：将旧的 http:// 检测 URL 升级为 https://
        try:
            await self._db.execute(
                "UPDATE check_urls SET url = REPLACE(url, 'http://', 'https://') WHERE url LIKE 'http://%'"
            )
            await self._db.commit()
        except Exception:
            pass

        # 兼容旧表：添加 empty_days 和 total_count 列（如不存在）
        try:
            await self._db.execute("ALTER TABLE subscriptions ADD COLUMN empty_days INTEGER DEFAULT 0")
            await self._db.commit()
        except Exception:
            pass
        try:
            await self._db.execute("ALTER TABLE subscriptions ADD COLUMN total_count INTEGER DEFAULT 0")
            await self._db.commit()
        except Exception:
            pass
        try:
            await self._db.execute("ALTER TABLE subscriptions ADD COLUMN fetch_status TEXT DEFAULT 'idle'")
            await self._db.commit()
        except Exception:
            pass

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            self._db = None

    def _row_to_record(self, row: aiosqlite.Row) -> ProxyDBRecord:
        """将数据库行转为 ProxyDBRecord"""
        return ProxyDBRecord(
            id=row["id"],
            protocol=row["protocol"],
            name=row["name"],
            address=row["address"],
            port=row["port"],
            link=row["link"],
            latency_ms=row["latency_ms"],
            fail_count=row["fail_count"],
            subscription_id=row["subscription_id"],
            last_check_time=row["last_check_time"],
            last_success_time=row["last_success_time"],
            created_at=row["created_at"],
        )

    async def insert_proxy(self, proxy: ProxyInfo, latency_ms: float, subscription_id: int) -> bool:
        """插入新节点，link 唯一约束，重复则忽略。返回是否插入成功"""
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        try:
            cursor = await self._db.execute(
                """INSERT OR IGNORE INTO proxies
                   (protocol, name, address, port, link, latency_ms, fail_count, subscription_id,
                    last_check_time, last_success_time, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
                (proxy.protocol, proxy.name, proxy.address, proxy.port, proxy.link,
                 latency_ms, subscription_id, now, now if latency_ms >= 0 else "", now),
            )
            await self._db.commit()
            self._invalidate_stats()
            return cursor.rowcount > 0
        except aiosqlite.IntegrityError:
            return False

    async def batch_insert_proxies(self, proxies: list[tuple[ProxyInfo, float, int]]) -> int:
        """批量插入新节点，link 唯一约束，重复则忽略。返回实际插入数量

        proxies: [(ProxyInfo, latency_ms, subscription_id), ...]
        使用 INSERT OR IGNORE + executemany，单次 commit
        """
        if not proxies:
            return 0
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        rows = [
            (proxy.protocol, proxy.name, proxy.address, proxy.port, proxy.link,
             latency, 0, sub_id, now, now if latency >= 0 else "", now)
            for proxy, latency, sub_id in proxies
        ]
        cursor = await self._db.executemany(
            """INSERT OR IGNORE INTO proxies
               (protocol, name, address, port, link, latency_ms, fail_count, subscription_id,
                last_check_time, last_success_time, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()
        # executemany 返回的总 rowcount 是所有语句的 rowcount之和
        inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        if inserted > 0:
            self._invalidate_stats()
        return inserted

    async def update_latency(self, proxy_id: int, latency_ms: float) -> None:
        """更新延迟并重置 fail_count"""
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        await self._db.execute(
            """UPDATE proxies
               SET latency_ms = ?, fail_count = 0,
                   last_check_time = ?, last_success_time = ?
               WHERE id = ?""",
            (latency_ms, now, now, proxy_id),
        )
        await self._db.commit()
        self._invalidate_stats()

    async def batch_update_latency(self, updates: list[tuple[int, float]]) -> None:
        """批量更新延迟并重置 fail_count

        updates: [(proxy_id, latency_ms), ...]
        单次 commit，避免逐条提交的性能开销
        """
        if not updates:
            return
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        await self._db.executemany(
            """UPDATE proxies
               SET latency_ms = ?, fail_count = 0,
                   last_check_time = ?, last_success_time = ?
               WHERE id = ?""",
            [(lat, now, now, pid) for pid, lat in updates],
        )
        await self._db.commit()
        self._invalidate_stats()

    async def batch_delete_proxies(self, proxy_ids: list[int]) -> None:
        """批量删除节点（检测失败直接删除）"""
        if not proxy_ids:
            return
        await self._db.executemany(
            "DELETE FROM proxies WHERE id = ?",
            [(pid,) for pid in proxy_ids],
        )
        await self._db.commit()
        self._invalidate_stats()

    async def delete_proxy(self, proxy_id: int) -> None:
        """删除指定节点"""
        await self._db.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
        await self._db.commit()
        self._invalidate_stats()

    async def delete_all_proxies(self) -> int:
        """删除所有节点，返回删除数量"""
        cursor = await self._db.execute("DELETE FROM proxies")
        await self._db.commit()
        self._invalidate_stats()
        return cursor.rowcount

    async def delete_proxies_by_subscription_id(self, subscription_id: int) -> int:
        """删除指定订阅源 ID 下的所有节点，返回删除数量"""
        cursor = await self._db.execute(
            "DELETE FROM proxies WHERE subscription_id = ?", (subscription_id,)
        )
        await self._db.commit()
        self._invalidate_stats()
        return cursor.rowcount

    async def get_all_proxies(self) -> list[ProxyDBRecord]:
        """获取所有节点"""
        cursor = await self._db.execute(
            "SELECT * FROM proxies ORDER BY latency_ms ASC"
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_available_proxies(self, max_latency: float) -> list[ProxyDBRecord]:
        """获取延迟低于阈值且未失败的可用节点"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies
               WHERE latency_ms > 0 AND latency_ms <= ? AND fail_count = 0
               ORDER BY latency_ms ASC""",
            (max_latency,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_subscription_output_proxies(self, max_latency: float) -> list[ProxyDBRecord]:
        """获取对外订阅输出节点列表

        输出所有延迟达标且未失败的节点
        """
        cursor = await self._db.execute(
            """SELECT * FROM proxies
               WHERE latency_ms > 0
                 AND latency_ms <= ?
                 AND fail_count = 0
               ORDER BY latency_ms ASC""",
            (max_latency,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_proxy_by_link(self, link: str) -> ProxyDBRecord | None:
        """根据 link 查询节点"""
        cursor = await self._db.execute(
            "SELECT * FROM proxies WHERE link = ?", (link,)
        )
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def get_proxies_by_links(self, links: list[str]) -> dict[str, ProxyDBRecord]:
        """根据 link 列表批量查询节点，返回 link→ProxyDBRecord 映射"""
        if not links:
            return {}
        placeholders = ",".join("?" for _ in links)
        cursor = await self._db.execute(
            f"SELECT * FROM proxies WHERE link IN ({placeholders})", links
        )
        rows = await cursor.fetchall()
        return {row["link"]: self._row_to_record(row) for row in rows}

    async def delete_proxies_by_subscription_id_and_protocol(self, subscription_id: int, protocol: str) -> int:
        """删除指定订阅源下特定协议的所有节点，返回删除数量"""
        cursor = await self._db.execute(
            "DELETE FROM proxies WHERE subscription_id = ? AND protocol = ?",
            (subscription_id, protocol),
        )
        await self._db.commit()
        self._invalidate_stats()
        return cursor.rowcount

    async def get_proxies_by_subscription_id(self, subscription_id: int) -> list[ProxyDBRecord]:
        """根据订阅源 ID 获取节点，按入库时间正序"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies WHERE subscription_id = ?
               ORDER BY created_at ASC""",
            (subscription_id,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_proxy_count_by_subscription_id(self, subscription_id: int) -> int:
        """获取指定订阅源的节点总数"""
        cursor = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM proxies WHERE subscription_id = ?", (subscription_id,)
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    def _row_to_subscription(self, row: aiosqlite.Row) -> SubscriptionRecord:
        """将数据库行转为 SubscriptionRecord"""
        return SubscriptionRecord(
            id=row["id"],
            url=row["url"],
            crontab=row["crontab"],
            latency_threshold=row["latency_threshold"],
            max_retries=row["max_retries"],
            max_concurrent=row["max_concurrent"],
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            empty_days=row["empty_days"] if "empty_days" in row.keys() else 0,
            total_count=row["total_count"] if "total_count" in row.keys() else 0,
            fetch_status=row["fetch_status"] if "fetch_status" in row.keys() else "idle",
        )

    # ---- 订阅管理 ----

    async def add_subscription(self, url: str, crontab: str = "0 * * * *",
                                latency_threshold: float = 1500.0,
                                max_retries: int = 3, max_concurrent: int = 50,
                                enabled: bool = True) -> SubscriptionRecord | None:
        """添加订阅源"""
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        try:
            cursor = await self._db.execute(
                """INSERT INTO subscriptions (url, crontab, latency_threshold, max_retries, max_concurrent, enabled, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (url, crontab, latency_threshold, max_retries, max_concurrent, int(enabled), now),
            )
            await self._db.commit()
            self._invalidate_stats()
            return await self.get_subscription_by_id(cursor.lastrowid)
        except aiosqlite.IntegrityError:
            return None

    async def get_subscription_by_id(self, sub_id: int) -> SubscriptionRecord | None:
        """根据 ID 获取订阅"""
        cursor = await self._db.execute("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
        row = await cursor.fetchone()
        return self._row_to_subscription(row) if row else None

    async def get_subscription_by_url(self, url: str) -> SubscriptionRecord | None:
        """根据 URL 获取订阅"""
        cursor = await self._db.execute("SELECT * FROM subscriptions WHERE url = ?", (url,))
        row = await cursor.fetchone()
        return self._row_to_subscription(row) if row else None

    async def get_all_subscriptions(self) -> list[SubscriptionRecord]:
        """获取所有订阅源"""
        cursor = await self._db.execute("SELECT * FROM subscriptions ORDER BY id ASC")
        rows = await cursor.fetchall()
        return [self._row_to_subscription(row) for row in rows]

    async def get_enabled_subscriptions(self) -> list[SubscriptionRecord]:
        """获取所有启用的订阅源"""
        cursor = await self._db.execute("SELECT * FROM subscriptions WHERE enabled = 1 ORDER BY id ASC")
        rows = await cursor.fetchall()
        return [self._row_to_subscription(row) for row in rows]

    async def update_subscription(self, sub_id: int, **kwargs) -> bool:
        """更新订阅源，支持部分字段更新"""
        allowed = {"url", "crontab", "latency_threshold", "max_retries", "max_concurrent", "enabled"}
        updates = {}
        for k, v in kwargs.items():
            if k in allowed:
                if k == "enabled":
                    updates[k] = int(v)
                else:
                    updates[k] = v
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [sub_id]
        cursor = await self._db.execute(
            f"UPDATE subscriptions SET {set_clause} WHERE id = ?", values
        )
        await self._db.commit()
        self._invalidate_stats()
        return cursor.rowcount > 0

    async def delete_subscription(self, sub_id: int) -> bool:
        """删除订阅源及其下所有节点（单次 commit）"""
        await self._db.execute("DELETE FROM proxies WHERE subscription_id = ?", (sub_id,))
        cursor = await self._db.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
        await self._db.commit()
        self._invalidate_stats()
        return cursor.rowcount > 0

    async def increment_empty_days(self, sub_id: int) -> None:
        """订阅源连续空节点天数 +1"""
        await self._db.execute(
            "UPDATE subscriptions SET empty_days = empty_days + 1 WHERE id = ?",
            (sub_id,),
        )
        await self._db.commit()

    async def reset_empty_days(self, sub_id: int) -> None:
        """订阅源有节点时重置空天数为0"""
        await self._db.execute(
            "UPDATE subscriptions SET empty_days = 0 WHERE id = ?",
            (sub_id,),
        )
        await self._db.commit()

    async def get_subscriptions_with_empty_days(self, min_days: int) -> list[SubscriptionRecord]:
        """获取连续空节点天数 >= min_days 的订阅源"""
        cursor = await self._db.execute(
            "SELECT * FROM subscriptions WHERE empty_days >= ?", (min_days,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_subscription(row) for row in rows]

    async def batch_update_subscription_meta(self, sub_id: int,
                                              total_count: int = None,
                                              fetch_status: str = None,
                                              reset_empty: bool = False) -> None:
        """批量更新订阅源元信息（total_count / fetch_status / empty_days），单次 commit"""
        updates = []
        params = []
        if total_count is not None:
            updates.append("total_count = ?")
            params.append(total_count)
        if fetch_status is not None:
            updates.append("fetch_status = ?")
            params.append(fetch_status)
        if reset_empty:
            updates.append("empty_days = 0")
        if not updates:
            return
        params.append(sub_id)
        await self._db.execute(
            f"UPDATE subscriptions SET {', '.join(updates)} WHERE id = ?", params
        )
        await self._db.commit()

    async def get_proxies_grouped_by_subscription(self, max_latency: float) -> dict[int, list[ProxyDBRecord]]:
        """获取按订阅源 ID 分组的可用节点，按入库时间正序

        只查询模板需要的列，减少数据传输和对象创建开销
        """
        cursor = await self._db.execute(
            """SELECT id, protocol, name, address, port, link, latency_ms, subscription_id, created_at
               FROM proxies
               WHERE latency_ms > 0 AND latency_ms <= ? AND fail_count = 0
               ORDER BY subscription_id ASC, created_at ASC""",
            (max_latency,),
        )
        rows = await cursor.fetchall()
        grouped = {}
        for row in rows:
            record = ProxyDBRecord(
                id=row["id"],
                protocol=row["protocol"],
                name=row["name"],
                address=row["address"],
                port=row["port"],
                link=row["link"],
                latency_ms=row["latency_ms"],
                fail_count=0,
                subscription_id=row["subscription_id"],
                last_check_time="",
                last_success_time="",
                created_at=row["created_at"],
            )
            grouped.setdefault(record.subscription_id, []).append(record)
        return grouped

    def _invalidate_stats(self) -> None:
        """标记统计缓存需要刷新"""
        self._stats_dirty = True

    async def get_stats(self) -> dict:
        """获取统计信息（带内存缓存）

        total: 订阅源数量
        available: 可用节点数
        avg_latency_ms: 平均延迟
        protocol_distribution: 协议分布

        使用一条 SQL 合并查询，减少 4 次独立数据库操作为 2 次（subscriptions + proxies）
        """
        if not self._stats_dirty and self._stats_cache is not None:
            return self._stats_cache

        cursor = await self._db.execute("SELECT COUNT(*) as total FROM subscriptions")
        total = (await cursor.fetchone())["total"]

        # 一条 SQL 同时获取可用数、平均延迟和协议分布
        cursor = await self._db.execute("""
            SELECT
                COUNT(*) as available,
                AVG(latency_ms) as avg_latency,
                protocol
            FROM proxies WHERE latency_ms > 0
            GROUP BY protocol
        """)
        rows = await cursor.fetchall()
        available = sum(row["available"] for row in rows)
        avg_latency = sum(row["available"] * (row["avg_latency"] or 0) for row in rows) / available if available > 0 else 0
        protocol_dist = {row["protocol"]: row["available"] for row in rows}

        self._stats_cache = {
            "total": total,
            "available": available,
            "avg_latency_ms": round(avg_latency, 1),
            "protocol_distribution": protocol_dist,
        }
        self._stats_dirty = False
        return self._stats_cache

    async def get_last_check_time(self) -> str:
        """获取最近一次检测时间"""
        cursor = await self._db.execute(
            """SELECT MAX(last_check_time) as latest FROM proxies
               WHERE last_check_time != ''"""
        )
        row = await cursor.fetchone()
        return row["latest"] or ""

    # ---- 检测目标 URL ----

    async def get_check_urls(self) -> list[dict]:
        """获取所有检测目标 URL"""
        cursor = await self._db.execute("SELECT id, url FROM check_urls ORDER BY id ASC")
        rows = await cursor.fetchall()
        return [{"id": row["id"], "url": row["url"]} for row in rows]

    async def add_check_url(self, url: str) -> dict | None:
        """添加检测目标 URL，重复则忽略"""
        try:
            cursor = await self._db.execute(
                "INSERT INTO check_urls (url) VALUES (?)", (url,)
            )
            await self._db.commit()
            return {"id": cursor.lastrowid, "url": url}
        except aiosqlite.IntegrityError:
            return None

    async def delete_check_url(self, url_id: int) -> bool:
        """删除检测目标 URL"""
        cursor = await self._db.execute("DELETE FROM check_urls WHERE id = ?", (url_id,))
        await self._db.commit()
        return cursor.rowcount > 0

    async def init_check_urls(self, urls: list[str]) -> None:
        """初始化检测 URL（仅在表为空时插入）"""
        cursor = await self._db.execute("SELECT COUNT(*) as cnt FROM check_urls")
        row = await cursor.fetchone()
        if row["cnt"] == 0 and urls:
            for url in urls:
                try:
                    await self._db.execute("INSERT INTO check_urls (url) VALUES (?)", (url,))
                except aiosqlite.IntegrityError:
                    pass
            await self._db.commit()

    # ---- 服务实例源管理 ----

    def _row_to_instance_source(self, row: aiosqlite.Row) -> InstanceSourceRecord:
        """将数据库行转为 InstanceSourceRecord"""
        return InstanceSourceRecord(
            id=row["id"],
            base_url=row["base_url"],
            username=row["username"],
            password=row["password"],
            crontab=row["crontab"],
            latency_threshold=row["latency_threshold"],
            max_concurrent=row["max_concurrent"],
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            total_count=row["total_count"] if "total_count" in row.keys() else 0,
            fetch_status=row["fetch_status"] if "fetch_status" in row.keys() else "idle",
        )

    async def add_instance_source(self, base_url: str, username: str, password: str,
                                   crontab: str = "*/10 * * * *",
                                   latency_threshold: float = 1500.0,
                                   max_concurrent: int = 50,
                                   enabled: bool = True) -> InstanceSourceRecord | None:
        """添加服务实例源"""
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        try:
            cursor = await self._db.execute(
                """INSERT INTO instance_sources
                   (base_url, username, password, crontab, latency_threshold, max_concurrent, enabled, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (base_url, username, password, crontab, latency_threshold, max_concurrent, int(enabled), now),
            )
            await self._db.commit()
            return await self.get_instance_source_by_id(cursor.lastrowid)
        except aiosqlite.IntegrityError:
            return None

    async def get_instance_source_by_id(self, source_id: int) -> InstanceSourceRecord | None:
        """根据 ID 获取服务实例源"""
        cursor = await self._db.execute("SELECT * FROM instance_sources WHERE id = ?", (source_id,))
        row = await cursor.fetchone()
        return self._row_to_instance_source(row) if row else None

    async def get_instance_source_by_url(self, base_url: str) -> InstanceSourceRecord | None:
        """根据 base_url 获取服务实例源"""
        cursor = await self._db.execute("SELECT * FROM instance_sources WHERE base_url = ?", (base_url,))
        row = await cursor.fetchone()
        return self._row_to_instance_source(row) if row else None

    async def get_all_instance_sources(self) -> list[InstanceSourceRecord]:
        """获取所有服务实例源"""
        cursor = await self._db.execute("SELECT * FROM instance_sources ORDER BY id ASC")
        rows = await cursor.fetchall()
        return [self._row_to_instance_source(row) for row in rows]

    async def get_enabled_instance_sources(self) -> list[InstanceSourceRecord]:
        """获取所有启用的服务实例源"""
        cursor = await self._db.execute("SELECT * FROM instance_sources WHERE enabled = 1 ORDER BY id ASC")
        rows = await cursor.fetchall()
        return [self._row_to_instance_source(row) for row in rows]

    async def update_instance_source(self, source_id: int, **kwargs) -> bool:
        """更新服务实例源，支持部分字段更新"""
        allowed = {"base_url", "username", "password", "crontab", "latency_threshold", "max_concurrent", "enabled"}
        updates = {}
        for k, v in kwargs.items():
            if k in allowed:
                if k == "enabled":
                    updates[k] = int(v)
                else:
                    updates[k] = v
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [source_id]
        cursor = await self._db.execute(
            f"UPDATE instance_sources SET {set_clause} WHERE id = ?", values
        )
        await self._db.commit()
        return cursor.rowcount > 0

    async def delete_instance_source(self, source_id: int) -> bool:
        """删除服务实例源"""
        cursor = await self._db.execute("DELETE FROM instance_sources WHERE id = ?", (source_id,))
        await self._db.commit()
        return cursor.rowcount > 0

    async def batch_update_instance_meta(self, source_id: int,
                                           total_count: int = None,
                                           fetch_status: str = None) -> None:
        """批量更新服务实例源元信息（total_count + fetch_status），单次 commit"""
        updates = []
        params = []
        if total_count is not None:
            updates.append("total_count = ?")
            params.append(total_count)
        if fetch_status is not None:
            updates.append("fetch_status = ?")
            params.append(fetch_status)
        if not updates:
            return
        params.append(source_id)
        await self._db.execute(
            f"UPDATE instance_sources SET {', '.join(updates)} WHERE id = ?", params
        )
        await self._db.commit()
