from __future__ import annotations
"""数据库操作层 - aiosqlite 异步封装"""

import os
from datetime import datetime, timezone

import aiosqlite

from app.models import ProxyDBRecord, ProxyInfo, SubscriptionRecord


class ProxyDatabase:
    """代理数据库异步操作层"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """初始化数据库连接和表结构"""
        # 确保数据目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS proxies (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                protocol    TEXT    NOT NULL,
                name        TEXT    NOT NULL DEFAULT '',
                address     TEXT    NOT NULL DEFAULT '',
                port        TEXT    NOT NULL DEFAULT '',
                link        TEXT    NOT NULL UNIQUE,
                latency_ms  REAL    DEFAULT -1,
                fail_count  INTEGER DEFAULT 0,
                source      TEXT    NOT NULL DEFAULT '',
                last_check_time    TEXT DEFAULT '',
                last_success_time  TEXT DEFAULT '',
                created_at         TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_proxies_protocol ON proxies(protocol);
            CREATE INDEX IF NOT EXISTS idx_proxies_latency ON proxies(latency_ms);
            CREATE INDEX IF NOT EXISTS idx_proxies_fail_count ON proxies(fail_count);
            CREATE INDEX IF NOT EXISTS idx_proxies_link ON proxies(link);

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
        """)
        await self._db.commit()

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
            source=row["source"],
            last_check_time=row["last_check_time"],
            last_success_time=row["last_success_time"],
            created_at=row["created_at"],
        )

    async def insert_proxy(self, proxy: ProxyInfo, latency_ms: float, source: str) -> bool:
        """插入新代理，link 唯一约束，重复则忽略。返回是否插入成功"""
        now = datetime.now(timezone.utc).isoformat()
        try:
            cursor = await self._db.execute(
                """INSERT OR IGNORE INTO proxies
                   (protocol, name, address, port, link, latency_ms, fail_count, source,
                    last_check_time, last_success_time, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
                (proxy.protocol, proxy.name, proxy.address, proxy.port, proxy.link,
                 latency_ms, source, now, now if latency_ms >= 0 else "", now),
            )
            await self._db.commit()
            return cursor.rowcount > 0
        except aiosqlite.IntegrityError:
            return False

    async def update_latency(self, proxy_id: int, latency_ms: float) -> None:
        """更新延迟并重置 fail_count"""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """UPDATE proxies
               SET latency_ms = ?, fail_count = 0,
                   last_check_time = ?, last_success_time = ?
               WHERE id = ?""",
            (latency_ms, now, now, proxy_id),
        )
        await self._db.commit()

    async def increment_fail(self, proxy_id: int) -> int:
        """fail_count + 1，返回当前值"""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """UPDATE proxies
               SET fail_count = fail_count + 1, last_check_time = ?
               WHERE id = ?""",
            (now, proxy_id),
        )
        await self._db.commit()

        cursor = await self._db.execute(
            "SELECT fail_count FROM proxies WHERE id = ?", (proxy_id,)
        )
        row = await cursor.fetchone()
        return row["fail_count"] if row else 0

    async def delete_proxy(self, proxy_id: int) -> None:
        """删除指定代理"""
        await self._db.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
        await self._db.commit()

    async def delete_proxies_by_fail_count(self, max_fail: int) -> int:
        """删除 fail_count >= max_fail 的代理，返回删除数量"""
        cursor = await self._db.execute(
            "DELETE FROM proxies WHERE fail_count >= ?", (max_fail,)
        )
        await self._db.commit()
        return cursor.rowcount

    async def get_all_proxies(self) -> list[ProxyDBRecord]:
        """获取所有代理"""
        cursor = await self._db.execute(
            "SELECT * FROM proxies ORDER BY latency_ms ASC"
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_available_proxies(self, max_latency: float) -> list[ProxyDBRecord]:
        """获取延迟低于阈值且未失败的可用代理"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies
               WHERE latency_ms > 0 AND latency_ms <= ? AND fail_count = 0
               ORDER BY latency_ms ASC""",
            (max_latency,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_proxy_by_link(self, link: str) -> ProxyDBRecord | None:
        """根据 link 查询代理"""
        cursor = await self._db.execute(
            "SELECT * FROM proxies WHERE link = ?", (link,)
        )
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def get_proxies_needing_verify(self, limit: int = 0) -> list[ProxyDBRecord]:
        """获取需要验证的代理（按 last_check_time 排序，最旧的优先）"""
        query = "SELECT * FROM proxies ORDER BY last_check_time ASC"
        if limit > 0:
            query += f" LIMIT {limit}"
        cursor = await self._db.execute(query)
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

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
        now = datetime.now(timezone.utc).isoformat()
        try:
            cursor = await self._db.execute(
                """INSERT INTO subscriptions (url, crontab, latency_threshold, max_retries, max_concurrent, enabled, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (url, crontab, latency_threshold, max_retries, max_concurrent, int(enabled), now),
            )
            await self._db.commit()
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
        """更新订阅源，支持部分字段更新。若 URL 变更则同步更新 proxies.source"""
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

        # 如果更新了 URL，同步更新 proxies 表的 source
        if "url" in updates:
            old_sub = await self.get_subscription_by_id(sub_id)
            if old_sub and old_sub.url != updates["url"]:
                await self._db.execute(
                    "UPDATE proxies SET source = ? WHERE source = ?",
                    (updates["url"], old_sub.url),
                )

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [sub_id]
        cursor = await self._db.execute(
            f"UPDATE subscriptions SET {set_clause} WHERE id = ?", values
        )
        await self._db.commit()
        return cursor.rowcount > 0

    async def delete_subscription(self, sub_id: int) -> bool:
        """删除订阅源"""
        cursor = await self._db.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
        await self._db.commit()
        return cursor.rowcount > 0

    async def increment_empty_days(self, sub_id: int) -> None:
        """订阅源连续空代理天数 +1"""
        await self._db.execute(
            "UPDATE subscriptions SET empty_days = empty_days + 1 WHERE id = ?",
            (sub_id,),
        )
        await self._db.commit()

    async def reset_empty_days(self, sub_id: int) -> None:
        """订阅源有代理时重置空天数为0"""
        await self._db.execute(
            "UPDATE subscriptions SET empty_days = 0 WHERE id = ?",
            (sub_id,),
        )
        await self._db.commit()

    async def get_subscriptions_with_empty_days(self, min_days: int) -> list[SubscriptionRecord]:
        """获取连续空代理天数 >= min_days 的订阅源"""
        cursor = await self._db.execute(
            "SELECT * FROM subscriptions WHERE empty_days >= ?", (min_days,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_subscription(row) for row in rows]

    async def get_proxy_count_by_source(self, source: str) -> int:
        """获取指定来源的代理总数"""
        cursor = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM proxies WHERE source = ?", (source,)
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def update_total_count(self, sub_id: int, count: int) -> None:
        """更新订阅源最新一次拉取的代理总数"""
        await self._db.execute(
            "UPDATE subscriptions SET total_count = ? WHERE id = ?",
            (count, sub_id),
        )
        await self._db.commit()

    async def update_fetch_status(self, sub_id: int, status: str) -> None:
        """更新订阅源拉取状态: idle / updating / success / failed"""
        await self._db.execute(
            "UPDATE subscriptions SET fetch_status = ? WHERE id = ?",
            (status, sub_id),
        )
        await self._db.commit()

    async def get_proxies_by_source(self, source: str) -> list[ProxyDBRecord]:
        """根据来源订阅 URL 获取代理，按入库时间正序"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies WHERE source = ?
               ORDER BY created_at ASC""",
            (source,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_available_proxies_by_source(self, source: str, max_latency: float) -> list[ProxyDBRecord]:
        """根据来源订阅 URL 获取可用代理，按入库时间正序"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies
               WHERE source = ? AND latency_ms > 0 AND latency_ms <= ? AND fail_count = 0
               ORDER BY created_at ASC""",
            (source, max_latency),
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_proxies_grouped_by_source(self, max_latency: float) -> dict[str, list[ProxyDBRecord]]:
        """获取按订阅来源分组的可用代理，按入库时间正序"""
        cursor = await self._db.execute(
            """SELECT * FROM proxies
               WHERE latency_ms > 0 AND latency_ms <= ? AND fail_count = 0
               ORDER BY source ASC, created_at ASC""",
            (max_latency,),
        )
        rows = await cursor.fetchall()
        grouped = {}
        for row in rows:
            record = self._row_to_record(row)
            grouped.setdefault(record.source, []).append(record)
        return grouped

    async def get_stats(self) -> dict:
        """获取统计信息"""
        cursor = await self._db.execute("SELECT COUNT(*) as total FROM proxies")
        total = (await cursor.fetchone())["total"]

        cursor = await self._db.execute(
            """SELECT COUNT(*) as available FROM proxies
               WHERE latency_ms > 0 AND fail_count = 0"""
        )
        available = (await cursor.fetchone())["available"]

        cursor = await self._db.execute(
            "SELECT AVG(latency_ms) as avg_latency FROM proxies WHERE latency_ms > 0"
        )
        avg_latency = (await cursor.fetchone())["avg_latency"] or 0

        cursor = await self._db.execute(
            "SELECT protocol, COUNT(*) as count FROM proxies GROUP BY protocol"
        )
        rows = await cursor.fetchall()
        protocol_dist = {row["protocol"]: row["count"] for row in rows}

        return {
            "total": total,
            "available": available,
            "unavailable": total - available,
            "avg_latency_ms": round(avg_latency, 1),
            "protocol_distribution": protocol_dist,
        }

    async def get_last_check_time(self) -> str:
        """获取最近一次检测时间"""
        cursor = await self._db.execute(
            """SELECT MAX(last_check_time) as latest FROM proxies
               WHERE last_check_time != ''"""
        )
        row = await cursor.fetchone()
        return row["latest"] or ""
