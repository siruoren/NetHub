from __future__ import annotations
"""数据库操作层 - aiosqlite 异步封装"""

import os
from datetime import datetime, timezone

import aiosqlite

from app.models import ProxyDBRecord, ProxyInfo


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
        """)
        await self._db.commit()

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
            await self._db.execute(
                """INSERT OR IGNORE INTO proxies
                   (protocol, name, address, port, link, latency_ms, fail_count, source,
                    last_check_time, last_success_time, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
                (proxy.protocol, proxy.name, proxy.address, proxy.port, proxy.link,
                 latency_ms, source, now, now if latency_ms >= 0 else "", now),
            )
            await self._db.commit()
            return self._db.changes > 0
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
