"""SQLite 持久化层（aiosqlite，单连接 + WAL）。"""
from __future__ import annotations

import json
from typing import Any, Optional

import aiosqlite

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS builds (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    detail           TEXT,
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT
);
CREATE TABLE IF NOT EXISTS steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id        INTEGER NOT NULL,
    step_uid        TEXT NOT NULL,
    command         TEXT NOT NULL,
    depends_on      TEXT NOT NULL,
    timeout_seconds REAL NOT NULL,
    status          TEXT NOT NULL,
    exit_code       INTEGER,
    detail          TEXT,
    log_path        TEXT,
    pid             INTEGER,
    pgid            INTEGER,
    proc_starttime  REAL,
    log_overflow    INTEGER NOT NULL DEFAULT 0,
    started_at      TEXT,
    finished_at     TEXT,
    UNIQUE(build_id, step_uid),
    FOREIGN KEY(build_id) REFERENCES builds(id)
);
"""

# 旧版本数据库的增量迁移：新增列用 IF NOT EXISTS 等价的 PRAGMA 检查完成
_COLUMN_MIGRATIONS = {
    "steps": {
        "log_overflow": "ALTER TABLE steps ADD COLUMN log_overflow INTEGER NOT NULL DEFAULT 0",
    },
}


class Database:
    def __init__(self) -> None:
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        config.ensure_dirs()
        self._conn = await aiosqlite.connect(
            config.DB_PATH, timeout=30, isolation_level=None
        )
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA busy_timeout=30000")
        await self._conn.executescript(_SCHEMA)
        await self._migrate()

    async def _migrate(self) -> None:
        """对既有数据库做向后兼容的增量加列（重启恢复依赖这些列）。"""
        assert self._conn is not None
        for table, columns in _COLUMN_MIGRATIONS.items():
            cur = await self._conn.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in await cur.fetchall()}
            for name, ddl in columns.items():
                if name not in existing:
                    await self._conn.execute(ddl)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        assert self._conn is not None
        cur = await self._conn.execute(sql, params)
        return cur.lastrowid

    async def one(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
        assert self._conn is not None
        cur = await self._conn.execute(sql, params)
        return await cur.fetchone()

    async def all(self, sql: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        assert self._conn is not None
        cur = await self._conn.execute(sql, params)
        return list(await cur.fetchall())

    # ---- 业务封装 -------------------------------------------------------

    async def create_build(self, name: str, now: str) -> int:
        return await self.execute(
            "INSERT INTO builds (name, status, created_at) VALUES (?, 'pending', ?)",
            (name, now),
        )

    async def create_step(
        self,
        build_id: int,
        step_uid: str,
        command: str,
        depends_on: list[str],
        timeout_seconds: float,
        log_path: str,
    ) -> None:
        await self.execute(
            """
            INSERT INTO steps
                (build_id, step_uid, command, depends_on, timeout_seconds,
                 status, log_path)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                build_id,
                step_uid,
                command,
                json.dumps(depends_on),
                timeout_seconds,
                log_path,
            ),
        )

    async def get_build(self, build_id: int) -> Optional[aiosqlite.Row]:
        return await self.one("SELECT * FROM builds WHERE id=?", (build_id,))

    async def list_builds(self, limit: int = 100) -> list[aiosqlite.Row]:
        return await self.all(
            "SELECT * FROM builds ORDER BY id DESC LIMIT ?", (limit,)
        )

    async def get_steps(self, build_id: int) -> list[aiosqlite.Row]:
        return await self.all(
            "SELECT * FROM steps WHERE build_id=? ORDER BY id", (build_id,)
        )

    async def get_step(self, build_id: int, step_uid: str) -> Optional[aiosqlite.Row]:
        return await self.one(
            "SELECT * FROM steps WHERE build_id=? AND step_uid=?",
            (build_id, step_uid),
        )

    async def set_build_status(
        self,
        build_id: int,
        status: str,
        now: str,
        detail: Optional[str] = None,
        started: bool = False,
        finished: bool = False,
    ) -> None:
        sets = ["status=?"]
        params: list[Any] = [status]
        if detail is not None:
            sets.append("detail=?")
            params.append(detail)
        if started:
            sets.append("started_at=COALESCE(started_at, ?)")
            params.append(now)
        if finished:
            sets.append("finished_at=?")
            params.append(now)
        params.append(build_id)
        await self.execute(f"UPDATE builds SET {', '.join(sets)} WHERE id=?", tuple(params))

    async def request_cancel(self, build_id: int) -> None:
        await self.execute(
            "UPDATE builds SET cancel_requested=1 WHERE id=?", (build_id,)
        )

    async def set_step_process(
        self, step_pk: int, pid: int, pgid: int, starttime: float, now: str
    ) -> None:
        await self.execute(
            "UPDATE steps SET pid=?, pgid=?, proc_starttime=?, status='running', "
            "started_at=COALESCE(started_at, ?) WHERE id=?",
            (pid, pgid, starttime, now, step_pk),
        )

    async def set_step_status(
        self,
        step_pk: int,
        status: str,
        now: str,
        exit_code: Optional[int] = None,
        detail: Optional[str] = None,
        log_overflow: bool = False,
    ) -> None:
        from .models import STEP_TERMINAL

        sets = ["status=?"]
        params: list[Any] = [status]
        # finished_at 只在进入终态时落笔；cleaning_up 等中间态不写
        if status in STEP_TERMINAL:
            sets.append("finished_at=?")
            params.append(now)
        if exit_code is not None:
            sets.append("exit_code=?")
            params.append(exit_code)
        if detail is not None:
            sets.append("detail=?")
            params.append(detail)
        if log_overflow:
            # 只置位不清零：一旦超限即永久事实，后续任何状态更新都不能抹掉
            sets.append("log_overflow=1")
        params.append(step_pk)
        await self.execute(f"UPDATE steps SET {', '.join(sets)} WHERE id=?", tuple(params))

    async def mark_log_overflow(self, step_pk: int) -> None:
        """首次确认日志超限时立即落库，保证重启也能识别该失败原因。"""
        await self.execute(
            "UPDATE steps SET log_overflow=1 WHERE id=? AND log_overflow=0",
            (step_pk,),
        )

    async def list_nonterminal_builds(self) -> list[aiosqlite.Row]:
        return await self.all(
            "SELECT * FROM builds WHERE status NOT IN ('succeeded','failed','cancelled','interrupted')"
        )

    async def list_steps_for_builds(self, build_ids: tuple[int, ...]) -> list[aiosqlite.Row]:
        if not build_ids:
            return []
        placeholders = ",".join("?" * len(build_ids))
        return await self.all(
            f"SELECT * FROM steps WHERE build_id IN ({placeholders}) ORDER BY id",
            build_ids,
        )
