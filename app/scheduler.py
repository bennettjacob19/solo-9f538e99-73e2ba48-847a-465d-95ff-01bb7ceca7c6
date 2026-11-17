"""流水线调度器。

并发模型：
- 全局 ``asyncio.Semaphore(2)`` 即"两个名额"，名额在整个步骤生命周期内持有，
  包含进程组清理阶段 —— 组未清空前不释放名额、不写步骤终态。
- 慢清理只占住自己的名额，不阻塞另一名额上的独立步骤/构建。
- 每个构建一个 asyncio 任务，轮询 DAG 状态推进；每个运行中的步骤一个子任务，
  通过 per-build cancel event 实现整次取消的即时响应。
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config, proc
from .db import Database
from .logcap import (
    OVERFLOW_REASON,
    BoundedLogWriter,
    bounded_append_log,
)
from .models import BUILD_TERMINAL, BuildCreate, validate_dag

_STEP_FAIL_STATES = {"failed", "skipped", "cancelled", "interrupted"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Scheduler:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._slots = asyncio.Semaphore(config.MAX_PARALLEL)
        self._build_events: dict[int, asyncio.Event] = {}
        self._build_tasks: dict[int, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------ API

    async def submit(self, payload: BuildCreate) -> int:
        validate_dag(payload.steps)  # ValueError 由路由层转 400
        build_id = await self.db.create_build(payload.name, _now())
        log_dir = config.LOG_ROOT / str(build_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        for step in payload.steps:
            log_path = log_dir / f"{step.id}.log"
            log_path.touch()
            await self.db.create_step(
                build_id,
                step.id,
                step.command,
                step.depends_on,
                step.timeout_seconds,
                str(log_path),
            )
        task = asyncio.create_task(self._run_build(build_id), name=f"build-{build_id}")
        self._build_tasks[build_id] = task
        task.add_done_callback(self._log_task_exception)
        return build_id

    async def cancel(self, build_id: int) -> tuple[Optional[dict[str, Any]], str]:
        """返回 (构建行或 None, 说明)。重复取消幂等，不改变已定终态。"""
        row = await self.db.get_build(build_id)
        if row is None:
            return None, "构建不存在"
        if row["status"] in BUILD_TERMINAL:
            return dict(row), f"构建已处于终态 {row['status']}，取消请求忽略"
        await self.db.request_cancel(build_id)
        event = self._build_events.get(build_id)
        if event is not None:
            event.set()
        return dict(row), "取消请求已受理，运行中的步骤将终止整个进程组"

    # ------------------------------------------------------------- 重启恢复

    async def recover(self) -> dict[str, int]:
        """API 启动时执行：先清理旧构建残留进程，再统一标记 interrupted。

        旧构建之外的进程（PID 已被复用、starttime 对不上）绝不动。
        恢复期间不恢复调度，未跑步骤一律不再启动。
        """
        stale = await self.db.list_nonterminal_builds()
        if not stale:
            return {"builds": 0, "killed_groups": 0}

        rows = await self.db.list_steps_for_builds(tuple(b["id"] for b in stale))
        grouped: dict[int, list[Any]] = {}
        for row in rows:
            grouped.setdefault(row["build_id"], []).append(row)

        killed_groups = 0
        for build in stale:
            build_id = build["id"]
            for step in grouped.get(build_id, []):
                if step["status"] not in ("running", "cleaning_up"):
                    continue
                pgid = step["pgid"]
                pid = step["pid"]
                starttime = step["proc_starttime"]
                if not pgid:
                    continue
                note: Optional[str] = None
                identity_ok = bool(
                    pid
                    and proc.is_same_process(pid, starttime)
                    and _safe_getpgid(pid) == pgid
                )
                members = await asyncio.to_thread(proc.list_pgid_members, pgid)
                if identity_ok:
                    note = "API 重启恢复：进程身份确认，终止旧进程组"
                elif members:
                    # 组长已死但组内仍有成员：组未空，pgid 不可能被复用，成员即残留
                    note = "API 重启恢复：组长已退出但组内仍有残留成员，强制清理"
                if note is None:
                    continue
                log_lines = [f"[runner] {note} build={build_id} step={step['step_uid']}"]
                cleared = await proc.terminate_process_group(
                    None, pgid, "重启清理", log_lines
                )
                killed_groups += 1
                bounded_append_log(step["log_path"], log_lines)
                if not cleared:
                    bounded_append_log(
                        step["log_path"], ["[runner] 警告: 重启清理超过硬等上限仍有残留"]
                    )

            for step in grouped.get(build_id, []):
                if step["status"] not in ("succeeded", "failed", "skipped", "cancelled"):
                    await self.db.set_step_status(
                        step["id"],
                        "interrupted",
                        _now(),
                        detail="API 进程重启，步骤被标记中断，不再启动",
                        # 重启前已确认的日志超限事实必须保留并体现在终态上
                        log_overflow=bool(step["log_overflow"]),
                    )
                    bounded_append_log(
                        step["log_path"], ["[runner] API 重启：该步骤标记为 interrupted"]
                    )
            await self.db.set_build_status(
                build_id,
                "interrupted",
                _now(),
                detail="API 进程重启，旧构建中断，未启动步骤不再执行",
                started=build["started_at"] is None,
                finished=True,
            )
        return {"builds": len(stale), "killed_groups": killed_groups}

    # ------------------------------------------------------------- 构建主循环

    async def _run_build(self, build_id: int) -> None:
        event = asyncio.Event()
        self._build_events[build_id] = event
        # (步骤 uid, 步骤任务) 成对保存，避免用任务名反解 uid
        step_tasks: list[tuple[str, asyncio.Task[str]]] = []
        try:
            await self.db.set_build_status(
                build_id, "running", _now(), started=True
            )
            rows = await self.db.get_steps(build_id)
            statuses = {row["step_uid"]: row["status"] for row in rows}
            by_id = {row["step_uid"]: row for row in rows}

            while True:
                # 先对账：已完成的步骤任务把终态写回内存视图，否则主循环
                # 会一直看到 "running" 而无法推进/退出
                for uid, task in step_tasks:
                    if task.done() and statuses[uid] not in _STEP_FAIL_STATES | {"succeeded"}:
                        try:
                            statuses[uid] = task.result()
                        except Exception as exc:  # 任务异常按失败处理
                            statuses[uid] = "failed"
                            bounded_append_log(
                                by_id[uid]["log_path"],
                                [f"[runner] 步骤任务异常: {exc!r}"],
                            )

                # 取消优先：未启动步骤不再启动，运行中的交给步骤任务自行终止
                if event.is_set():
                    for uid, st in statuses.items():
                        if st == "pending":
                            statuses[uid] = "cancelled"
                            await self.db.set_step_status(
                                by_id[uid]["id"],
                                "cancelled",
                                _now(),
                                detail="构建已取消，步骤未启动",
                            )
                    break

                progressed = False
                for uid, row in by_id.items():
                    if statuses[uid] != "pending":
                        continue
                    deps = json.loads(row["depends_on"])
                    dep_st = [statuses[d] for d in deps]
                    if any(s in _STEP_FAIL_STATES for s in dep_st):
                        statuses[uid] = "skipped"
                        await self.db.set_step_status(
                            row["id"],
                            "skipped",
                            _now(),
                            detail=f"上游步骤未成功，后代跳过: {deps}",
                        )
                        progressed = True
                        continue
                    # 依赖未全部结束则等下一轮；全部 succeeded 才启动
                    if any(s != "succeeded" for s in dep_st):
                        continue
                    task = asyncio.create_task(
                        self._run_step_with_slot(build_id, row),
                        name=f"build-{build_id}-step-{uid}",
                    )
                    step_tasks.append((uid, task))
                    statuses[uid] = "running"
                    progressed = True

                if all(st in _STEP_FAIL_STATES or st == "succeeded" for st in statuses.values()):
                    break
                # 没有新推进时睡眠等待；有推进也短暂让出，避免忙等
                await asyncio.sleep(config.SCHEDULER_TICK if not progressed else 0)

            # 等待所有在途步骤（含进程组清理）落终态后再确认构建终态
            results = await asyncio.gather(
                *(task for _, task in step_tasks), return_exceptions=True
            )
            for (uid, _), result in zip(step_tasks, results):
                if isinstance(result, Exception):
                    statuses[uid] = "failed"
                else:
                    statuses[uid] = result

            final_rows = await self.db.get_steps(build_id)
            final_statuses = [r["status"] for r in final_rows]
            if "failed" in final_statuses or "interrupted" in final_statuses:
                final = "failed"
                detail = "存在失败步骤"
            elif "cancelled" in final_statuses:
                final = "cancelled"
                detail = "构建被取消"
            else:
                final = "succeeded"
                detail = "全部步骤成功"
            await self.db.set_build_status(
                build_id, final, _now(), detail=detail, finished=True
            )
        finally:
            self._build_events.pop(build_id, None)
            self._build_tasks.pop(build_id, None)

    # ------------------------------------------------------------- 单步执行

    async def _run_step_with_slot(self, build_id: int, row: Any) -> str:
        """名额覆盖"启动 -> 运行 -> 进程组完全清空 -> 终态落库"全过程。"""
        async with self._slots:
            # 排队期间构建可能已被取消：取消优先，不得再启动新进程
            event = self._build_events.get(build_id)
            if event is not None and event.is_set():
                await self.db.set_step_status(
                    row["id"],
                    "cancelled",
                    _now(),
                    detail="构建已取消，步骤未启动",
                )
                return "cancelled"
            return await self._run_step(build_id, row)

    async def _run_step(self, build_id: int, row: Any) -> str:
        step_pk = row["id"]
        step_uid = row["step_uid"]
        command = row["command"]
        timeout = float(row["timeout_seconds"])
        workdir = config.WORKDIR_ROOT / str(build_id)
        workdir.mkdir(parents=True, exist_ok=True)
        log_path = Path(row["log_path"])
        # 步骤全部输出（stdout+stderr 合并）与 runner 元数据共用一个受上限写入器，
        # 保证日志文件任何时刻都不超过 PIPELINE_LOG_MAX_BYTES
        writer = BoundedLogWriter(str(log_path))
        try:
            for line in (
                f"[runner] build={build_id} step={step_uid} 开始",
                f"[runner] workdir={workdir}",
                f"[runner] command={command}",
                f"[runner] timeout={timeout}s",
                f"[runner] log_max_bytes={config.LOG_MAX_BYTES}",
            ):
                writer.append_line(line)

            env = {
                **os.environ,
                "PIPELINE_BUILD_ID": str(build_id),
                "PIPELINE_STEP_ID": step_uid,
            }
            try:
                process, pid, pgid, starttime = await proc.spawn(command, workdir, env)
            except Exception as exc:  # 命令本身都无法启动
                await self.db.set_step_status(
                    step_pk, "failed", _now(), detail=f"进程启动失败: {exc}"
                )
                writer.append_line(f"[runner] 进程启动失败: {exc}")
                return "failed"

            await self.db.set_step_process(step_pk, pid, pgid, starttime, _now())
            cancel_event = self._build_events[build_id]
            overflow_event = asyncio.Event()

            async def _pump_output() -> None:
                """读取合并后的 stdout/stderr 并受上限落盘。

                首次触达上限即置位 overflow_event（竞速终止进程组），
                并立即把超限事实落库，确保即使 API 重启也能识别该失败。
                """
                reader = process.stdout
                assert reader is not None
                while True:
                    chunk = await reader.read(64 * 1024)
                    if not chunk:  # EOF：管道随进程组所有写端关闭而结束
                        return
                    if writer.write(chunk):
                        # 标记已由写入器在文件内原位盖好，这里只负责落库与通知
                        await self.db.mark_log_overflow(step_pk)
                        overflow_event.set()

            pump_task = asyncio.create_task(
                _pump_output(), name=f"build-{build_id}-step-{step_uid}-log"
            )
            pump_error: Optional[BaseException] = None
            cleared = False
            had_leftovers = False
            exit_code: Optional[int] = None
            try:
                outcome, detail = await self._watch(
                    process, timeout, cancel_event, overflow_event
                )
                # 进入清理阶段即对外可见：终态尚未确认、名额仍占用
                await self.db.set_step_status(
                    step_pk, "cleaning_up", _now(), detail=detail
                )
                leftovers_before = await asyncio.to_thread(
                    proc.list_pgid_members, pgid
                )
                had_leftovers = bool(leftovers_before)
                cleanup_log: list[str] = []
                if outcome == "log_overflow":
                    reason = "日志超过落盘上限"
                elif outcome == "timeout":
                    reason = "步骤超时"
                elif outcome == "cancelled":
                    reason = "构建取消"
                else:
                    reason = "主命令退出后检查残留子进程"
                # 超限时进程可能仍在运行，统一走进程组终止流程，清空前不释放名额。
                # 必须先终止整个进程组：后台/遗留子进程持有管道写端，不先杀掉，
                # 输出泵的 EOF 永远不会到来（清理与抽干会互相等待）。
                cleared = await proc.terminate_process_group(
                    process, pgid, reason, cleanup_log
                )
                for line in cleanup_log:
                    writer.append_line(line)
                exit_code = process.returncode

                # 进程组已清空 -> 管道所有写端关闭，此时抽干管道内残留输出并
                # 复核日志是否超限。即使命令早已退出，未确认日志未超限时
                # 不会写入 succeeded，依赖也就不会被放行。
                pump_error = await _safe_cancel(pump_task)
            finally:
                # 异常路径（如清理阶段报错）也要兜底收掉输出泵，避免任务泄漏
                if not pump_task.done():
                    pump_task.cancel()
                    pump_error = await _safe_cancel(pump_task)
            overflowed = overflow_event.is_set()

            # 终态优先级：未清空 > 日志超限 > 超时 > 取消 > 遗留子进程 > 退出码
            if not cleared:
                status, final_detail = (
                    "failed",
                    f"{detail or ''}; 进程组在硬等上限内未清空".strip("; "),
                )
            elif overflowed:
                status = "failed"
                final_detail = (
                    f"{OVERFLOW_REASON}（上限 {config.LOG_MAX_BYTES} 字节）"
                )
            elif outcome == "timeout":
                status, final_detail = "failed", detail
            elif outcome == "cancelled":
                status, final_detail = "cancelled", detail
            elif had_leftovers:
                status = "failed"
                final_detail = (
                    f"主命令已退出(exit={exit_code})，但检测到遗留子进程，"
                    "进程组清理后按失败处理"
                )
            elif pump_error is not None:
                status = "failed"
                final_detail = f"日志读取异常: {pump_error!r}"
            elif exit_code == 0:
                status, final_detail = "succeeded", None
            else:
                status, final_detail = "failed", f"命令以非零状态退出: {exit_code}"

            await self.db.set_step_status(
                step_pk,
                status,
                _now(),
                exit_code=exit_code if isinstance(exit_code, int) else None,
                detail=final_detail,
                log_overflow=overflowed,
            )
            writer.append_line(
                f"[runner] 步骤终态={status} exit={exit_code} "
                f"遗留子进程={had_leftovers} 日志超限={overflowed} "
                f"detail={final_detail or '-'}"
            )
            return status
        finally:
            writer.close()

    async def _watch(
        self,
        process: Any,
        timeout: float,
        cancel_event: asyncio.Event,
        overflow_event: asyncio.Event,
    ) -> tuple[str, Optional[str]]:
        """四者竞速：进程退出 / 日志超限 / 取消 / 超时。

        超限、取消与超时都只负责发信号，实际的进程组终止统一由调用方在
        清理阶段完成；进程退出不代表成功，调用方仍要等待输出泵抽干并复核
        日志是否超限。
        """
        wait_task = asyncio.create_task(process.wait())
        cancel_task = asyncio.create_task(cancel_event.wait())
        timeout_task = asyncio.create_task(asyncio.sleep(timeout))
        overflow_task = asyncio.create_task(overflow_event.wait())
        try:
            await asyncio.wait(
                {wait_task, overflow_task, cancel_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if overflow_task.done():
                return (
                    "log_overflow",
                    f"日志达到落盘上限 {config.LOG_MAX_BYTES} 字节，终止整个进程组",
                )
            if wait_task.done():
                return "exited", None
            if cancel_task.done():
                return "cancelled", "构建已取消，终止整个进程组"
            return "timeout", f"步骤超过 {timeout}s 超时，终止整个进程组"
        finally:
            for task in (cancel_task, timeout_task, overflow_task):
                task.cancel()
            await asyncio.gather(
                cancel_task, timeout_task, overflow_task, return_exceptions=True
            )

    # ------------------------------------------------------------- 工具方法

    @staticmethod
    def _append_log(log_path: str, lines: list[str]) -> None:
        # 所有日志追加都必须经过上限管控，杜绝任何路径把文件写到上限之上
        bounded_append_log(log_path, lines)

    @staticmethod
    def _log_task_exception(task: asyncio.Task[Any]) -> None:
        if task.cancelled() or task.exception() is None:
            return
        # 构建任务内部已兜住大部分异常；这里仅留痕，避免静默吞掉
        import logging

        logging.getLogger("pipeline").exception(
            "构建任务异常: %s", task.exception()
        )


async def _safe_cancel(task: asyncio.Task[Any]) -> Optional[BaseException]:
    """收尾输出泵任务：取消并吞掉 CancelledError，返回真实异常（若有）。"""
    try:
        await task
    except asyncio.CancelledError:
        return None
    except BaseException as exc:  # 输出泵自身异常，交由调用方判失败
        return exc
    return None


def _safe_getpgid(pid: int) -> Optional[int]:
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None
