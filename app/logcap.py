"""统一的落盘日志上限管控。

每个步骤的 stdout 与 stderr 合并写入同一个日志文件，全文件共享一个字节上限
``PIPELINE_LOG_MAX_BYTES``（默认 1 MiB）。本模块保证：

- 任何时刻磁盘上的日志文件大小都不会超过上限（包括 runner 元数据行）；
- 写满后命令的后续输出被丢弃，且 :class:`BoundedLogWriter` 会被置为超限，
  调度器据此停止该步骤的整个进程组并按失败处理；
- 超限原因以结构化标记随日志保存：超限时在文件末尾原位写入标记（若尾部
  剩余空间不足则覆写最后若干字节），任何上限配置下标记都落得下，重启后
  仅凭文件也能识别；同时超限事实持久化到 SQLite；
- 重启恢复等步骤生命周期之外的追加同样受上限约束。

文件始终紧凑（无预占填充、无空洞）：正常写入走 ``write`` 推进逻辑游标，
标记写入走 ``pwrite`` 原位覆盖，不改变文件大小。

注意：运行期写入都在调度器所在的 asyncio 事件循环线程内进行（输出泵与
``append_line`` 均为 loop 内协程/函数），因此实现不加锁；唯一的线程外写入
（重启恢复）使用进程内互斥锁保护。
"""
from __future__ import annotations

import os
import threading
from typing import Optional

from . import config

# 写入日志文件的结构化超限标记，重启后仅靠文件也能识别超限原因
OVERFLOW_MARKER = "[runner] 日志达到落盘上限，后续输出已丢弃"
# 数据库 detail / API reason 使用的规范文案
OVERFLOW_REASON = "日志输出超过落盘上限，进程组已终止并按失败处理"

_recovery_lock = threading.Lock()


def _write_all(fd: int, data: bytes) -> None:
    """os.write 可能短写，循环写完为止。"""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            break
        view = view[written:]


def _marker_bytes() -> bytes:
    return (OVERFLOW_MARKER + "\n").encode("utf-8")


class BoundedLogWriter:
    """带字节上限的追加写入器，句柄在步骤生命周期内复用。

    本类不实现 ``fileno``，因此不可能被直接当作子进程的 stdout/stderr；
    子进程输出经 asyncio PIPE 由输出泵读取后调用 :meth:`write` 落盘。
    """

    def __init__(self, path: str, limit: Optional[int] = None) -> None:
        self.path = path
        self.limit = max(1, config.LOG_MAX_BYTES if limit is None else int(limit))
        self._marker = _marker_bytes()
        # 不用 O_APPEND：标记需要 pwrite 原位覆盖，offset 全部显式管理
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            self._written = os.fstat(self._fd).st_size
        except OSError:
            self._written = 0
        # 重启场景：文件写到 cap 只会发生在超限路径（标记可能因 cap 极小
        # 被截断，因此不能只靠标记内容判断），cap 大小本身即超限信号。
        self.overflow = False
        self._full = False
        if self._written >= self.limit:
            self.overflow = True
            self._full = True
        # 后续 write 从文件末尾继续
        try:
            os.lseek(self._fd, 0, os.SEEK_END)
        except OSError:
            pass

    def _stamp_marker(self) -> None:
        """在文件末尾原位写入超限标记，保证文件大小仍不超过 cap。

        标记固定写在文件最后 ``len(stamp)`` 字节：放得下就追加到末尾，
        放不下就覆写最后若干字节（标记按空间截断）。两种方式下文件大小
        都不会超过 cap。
        """
        stamp = self._marker
        if len(stamp) > self.limit:  # cap 比标记还小：只写能容纳的前缀
            stamp = stamp[: self.limit]
        try:
            if self._written + len(stamp) <= self.limit:
                # 尾部空间充足：正常追加，文件增长但仍在 cap 内
                _write_all(self._fd, stamp)
                self._written += len(stamp)
            else:
                # 空间不足：原位覆写文件尾部，文件大小保持不变
                fit_len = min(len(stamp), self.limit)
                os.pwrite(self._fd, stamp[:fit_len], self.limit - fit_len)
        except OSError:
            pass

    def write(self, data: bytes) -> bool:
        """落盘命令输出中不超过上限的部分。

        返回 True 表示命令输出首次触达上限（调用方应据此触发超限处理）。
        即使 runner 元数据已先行把文件写满（极端小上限），命令的第一次
        写入尝试仍会返回 True；之后的数据一律丢弃并返回 False。
        """
        if not data or self.overflow:
            return False
        if self._full:
            # 元数据先行写满：命令输出无空间，按命令超限报告
            self.overflow = True
            self._stamp_marker()
            return True
        remaining = self.limit - self._written
        if remaining <= 0:
            self._full = True
            self.overflow = True
            self._stamp_marker()
            return True
        chunk = data[:remaining]
        try:
            _write_all(self._fd, chunk)
        except OSError:
            return False
        self._written += len(chunk)
        if self._written >= self.limit:
            self._full = True
            self.overflow = True
            self._stamp_marker()
            return True
        return False

    def append_line(self, line: str, limit: Optional[int] = None) -> None:
        """供 runner 元数据行使用的逐行追加，同样受上限约束。

        元数据自身写满只置内部 ``_full`` 标志，不提前宣告命令超限；
        命令输出路径（:meth:`write`）会在下次写入时报告。
        """
        if self._full:
            return
        data = (line + "\n").encode("utf-8", errors="replace")
        remaining = self.limit - self._written
        if remaining <= 0:
            self._full = True
            return
        chunk = data[:remaining]
        try:
            _write_all(self._fd, chunk)
            self._written += len(chunk)
            if self._written >= self.limit:
                self._full = True
        except OSError:
            return

    def close(self) -> None:
        try:
            os.close(self._fd)
        except OSError:
            pass


def bounded_append_log(log_path: str, lines: list[str], limit: Optional[int] = None) -> None:
    """步骤生命周期之外（主要是重启恢复）的安全追加。

    受同一上限约束：按文件当前大小计算剩余空间，逐行截断写入；写满后
    若标记尚不存在则原位补盖超限标记。保证任何路径都无法把日志文件写到
    ``PIPELINE_LOG_MAX_BYTES`` 之上。
    """
    cap = max(1, config.LOG_MAX_BYTES if limit is None else int(limit))
    marker = _marker_bytes()
    with _recovery_lock:
        try:
            fd = os.open(log_path, os.O_WRONLY | os.O_CREAT, 0o644)
        except OSError:
            return
        try:
            try:
                size = os.fstat(fd).st_size
            except OSError:
                size = 0
            if size >= cap:
                return
            os.lseek(fd, 0, os.SEEK_END)
            written = size
            for line in lines:
                remaining = cap - written
                if remaining <= 0:
                    break
                data = (line + "\n").encode("utf-8", errors="replace")
                chunk = data[:remaining]
                if not chunk:
                    break
                try:
                    _write_all(fd, chunk)
                except OSError:
                    break
                written += len(chunk)
            # 写满且文件中还没有标记时，原位补盖（覆盖最后若干字节）
            if written >= cap:
                try:
                    with open(log_path, "rb") as fh:
                        has_marker = marker in fh.read()
                except OSError:
                    has_marker = True
                if not has_marker:
                    stamp = marker if len(marker) <= cap else marker[:cap]
                    offset = max(0, cap - len(stamp))
                    try:
                        os.pwrite(fd, stamp, offset)
                    except OSError:
                        pass
        finally:
            os.close(fd)
