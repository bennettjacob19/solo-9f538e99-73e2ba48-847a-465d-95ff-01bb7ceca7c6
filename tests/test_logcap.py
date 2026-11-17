"""落盘日志上限管控的单元测试（BoundedLogWriter / bounded_append_log）。"""
from __future__ import annotations

from app.logcap import (
    OVERFLOW_MARKER,
    BoundedLogWriter,
    bounded_append_log,
)


def test_writer_never_exceeds_limit_and_marks_overflow(tmp_path):
    path = tmp_path / "step.log"
    writer = BoundedLogWriter(str(path), limit=200)
    assert path.stat().st_size == 0  # 紧凑文件：未写入时大小为 0
    assert writer.overflow is False
    first = False
    for i in range(50):
        if writer.write(f"line-{i:02d}-".encode()):  # 每块 9 字节
            first = True
            break
    assert first is True  # 某一次写入首次触顶
    assert writer.overflow is True
    # 触顶后继续写一律丢弃
    assert writer.write(b"more-data") is False
    writer.append_line("runner note")
    writer.close()

    assert path.stat().st_size <= 200
    raw = path.read_bytes()
    assert len(raw) <= 200
    assert OVERFLOW_MARKER.encode("utf-8") in raw  # 标记已随文件保存


def test_writer_persists_overflow_across_reopen(tmp_path):
    path = tmp_path / "step.log"
    writer = BoundedLogWriter(str(path), limit=128)
    hit = writer.write(b"x" * 1000)
    assert hit is True and writer.overflow is True
    writer.close()
    assert path.stat().st_size == 128

    # 模拟重启：重新打开同一文件，写入器应识别出已超限并拒绝再写
    reopened = BoundedLogWriter(str(path), limit=128)
    assert reopened.overflow is True
    assert reopened.write(b"y") is False
    reopened.close()
    assert path.stat().st_size == 128


def test_append_line_is_cap_aware(tmp_path):
    path = tmp_path / "step.log"
    writer = BoundedLogWriter(str(path), limit=100)
    writer.write(b"a" * 40)
    writer.append_line("1234567890")  # 只能占用剩余空间
    writer.close()
    assert path.stat().st_size <= 100


def test_tiny_cap_still_detects_overflow(tmp_path):
    # 上限只有 1 字节：任何输出都必须能被识别为超限，且文件不超 1 字节
    path = tmp_path / "tiny.log"
    writer = BoundedLogWriter(str(path), limit=1)
    assert writer.write(b"hello") is True
    assert writer.overflow is True
    writer.close()
    assert path.stat().st_size == 1

    reopened = BoundedLogWriter(str(path), limit=1)
    assert reopened.overflow is True
    reopened.close()
    assert path.stat().st_size == 1


def test_metadata_fills_cap_then_command_output_still_reports(tmp_path):
    # runner 元数据先行写满（极端小上限），命令的第一次输出仍须报告超限
    path = tmp_path / "meta-full.log"
    writer = BoundedLogWriter(str(path), limit=20)
    writer.append_line("[runner] build=1 step=a 开始-这行足够长到写满二十字节")
    assert writer.overflow is False  # 元数据写满本身不算命令超限
    assert writer._full is True
    assert writer.write(b"command-output") is True  # 命令输出首次报告
    assert writer.overflow is True
    assert writer.write(b"more") is False  # 之后丢弃
    writer.close()
    assert path.stat().st_size <= 20


def test_normal_content_kept_intact_under_cap(tmp_path):
    path = tmp_path / "normal.log"
    writer = BoundedLogWriter(str(path), limit=1024)
    assert writer.write(b"important-output\n") is False
    writer.append_line("[runner] done")
    writer.close()
    raw = path.read_bytes()
    assert b"important-output" in raw
    assert b"[runner] done" in raw
    assert len(raw) < 1024  # 未超限：文件紧凑，不会被撑到 cap


def test_bounded_append_log_respects_cap(tmp_path):
    path = tmp_path / "recover.log"
    bounded_append_log(str(path), ["a" * 30, "b" * 30], limit=40)
    assert path.stat().st_size <= 40
    # 已写满后再追加也不会突破上限（重启恢复路径）
    bounded_append_log(str(path), ["[runner] API 重启"], limit=40)
    assert path.stat().st_size <= 40


def test_bounded_append_log_creates_file(tmp_path):
    path = tmp_path / "new.log"
    bounded_append_log(str(path), ["hello"], limit=1024)
    assert b"hello" in path.read_bytes()


def test_on_disk_size_never_exceeds_cap_across_writes(tmp_path):
    # 覆盖：远大于/接近/小于标记长度的多种 cap，以及变长写入
    marker_len = len((OVERFLOW_MARKER + "\n").encode("utf-8"))
    for cap in (1, 2, 10, marker_len - 1, marker_len, marker_len + 5, 500):
        path = tmp_path / f"cap-{cap}.log"
        writer = BoundedLogWriter(str(path), limit=cap)
        writer.append_line("[runner] 开始元数据行")
        sizes = [path.stat().st_size]
        for i in range(200):
            writer.write(f"output-block-{i}-".encode() * (1 + i % 7))
            sizes.append(path.stat().st_size)
        writer.append_line("[runner] 终态元数据行")
        writer.close()
        final = path.stat().st_size
        assert final <= cap, f"cap={cap} 文件大小 {final} 超限"
        assert all(s <= cap for s in sizes), f"cap={cap} 过程中出现超限: {sizes}"
        assert writer.overflow is True
        # 重开仍识别
        assert BoundedLogWriter(str(path), limit=cap).overflow is True
