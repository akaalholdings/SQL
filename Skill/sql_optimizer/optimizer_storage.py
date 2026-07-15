#!/usr/bin/env python3
"""Private, serialized file storage helpers for sql_optimizer state."""

from __future__ import annotations

from contextlib import contextmanager
import os
import pathlib
import tempfile
from typing import Iterator

try:  # pragma: no cover - supported hosts are POSIX desktops
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


def _absolute(path: pathlib.Path | str) -> pathlib.Path:
    return pathlib.Path(os.path.abspath(pathlib.Path(path).expanduser()))


def _reject_symlink_components(path: pathlib.Path) -> None:
    current = pathlib.Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise OSError(f"refusing symlinked optimizer storage path: {current}")


def ensure_private_dir(path: pathlib.Path | str) -> pathlib.Path:
    """Create a state directory and tighten its mode if it already exists."""
    path = _absolute(path)
    _reject_symlink_components(path)
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    path.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIR_MODE)
    _reject_symlink_components(path)
    if not path.is_dir():
        raise OSError(f"optimizer storage path is not a directory: {path}")
    path.chmod(PRIVATE_DIR_MODE)
    for created in missing:
        if created.is_symlink() or not created.is_dir():
            raise OSError(f"optimizer storage path is not a directory: {created}")
        created.chmod(PRIVATE_DIR_MODE)
    return path


def secure_file(path: pathlib.Path | str) -> pathlib.Path:
    """Restrict an existing state file and reject symlinked paths."""
    path = _absolute(path)
    _reject_symlink_components(path)
    if path.exists() and not path.is_file():
        raise OSError(f"optimizer storage path is not a file: {path}")
    if path.exists():
        path.chmod(PRIVATE_FILE_MODE)
    return path


def _fsync_directory(path: pathlib.Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _open_private_append(path: pathlib.Path):
    path = secure_file(path)
    ensure_private_dir(path.parent)
    fd = os.open(
        path,
        os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
        PRIVATE_FILE_MODE,
    )
    os.fchmod(fd, PRIVATE_FILE_MODE)
    return os.fdopen(fd, "a", encoding="utf-8")


def append_text_line(path: pathlib.Path, text: str) -> None:
    """Append and flush one line; callers hold the store lock."""
    with _open_private_append(path) as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)


def atomic_write_text(path: pathlib.Path, text: str) -> None:
    """Atomically replace one private text file with restrictive permissions."""
    path = _absolute(path)
    ensure_private_dir(path.parent)
    secure_file(path)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = pathlib.Path(temporary_name)
    try:
        os.fchmod(fd, PRIVATE_FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        path.chmod(PRIVATE_FILE_MODE)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


@contextmanager
def exclusive_lock(root: pathlib.Path, name: str = ".lock") -> Iterator[None]:
    """Serialize one store's read-modify-write operations."""
    root = ensure_private_dir(root)
    lock_path = root / name
    secure_file(lock_path)
    fd = os.open(
        lock_path,
        os.O_CREAT | os.O_APPEND | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        PRIVATE_FILE_MODE,
    )
    os.fchmod(fd, PRIVATE_FILE_MODE)
    handle = os.fdopen(fd, "a+")
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
