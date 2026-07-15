#!/usr/bin/env python3
"""Small POSIX file-lock helper for the enforcer's durable state files."""

from __future__ import annotations

from contextlib import contextmanager
import os
import pathlib
import tempfile
from typing import Iterator

try:  # pragma: no cover - supported desktop hosts are POSIX
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

DIRECTORY_MODE = 0o700
FILE_MODE = 0o600


def _absolute(path: pathlib.Path | str) -> pathlib.Path:
    return pathlib.Path(os.path.abspath(pathlib.Path(path).expanduser()))


def _reject_symlink_components(path: pathlib.Path) -> None:
    current = pathlib.Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise OSError(f"refusing symlinked enforcer storage path: {current}")


def secure_dir(path: pathlib.Path | str) -> pathlib.Path:
    """Create a store directory and restrict it to its owner."""
    path = _absolute(path)
    _reject_symlink_components(path)
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    path.mkdir(parents=True, exist_ok=True)
    _reject_symlink_components(path)
    if not path.is_dir():
        raise OSError(f"enforcer storage path is not a directory: {path}")
    os.chmod(path, DIRECTORY_MODE)
    for created in missing:
        if created.is_symlink() or not created.is_dir():
            raise OSError(f"enforcer storage path is not a directory: {created}")
        os.chmod(created, DIRECTORY_MODE)
    return path


def secure_file(path: pathlib.Path | str) -> pathlib.Path:
    """Restrict an existing enforcer file and reject symlinked state."""
    path = _absolute(path)
    _reject_symlink_components(path)
    if path.exists() and not path.is_file():
        raise OSError(f"enforcer storage path is not a file: {path}")
    if path.exists():
        os.chmod(path, FILE_MODE)
    return path


def _fsync_dir(path: pathlib.Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError:
            pass
    finally:
        os.close(fd)


def atomic_write_text(path: pathlib.Path | str, text: str) -> None:
    """Durably replace one owner-only file in the same directory."""
    path = _absolute(path)
    secure_dir(path.parent)
    secure_file(path)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        os.fchmod(fd, FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, FILE_MODE)
        _fsync_dir(path.parent)
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def append_durable_text(path: pathlib.Path | str, text: str) -> None:
    """Append and fsync an owner-only event-log record."""
    path = _absolute(path)
    secure_dir(path.parent)
    secure_file(path)
    fd = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        FILE_MODE,
    )
    try:
        os.fchmod(fd, FILE_MODE)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_dir(path.parent)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


@contextmanager
def exclusive_lock(root: pathlib.Path, name: str = ".lock") -> Iterator[None]:
    """Serialize one store's read-modify-write operations."""
    root = secure_dir(root)
    lock_path = root / name
    secure_file(lock_path)
    fd = os.open(
        str(lock_path),
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        FILE_MODE,
    )
    os.fchmod(fd, FILE_MODE)
    with os.fdopen(fd, "a+", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
