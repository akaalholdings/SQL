#!/usr/bin/env python3
"""Backward-compatible imports for the uniquely named optimizer storage module."""

from optimizer_storage import (
    append_text_line,
    atomic_write_text,
    ensure_private_dir,
    exclusive_lock,
    secure_file,
)

__all__ = (
    "append_text_line",
    "atomic_write_text",
    "ensure_private_dir",
    "exclusive_lock",
    "secure_file",
)
