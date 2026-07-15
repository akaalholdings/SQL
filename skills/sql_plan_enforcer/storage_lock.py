#!/usr/bin/env python3
"""Backward-compatible imports for the uniquely named enforcer storage module."""

from enforcer_storage import (
    append_durable_text,
    atomic_write_text,
    exclusive_lock,
    secure_dir,
    secure_file,
)

__all__ = (
    "append_durable_text",
    "atomic_write_text",
    "exclusive_lock",
    "secure_dir",
    "secure_file",
)
