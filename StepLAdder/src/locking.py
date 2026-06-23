from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient


@contextmanager
def try_acquire_singleflight_lock(
    *,
    connection_string: str,
    container_name: str,
    blob_name: str,
    lease_seconds: int,
) -> Iterator[bool]:
    service = BlobServiceClient.from_connection_string(connection_string)
    container_client = service.get_container_client(container=container_name)

    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    blob_client = container_client.get_blob_client(blob=blob_name)
    try:
        blob_client.upload_blob(data=b"", overwrite=False)
    except ResourceExistsError:
        pass

    lease = None
    acquired = False
    try:
        lease = blob_client.acquire_lease(lease_duration=lease_seconds)
        acquired = True
        yield True
    except HttpResponseError as exc:
        if exc.status_code != 409:
            raise
        yield False
    finally:
        if acquired and lease is not None:
            try:
                lease.release()
            except Exception as exc:  # pragma: no cover
                logging.warning("Failed releasing blob lease lock: %s", exc)
