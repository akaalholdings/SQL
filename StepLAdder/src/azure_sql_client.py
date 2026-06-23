from __future__ import annotations

import re
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass

from azure.mgmt.sql import SqlManagementClient


@dataclass(frozen=True)
class DatabaseCompute:
    location: str
    sku_name: str
    sku_tier: str
    sku_family: str | None
    vcores: int


class AzureSqlDatabaseClient:
    def __init__(
        self,
        *,
        subscription_id: str,
        credential,
        resource_group: str,
        server_name: str,
        database_name: str,
    ) -> None:
        self._resource_group = resource_group
        self._server_name = server_name
        self._database_name = database_name
        self._client = SqlManagementClient(credential=credential, subscription_id=subscription_id)

    def get_database_compute(self) -> DatabaseCompute:
        db = self._client.databases.get(
            resource_group_name=self._resource_group,
            server_name=self._server_name,
            database_name=self._database_name,
        )

        if db.sku is None or db.sku.capacity is None:
            raise RuntimeError("Database SKU metadata is missing; expected vCore-based Azure SQL database.")

        return DatabaseCompute(
            location=db.location,
            sku_name=db.sku.name,
            sku_tier=db.sku.tier,
            sku_family=getattr(db.sku, "family", None),
            vcores=int(db.sku.capacity),
        )

    def request_scale(self, *, current: DatabaseCompute, target_vcores: int, wait_seconds: int = 30) -> None:
        if target_vcores < 1:
            raise ValueError("target_vcores must be >= 1")

        target_sku_name = _with_target_capacity_in_sku_name(current.sku_name, target_vcores)

        sku_payload = {
            "name": target_sku_name,
            "tier": current.sku_tier,
            "capacity": int(target_vcores),
        }
        if current.sku_family:
            sku_payload["family"] = current.sku_family

        poller = self._client.databases.begin_update(
            resource_group_name=self._resource_group,
            server_name=self._server_name,
            database_name=self._database_name,
            parameters={"sku": sku_payload},
        )

        if wait_seconds <= 0:
            return

        try:
            poller.result(timeout=wait_seconds)
        except (FutureTimeoutError, TimeoutError):
            return


def _with_target_capacity_in_sku_name(current_sku_name: str, target_vcores: int) -> str:
    match = re.match(r"^(.*_)(\d+)$", current_sku_name)
    if not match:
        return current_sku_name
    return f"{match.group(1)}{target_vcores}"
