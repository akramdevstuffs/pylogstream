"""Async wrapper around kazoo (synchronous ZooKeeper client).

All blocking kazoo calls are offloaded via asyncio.to_thread so the event
loop is never blocked.
"""

import asyncio
import json
import logging
from typing import Any, Callable, Awaitable

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

logger = logging.getLogger(__name__)

# Re-export kazoo exceptions so callers don't import kazoo directly.
__all__ = ["ZKClient", "NodeExistsError", "NoNodeError"]


class ZKClient:
    """Thin async wrapper around KazooClient."""

    def __init__(self, hosts: str):
        self._hosts = hosts
        self._zk: KazooClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._zk = KazooClient(hosts=self._hosts)
        await asyncio.to_thread(self._zk.start)
        logger.info("ZooKeeper connected to %s", self._hosts)

    async def close(self) -> None:
        if self._zk:
            await asyncio.to_thread(self._zk.stop)
            await asyncio.to_thread(self._zk.close)
            logger.info("ZooKeeper connection closed")

    # ------------------------------------------------------------------
    # Basic node operations
    # ------------------------------------------------------------------

    async def ensure_path(self, path: str) -> None:
        """Create all nodes in path that don't exist yet (like mkdir -p)."""
        await asyncio.to_thread(self._zk.ensure_path, path)

    async def create(self, path: str, data: Any, ephemeral: bool = False) -> str:
        """Create a node. data is JSON-serialised automatically."""
        raw = json.dumps(data).encode()
        return await asyncio.to_thread(self._zk.create, path, raw, ephemeral=ephemeral)

    async def create_ephemeral(self, path: str, data: Any) -> str:
        """Create an ephemeral node; raises NodeExistsError if it already exists."""
        return await self.create(path, data, ephemeral=True)

    async def set(self, path: str, data: Any) -> None:
        """Overwrite the data of an existing node."""
        raw = json.dumps(data).encode()
        await asyncio.to_thread(self._zk.set, path, raw)

    async def get(self, path: str) -> Any | None:
        """Return the JSON-decoded value of a node, or None if it doesn't exist."""
        try:
            raw, _ = await asyncio.to_thread(self._zk.get, path)
            return json.loads(raw.decode()) if raw else None
        except NoNodeError:
            return None

    async def get_children(self, path: str) -> list[str]:
        """Return the children of a node, or [] if the node doesn't exist."""
        try:
            return await asyncio.to_thread(self._zk.get_children, path)
        except NoNodeError:
            return []

    async def exists(self, path: str) -> bool:
        stat = await asyncio.to_thread(self._zk.exists, path)
        return stat is not None

    async def delete(self, path: str) -> None:
        """Delete a node. Silently ignores NoNodeError."""
        try:
            await asyncio.to_thread(self._zk.delete, path)
        except NoNodeError:
            pass

    # ------------------------------------------------------------------
    # Watcher helpers
    # ------------------------------------------------------------------

    def watch_data(self, path: str, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a one-shot watcher that fires *callback* when node data changes or is deleted.

        The callback is scheduled on the running event loop so it can safely
        call async methods.
        """
        loop = asyncio.get_event_loop()

        def _watcher(event):
            asyncio.run_coroutine_threadsafe(callback(), loop)

        # `watch` parameter registers the one-shot watcher in kazoo.
        self._zk.get(path, watch=_watcher)

    def watch_exists(self, path: str, callback: Callable[[], Awaitable[None]]) -> None:
        """Fire *callback* when the node is created or deleted."""
        loop = asyncio.get_event_loop()

        def _watcher(event):
            asyncio.run_coroutine_threadsafe(callback(), loop)

        self._zk.exists(path, watch=_watcher)
