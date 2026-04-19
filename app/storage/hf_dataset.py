import asyncio
import copy
import json
import tempfile
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError

from app.config import Settings


class HFDataStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.api = HfApi(token=settings.hf_token)
        self.data: dict[str, Any] = self._default_data()
        self._lock = asyncio.Lock()
        self._dirty = False
        self._auto_sync_task: asyncio.Task | None = None

    @staticmethod
    def _default_data() -> dict[str, Any]:
        return {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "master": {
                "users": {},
                "admins": [],
                "banned": [],
                "stats": {"total_starts": 0, "total_messages": 0},
                "session_b64": "",
            },
            "assistants": {},
        }

    async def initialize(self) -> None:
        await self._ensure_repo()
        await self.load()

    async def _ensure_repo(self) -> None:
        def _create() -> None:
            self.api.create_repo(
                repo_id=self.settings.hf_repo_id,
                repo_type="dataset",
                exist_ok=True,
                token=self.settings.hf_token,
            )

        await asyncio.to_thread(_create)

    async def load(self) -> None:
        async with self._lock:
            try:
                local = await asyncio.to_thread(
                    hf_hub_download,
                    repo_id=self.settings.hf_repo_id,
                    repo_type="dataset",
                    filename=self.settings.hf_data_path,
                    token=self.settings.hf_token,
                )
                with open(local, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except (EntryNotFoundError, RepositoryNotFoundError, FileNotFoundError):
                self.data = self._default_data()
                self._dirty = True
                await self.sync()

    def get_data(self) -> dict[str, Any]:
        return self.data

    def get_snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self.data)

    def mark_dirty(self) -> None:
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._dirty = True

    async def sync(self, force: bool = False) -> None:
        async with self._lock:
            if not force and not self._dirty:
                return

            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json", encoding="utf-8") as tf:
                json.dump(self.data, tf, ensure_ascii=False, indent=2)
                temp_path = tf.name

            def _upload() -> None:
                self.api.upload_file(
                    path_or_fileobj=temp_path,
                    path_in_repo=self.settings.hf_data_path,
                    repo_id=self.settings.hf_repo_id,
                    repo_type="dataset",
                    token=self.settings.hf_token,
                    commit_message="sync database",
                )

            try:
                await asyncio.to_thread(_upload)
                self._dirty = False
            finally:
                Path(temp_path).unlink(missing_ok=True)

    async def start_auto_sync(self) -> None:
        if self._auto_sync_task and not self._auto_sync_task.done():
            return

        async def _runner() -> None:
            while True:
                await asyncio.sleep(self.settings.auto_sync_interval)
                try:
                    await self.sync()
                except Exception:
                    logging.exception("Auto-sync failed for repo %s", self.settings.hf_repo_id)

        self._auto_sync_task = asyncio.create_task(_runner())

    async def stop_auto_sync(self) -> None:
        if self._auto_sync_task and not self._auto_sync_task.done():
            self._auto_sync_task.cancel()
            try:
                await self._auto_sync_task
            except asyncio.CancelledError:
                pass
