import asyncio
import base64
from pathlib import Path

from app.bots.master import MasterController
from app.config import Settings
from app.session_manager import SessionManager
from app.storage.hf_dataset import HFDataStore


async def ensure_master_session(settings: Settings, store: HFDataStore) -> None:
    session_file = Path(settings.master_session_file)
    master = store.get_data().setdefault("master", {})

    if session_file.exists():
        master["session_b64"] = base64.b64encode(session_file.read_bytes()).decode("utf-8")
        store.mark_dirty()
        return

    raw_b64 = master.get("session_b64")
    if raw_b64:
        session_file.write_bytes(base64.b64decode(raw_b64.encode("utf-8")))


async def main() -> None:
    settings = Settings.from_env()
    if not settings.api_id or not settings.api_hash:
        raise RuntimeError("API_ID and API_HASH are required")
    if not settings.hf_token or not settings.hf_repo_id:
        raise RuntimeError("HF_TOKEN and HF_REPO_ID are required")

    store = HFDataStore(settings)
    await store.initialize()
    await ensure_master_session(settings, store)

    sessions = SessionManager(settings, store)
    await sessions.load_all()

    master = MasterController(settings, store, sessions)
    await master.start()
    await store.start_auto_sync()

    try:
        await master.run()
    finally:
        await store.sync(force=True)
        await store.stop_auto_sync()
        await sessions.shutdown()
        await master.stop()


if __name__ == "__main__":
    asyncio.run(main())
