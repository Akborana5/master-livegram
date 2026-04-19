import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from telethon import Button, TelegramClient, events
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    InputUserDeactivatedError,
    PeerFloodError,
    UserIsBlockedError,
)
from telethon.tl.custom.message import Message

from app.config import Settings
from app.storage.hf_dataset import HFDataStore
from app.utils.media import send_payload, serialize_message


class AssistantRuntime:
    MIN_ELAPSED_TIME_SECONDS = 0.1

    def __init__(
        self,
        settings: Settings,
        store: HFDataStore,
        assistant_id: str,
        session_path: str,
    ) -> None:
        self.settings = settings
        self.store = store
        self.assistant_id = assistant_id
        self.client = TelegramClient(session_path, settings.api_id, settings.api_hash)
        self.pending_actions: dict[int, dict[str, Any]] = {}

    @property
    def data(self) -> dict[str, Any]:
        return self.store.get_data()["assistants"][self.assistant_id]

    def _admins(self) -> set[int]:
        admins = set(self.data.get("admins", []))
        admins.add(self.settings.super_admin_id)
        admins.add(self.data["owner_id"])
        return admins

    def _is_admin(self, user_id: int) -> bool:
        return user_id in self._admins()

    async def start(self) -> None:
        await self.client.start()
        self._register_handlers()

    async def stop(self) -> None:
        await self.client.disconnect()

    def _register_handlers(self) -> None:
        self.client.add_event_handler(self._on_new_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self._on_callback, events.CallbackQuery)

    def _ensure_user(self, user_id: int, premium: bool) -> dict[str, Any]:
        users = self.data.setdefault("users", {})
        user_key = str(user_id)
        now = datetime.now(timezone.utc).isoformat()
        if user_key not in users:
            users[user_key] = {
                "premium": premium,
                "blocked": False,
                "first_seen": now,
                "last_seen": now,
                "start_count": 0,
                "message_count": 0,
            }
        users[user_key]["premium"] = premium
        users[user_key]["last_seen"] = now
        return users[user_key]

    async def _log_user_message(self, event: events.NewMessage.Event, user_id: int) -> None:
        target = self.data.get("log_chat_id") or self.data["owner_id"]
        header = f"📩 Assistant {self.assistant_id}\nFrom: `{user_id}`"
        await self.client.send_message(target, header)
        forwarded = await event.message.forward_to(target)
        reply_map = self.data.setdefault("reply_map", {})
        reply_map[str(forwarded.id)] = user_id
        self.store.mark_dirty()

    async def _apply_auto_reply(self, user_id: int, is_start: bool) -> None:
        payload = self.data.get("start_post") if is_start else self.data.get("setmsg")
        if payload:
            await send_payload(self.client, user_id, payload)

    async def _handle_admin_command(self, event: events.NewMessage.Event) -> bool:
        text = (event.raw_text or "").strip()
        user_id = event.sender_id or 0
        if not text.startswith("/"):
            return False

        parts = text.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if command == "/menu":
            await event.respond(
                "Assistant Menu",
                buttons=[
                    [Button.inline("SET START POST", data=f"asetstart:{self.assistant_id}")],
                    [Button.inline("SET MESSAGE", data=f"asetmsg:{self.assistant_id}")],
                    [Button.inline("STATS", data=f"astats:{self.assistant_id}")],
                    [Button.inline("BROADCAST", data=f"abroadcast:{self.assistant_id}")],
                ],
            )
            return True

        if command in {"/ban", "/unban", "/promote", "/demote"} and arg.isdigit():
            target = int(arg)
            if command == "/ban":
                self.data.setdefault("blocked_users", [])
                if target not in self.data["blocked_users"]:
                    self.data["blocked_users"].append(target)
                user_data = self._ensure_user(target, False)
                user_data["blocked"] = True
            elif command == "/unban":
                self.data["blocked_users"] = [x for x in self.data.get("blocked_users", []) if x != target]
                user_data = self._ensure_user(target, False)
                user_data["blocked"] = False
            elif command == "/promote":
                self.data.setdefault("admins", [])
                if target not in self.data["admins"]:
                    self.data["admins"].append(target)
            elif command == "/demote":
                self.data["admins"] = [x for x in self.data.get("admins", []) if x != target]
            self.store.mark_dirty()
            await event.reply("Updated.")
            return True

        if command in {"/ban", "/unban", "/promote", "/demote"}:
            await event.reply("Usage: /ban <id>, /unban <id>, /promote <id>, /demote <id>")
            return True

        return False

    async def _handle_reply_bridge(self, event: events.NewMessage.Event) -> bool:
        if not event.is_reply:
            return False
        if event.chat_id != (self.data.get("log_chat_id") or self.data["owner_id"]):
            return False

        reply = await event.get_reply_message()
        if not reply:
            return False

        user_id = self.data.get("reply_map", {}).get(str(reply.id))
        if not user_id:
            return False

        payload = await serialize_message(self.client, event.message)
        await send_payload(self.client, user_id, payload)
        return True

    async def _handle_pending_action(self, event: events.NewMessage.Event) -> bool:
        action = self.pending_actions.get(event.sender_id or 0)
        if not action:
            return False

        if (event.raw_text or "").strip().lower() == "cancel":
            self.pending_actions.pop(event.sender_id or 0, None)
            await event.reply("Cancelled.")
            return True

        if action["type"] in {"set_start", "set_msg"}:
            payload = await serialize_message(self.client, event.message)
            if action["type"] == "set_start":
                self.data["start_post"] = payload
            else:
                self.data["setmsg"] = payload
            self.store.mark_dirty()
            self.pending_actions.pop(event.sender_id or 0, None)
            await event.reply("Saved.")
            return True

        if action["type"] == "broadcast_prepare":
            payload = await serialize_message(self.client, event.message)
            self.pending_actions[event.sender_id or 0] = {"type": "broadcast_confirm", "payload": payload}
            await event.reply(
                "Are you sure you want to broadcast to all users?",
                buttons=[
                    [Button.inline("YES", data=f"abcyes:{self.assistant_id}")],
                    [Button.inline("CANCEL", data=f"abcancel:{self.assistant_id}")],
                ],
            )
            return True

        return False

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        if event.sender_id is None:
            return

        self.data["last_active_at"] = datetime.now(timezone.utc).isoformat()
        self.store.mark_dirty()

        if self._is_admin(event.sender_id):
            if await self._handle_pending_action(event):
                return
            if await self._handle_admin_command(event):
                return
            await self._handle_reply_bridge(event)
            return

        if not event.is_private:
            return

        if event.sender_id in self.data.get("blocked_users", []):
            return

        sender = await event.get_sender()
        premium = bool(getattr(sender, "premium", False))
        user_data = self._ensure_user(event.sender_id, premium)
        user_data["message_count"] += 1
        self.data.setdefault("stats", {"total_starts": 0, "total_messages": 0})
        self.data["stats"]["total_messages"] += 1

        is_start = (event.raw_text or "").strip().startswith("/start")
        if is_start:
            user_data["start_count"] += 1
            self.data["stats"]["total_starts"] += 1

        self.store.mark_dirty()

        await self._log_user_message(event, event.sender_id)
        await self._apply_auto_reply(event.sender_id, is_start)

    def _stats_text(self) -> str:
        users = self.data.get("users", {})
        total = len(users)
        premium = sum(1 for u in users.values() if u.get("premium"))
        blocked = len(self.data.get("blocked_users", []))
        non_premium = total - premium
        admins = sorted(self._admins())
        stats = self.data.get("stats", {})
        return (
            f"Total users: {total}\n"
            f"Premium users: {premium}\n"
            f"Non-premium users: {non_premium}\n"
            f"Blocked users: {blocked}\n"
            f"Total admins: {len(admins)}\n"
            f"Admin IDs: {', '.join(map(str, admins))}\n"
            f"Total /start count: {stats.get('total_starts', 0)}\n"
            f"Total messages count: {stats.get('total_messages', 0)}"
        )

    async def _broadcast(self, admin_id: int, payload: dict[str, Any], status_msg: Message) -> None:
        users = [int(x) for x in self.data.get("users", {}).keys() if int(x) not in self.data.get("blocked_users", [])]
        total = len(users)
        if total == 0:
            await status_msg.edit("No users to broadcast.")
            return

        success = failed = blocked = 0
        started = time.time()
        checkpoints = {25, 50, 75, 100}
        completed_marks: set[int] = set()

        for index, user_id in enumerate(users, start=1):
            try:
                await send_payload(self.client, user_id, payload)
                success += 1
                await asyncio.sleep(0.05)
            except FloodWaitError as e:
                await asyncio.sleep(max(1, int(e.seconds)))
                try:
                    await send_payload(self.client, user_id, payload)
                    success += 1
                except Exception:
                    failed += 1
            except (UserIsBlockedError, InputUserDeactivatedError, ChatWriteForbiddenError):
                blocked += 1
                failed += 1
                if user_id not in self.data.setdefault("blocked_users", []):
                    self.data["blocked_users"].append(user_id)
                    self.store.mark_dirty()
            except PeerFloodError:
                await asyncio.sleep(2)
                failed += 1
            except Exception:
                failed += 1

            progress = int(index / total * 100)
            hit = [x for x in checkpoints if progress >= x and x not in completed_marks]
            if hit:
                mark = max(hit)
                completed_marks.add(mark)
                elapsed = max(time.time() - started, self.MIN_ELAPSED_TIME_SECONDS)
                speed = index / elapsed
                remaining = total - index
                eta = int(remaining / speed)
                await status_msg.edit(
                    f"{mark}% completed\n"
                    f"Sent: {index}/{total}\n"
                    f"ETA: {eta}s\n"
                    f"Speed: {speed:.2f} msg/sec"
                )

        duration = int(time.time() - started)
        self.store.mark_dirty()
        await status_msg.edit(
            "Broadcast Completed ✅\n\n"
            f"Total Users: {total}\n"
            f"Sent: {success}\n"
            f"Failed: {failed}\n"
            f"Blocked: {blocked}\n"
            f"Time Taken: {duration} seconds"
        )

    async def _on_callback(self, event: events.CallbackQuery.Event) -> None:
        sender_id = event.sender_id or 0
        if not self._is_admin(sender_id):
            await event.answer("Not allowed", alert=True)
            return

        data = (event.data or b"").decode("utf-8")
        if data == f"asetstart:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "set_start"}
            await event.edit("Send STARTPOST message now, or type Cancel.")
            return

        if data == f"asetmsg:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "set_msg"}
            await event.edit("Send SETMSG message now, or type Cancel.")
            return

        if data == f"astats:{self.assistant_id}":
            await event.edit("Processing...")
            await event.edit(self._stats_text())
            return

        if data == f"abroadcast:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "broadcast_prepare"}
            await event.edit("Send broadcast message now (text/media), or type Cancel.")
            return

        if data == f"abcancel:{self.assistant_id}":
            self.pending_actions.pop(sender_id, None)
            await event.edit("Broadcast cancelled.")
            return

        if data == f"abcyes:{self.assistant_id}":
            action = self.pending_actions.get(sender_id)
            if not action or action.get("type") != "broadcast_confirm":
                await event.answer("No pending broadcast", alert=True)
                return
            payload = action["payload"]
            self.pending_actions.pop(sender_id, None)
            msg = await event.edit("Broadcast started...")
            await self._broadcast(sender_id, payload, msg)
            return
