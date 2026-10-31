"""Human-like reply engine for AstrBot.

Splits a long reply at punctuation only (never by character count), sends the
pieces one by one with a typing rhythm, and never drops queued sentences when
the user sends another message (it just speeds the remaining ones up).

Features
--------
- Only replies with more than ``split_over_sentences`` sentences are split.
- Sentences longer than ``long_sentence_threshold`` are additionally cut at
  commas; short fragments are merged back so bubbles never feel choppy.
- Trailing ``。`` is stripped for a natural chat feel (``？！～`` are kept).
- Between pieces the bot tries to report a "typing" state via the OneBot
  ``set_input_status`` family of actions (silently ignored when unsupported).
- Errors are never surfaced to the user; sending failures stay in the log.
"""

import asyncio
import random
import re

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain

SPLIT_RE = re.compile(r"[^。！？!?~～…\n]+[。！？!?~～…\n]*")
COMMA_RE = re.compile(r"[，,、；;：:]")


@register(
    "astrbot_plugin_human_like_engine",
    "dsh",
    "按标点把长回复切成几句逐条发送，带打字节奏与插话处理",
    "0.2.0",
)
class HumanLikeEngine(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.conf = config or {}
        self._queues: dict[str, asyncio.Queue] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._interrupted: dict[str, bool] = {}

    def _opt(self, key, default):
        try:
            value = self.conf.get(key, default)
        except Exception:
            return default
        return default if value is None else value

    # ---------- splitting ----------
    def _no_period(self, s: str) -> str:
        if not bool(self._opt("strip_period", True)):
            return s
        out = s.rstrip()
        while out.endswith("。"):
            out = out[:-1].rstrip()
        return out if out else s

    def _split(self, text: str) -> list[str]:
        text = text.strip()
        if not text:
            return []
        parts = [p.strip() for p in SPLIT_RE.findall(text) if p.strip()]
        if not parts:
            return [self._no_period(text)]

        long_threshold = int(self._opt("long_sentence_threshold", 20))
        expanded: list[str] = []
        for part in parts:
            if len(part) > long_threshold and COMMA_RE.search(part):
                expanded.extend(
                    x.strip()
                    for x in re.split(r"(?<=[，,、；;：:])", part)
                    if x.strip()
                )
            else:
                expanded.append(part)

        merge_min = int(self._opt("merge_min_chars", 6))
        merged: list[str] = []
        pending = ""
        for i, piece in enumerate(expanded):
            piece = pending + piece
            pending = ""
            if len(piece) < merge_min and i < len(expanded) - 1:
                pending = piece
                continue
            merged.append(piece)
        if pending:
            if merged:
                merged[-1] += pending
            else:
                merged.append(pending)

        if len(merged) <= int(self._opt("split_over_sentences", 2)):
            return [self._no_period(text)]
        return [self._no_period(x) for x in merged]

    # ---------- typing state ----------
    async def _typing(self, event: AstrMessageEvent):
        try:
            bot = getattr(event, "bot", None)
            if bot is None:
                return
            uid = ""
            try:
                uid = str(event.get_sender_id())
            except Exception:
                pass
            for action in ("set_input_status", "set_typing", "send_typing"):
                try:
                    await bot.call_action(action, user_id=uid)
                    return
                except Exception:
                    continue
        except Exception:
            pass

    async def _send_plain(self, event: AstrMessageEvent, text: str) -> bool:
        try:
            await event.send(MessageChain([Plain(text)]))
            return True
        except Exception as exc:
            logger.warning("[hle] send failed: %s", exc)
            return False

    # ---------- worker ----------
    def _ensure_worker(self, umo: str, event: AstrMessageEvent):
        q = self._queues.get(umo)
        if q is None:
            q = asyncio.Queue()
            self._queues[umo] = q
        worker = self._workers.get(umo)
        if worker is None or worker.done():
            self._workers[umo] = asyncio.create_task(self._worker(umo, event, q))
        return q

    async def _worker(self, umo: str, event: AstrMessageEvent, q: asyncio.Queue):
        try:
            while True:
                try:
                    batch = await asyncio.wait_for(
                        q.get(), timeout=float(self._opt("idle_timeout", 120))
                    )
                except asyncio.TimeoutError:
                    break
                segments = batch.get("segments", [])
                self._interrupted[umo] = False
                if len(segments) > 1:
                    await asyncio.sleep(float(self._opt("read_delay", 0.8)))
                for idx, seg in enumerate(segments):
                    await self._typing(event)
                    if not await self._send_plain(event, seg):
                        break
                    if idx < len(segments) - 1:
                        base = random.uniform(
                            float(self._opt("gap_min", 1.5)),
                            float(self._opt("gap_max", 3.0)),
                        )
                        gap = base + len(segments[idx + 1]) * float(
                            self._opt("gap_per_char", 0.1)
                        )
                        if self._interrupted.get(umo):
                            gap *= float(self._opt("interrupt_speedup", 0.3))
                        await asyncio.sleep(gap)
                q.task_done()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("[hle] worker error: %s", exc)
        finally:
            self._workers.pop(umo, None)

    # ---------- hooks ----------
    @filter.on_decorating_result(priority=100)
    async def split_and_send(self, event: AstrMessageEvent):
        if not bool(self._opt("enable", True)):
            return
        result = event.get_result()
        chain = getattr(result, "chain", None)
        if not result or not chain:
            return

        text = "".join(c.text for c in chain if isinstance(c, Plain) and c.text)
        if not text.strip():
            return

        segments = self._split(text)
        if len(segments) <= 1:
            if segments and segments[0] != text:
                result.chain = [Plain(segments[0])]
            return

        umo = event.unified_msg_origin
        result.chain = []
        q = self._ensure_worker(umo, event)
        q.put_nowait({"segments": segments})

    @filter.event_message_type(filter.EventMessageType.ALL, priority=99999)
    async def notice_interrupt(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if umo in self._workers:
            self._interrupted[umo] = True
