from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from fastapi import WebSocket


@dataclass(frozen=True)
class _Client:
    ws: WebSocket
    user_id: str | None


class WebSocketManager:
    def __init__(self) -> None:
        # channel → { client_id → _Client }
        self._channels: dict[str, dict[str, _Client]] = defaultdict(dict)
        self._lock = asyncio.Lock()

    async def connect(
        self,
        channel: str,
        client_id: str,
        ws: WebSocket,
        user_id: str | None = None
    ) -> None:
        await ws.accept()
        async with self._lock:
            self._channels[channel][client_id] = _Client(ws=ws, user_id=user_id)

        print(f"[WS] {client_id[:8]} (user={user_id}) connecté au channel '{channel}'")

        if user_id is not None:
            await self.broadcast(channel, "PRESENCE", {"userId": user_id, "action": "JOIN"})

    async def disconnect(self, channel: str, client_id: str) -> None:
        async with self._lock:
            client = self._channels[channel].pop(client_id, None)

        if client and client.user_id is not None:
            # broadcast est async => on peut la faire directement ici
            await self.broadcast(channel, "PRESENCE", {"userId": client.user_id, "action": "LEAVE"})

        print(f"[WS] {client_id[:8]} déconnecté du channel '{channel}'")

    @staticmethod
    def _encode(channel: str, event: str, data: Any) -> str:
        from datetime import datetime

        return json.dumps({
            "channel": channel,
            "action": event.upper(),
            "payload": data,
            "id": (data.get("id") if isinstance(data, dict) else None),
            "timestamp": datetime.utcnow().isoformat(),
        }, ensure_ascii=False, default=str)

    async def send_to_user(self, user_id: str, channel: str, event: str, data: Any) -> int:
        """Envoie à un seul utilisateur (toutes ses connexions sur ce canal). Retourne le nombre de clients touchés."""
        message = self._encode(channel, event, data)

        async with self._lock:
            targets = [
                (cid, c) for cid, c in self._channels[channel].items() if c.user_id == user_id
            ]

        dead: list[str] = []
        for cid, client in targets:
            try:
                await client.ws.send_text(message)
            except Exception as e:
                print(f"[WS] Erreur envoi à {cid[:8]} (user={user_id}) sur '{channel}': {e}")
                dead.append(cid)

        if dead:
            await asyncio.gather(*(self.disconnect(channel, cid) for cid in set(dead)))

        return len(targets) - len(dead)

    async def broadcast(self, channel: str, event: str, data: Any) -> None:
        message = self._encode(channel, event, data)

        # Snapshot sous lock
        async with self._lock:
            clients = list(self._channels[channel].items())

        dead: list[str] = []

        async def _send(cid: str, client: _Client) -> None:
            try:
                await client.ws.send_text(message)
            except Exception as e:
                print(f"[WS] Erreur envoi à {cid[:8]} sur '{channel}': {e}")
                dead.append(cid)

        # Envoi parallèle
        await asyncio.gather(*(_send(cid, c) for cid, c in clients), return_exceptions=True)

        if dead:
            # Supprimer proprement
            await asyncio.gather(*(self.disconnect(channel, cid) for cid in set(dead)))

        print(
            f"[WS BROADCAST] channel={channel} action={event.upper()} "
            f"clients={len(self._channels[channel])} "
            f"id={(data.get('id') if isinstance(data, dict) else '-')}"
        )

    def connected_count(self, channel: str) -> int:
        return len(self._channels.get(channel, {}))

    def all_stats(self) -> dict[str, int]:
        return {ch: len(clients) for ch, clients in self._channels.items()}

    def get_users_in_channel(self, channel: str) -> list[str]:
        users = [
            c.user_id for c in self._channels.get(channel, {}).values()
            if c.user_id is not None
        ]
        return list(set(users))



ws = WebSocketManager()