from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger("xcore.websocket")


@dataclass(frozen=True)
class _Client:
    ws: WebSocket
    user_id: str | None
    # Contexte d'autorisation figé à la connexion (tenant_id, permissions) :
    # permet un envoi ciblé par règle d'accès (send_where) sans requête base.
    claims: Mapping[str, Any] = field(default_factory=dict)


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
        user_id: str | None = None,
        claims: Mapping[str, Any] | None = None,
    ) -> None:
        await ws.accept()
        async with self._lock:
            self._channels[channel][client_id] = _Client(ws=ws, user_id=user_id, claims=dict(claims or {}))
        # Pas de diffusion de présence : le canal est partagé par tous les
        # tenants, annoncer les connexions exposerait les identifiants d'utilisateurs.
        logger.debug("websocket connecté client=%s channel=%s", client_id[:8], channel)

    async def disconnect(self, channel: str, client_id: str) -> None:
        async with self._lock:
            self._channels[channel].pop(client_id, None)
        logger.debug("websocket déconnecté client=%s channel=%s", client_id[:8], channel)

    @staticmethod
    def _encode(channel: str, event: str, data: Any) -> str:
        return json.dumps({
            "channel": channel,
            "action": event.upper(),
            "payload": data,
            "id": (data.get("id") if isinstance(data, dict) else None),
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
                logger.warning("envoi websocket échoué client=%s channel=%s : %s", cid[:8], channel, e)
                dead.append(cid)

        if dead:
            await asyncio.gather(*(self.disconnect(channel, cid) for cid in set(dead)))

        return len(targets) - len(dead)

    async def send_where(
        self, channel: str, event: str, data: Any, predicate: Callable[[Mapping[str, Any]], bool]
    ) -> int:
        """Envoie aux seuls clients dont le contexte d'autorisation satisfait
        `predicate` (ex. : utilisateurs autorisés sur le numéro WhatsApp)."""
        message = self._encode(channel, event, data)
        async with self._lock:
            targets = [(cid, c) for cid, c in self._channels[channel].items() if c.user_id and predicate(c.claims)]

        dead: list[str] = []
        for cid, client in targets:
            try:
                await client.ws.send_text(message)
            except Exception as e:
                logger.warning("envoi websocket échoué client=%s channel=%s : %s", cid[:8], channel, e)
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
                logger.warning("envoi websocket échoué client=%s channel=%s : %s", cid[:8], channel, e)
                dead.append(cid)

        # Envoi parallèle
        await asyncio.gather(*(_send(cid, c) for cid, c in clients), return_exceptions=True)

        if dead:
            # Supprimer proprement
            await asyncio.gather(*(self.disconnect(channel, cid) for cid in set(dead)))

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