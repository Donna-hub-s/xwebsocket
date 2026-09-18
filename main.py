from __future__ import annotations

import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Request, WebSocket, WebSocketDisconnect
from xcore.kernel.api.auth import AuthPayload, get_auth_backend
from xcore.sdk import get_logger
from xcore.services.base import BaseService, ServiceStatus

from .ws import WebSocketManager

logger = get_logger("ext.websocket")

# Durée de vie d'un ticket WS (secondes) — juste assez pour laisser le temps
# au navigateur d'ouvrir la connexion juste après l'avoir obtenu, jamais
# réutilisable une fois consommé (voir issue_ticket/_redeem_ticket) : un
# ticket qui traînerait dans un historique réseau ne vaut rien passé ce délai.
_TICKET_TTL_S = 30.0


@dataclass
class WebSocketConf:
    channel: list[str]

    @classmethod
    def from_dict(cls, data: dict) -> "WebSocketConf":
        return cls(channel=data.get("channel", []))


class WsManager(BaseService):
    name = "websocket"

    def __init__(self, config: dict) -> None:
        self._status = ServiceStatus.INITIALIZING
        self.config = config

        self.client_ids: dict[str, dict[str, str]] = {}  # <-- initialisé
        self.ws: Optional[WebSocketManager] = None
        self.configuration: Optional[WebSocketConf] = None
        # Tickets WS à usage unique — voir issue_ticket/_redeem_ticket. En
        # mémoire du processus, comme le reste de l'état WS (cohérent avec la
        # contrainte API_WORKERS=1 déjà documentée pour ce module).
        self._tickets: dict[str, dict[str, Any]] = {}

    async def init(self) -> None:
        self.configuration = WebSocketConf.from_dict(data=self.config)
        self.ws = WebSocketManager()
        self._status = ServiceStatus.READY

    def issue_ticket(
        self,
        user_id: str,
        tenant_id: str | None,
        permissions: list[str] | None = None,
        roles: list[str] | None = None,
        channel: str = "user",
        ttl: float = _TICKET_TTL_S,
    ) -> str:
        """Émet un ticket à usage unique, courte durée de vie, substituable au
        JWT d'accès pour la seule poignée de main WS — évite d'exposer le
        vrai jeton (httpOnly) au JavaScript du navigateur. Consommé par
        _redeem_ticket, jamais rejouable."""
        now = time.monotonic()
        # Purge opportuniste des tickets expirés non consommés (dict petit,
        # pas besoin d'une tâche de fond dédiée).
        expired = [t for t, data in self._tickets.items() if data["expires_at"] <= now]
        for t in expired:
            self._tickets.pop(t, None)

        ticket = secrets.token_urlsafe(32)
        self._tickets[ticket] = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "permissions": list(permissions or []),
            "roles": list(roles or []),
            "channel": channel,
            "expires_at": now + ttl,
        }
        return ticket

    def _redeem_ticket(self, ticket: str, channel: str) -> dict[str, Any] | None:
        data = self._tickets.pop(ticket, None)
        if data is None:
            return None
        if data["expires_at"] <= time.monotonic() or data["channel"] != channel:
            return None
        return data

    async def shutdown(self) -> None:
        # Ici shutdown est sync dans ton code original
        self.ws = None
        self._status = ServiceStatus.STOPPED

    async def ws_endpoint(self, ws: WebSocket, request: Request | WebSocket, channel: str):
        # request est en pratique le WebSocket lui-même (pas de Request
        # injectable dans un scope ASGI "websocket") — il expose la même
        # interface .headers/.cookies/.query_params, donc ça passe.
        ticket = request.query_params.get("ticket")
        response: AuthPayload | dict[str, Any] | None
        if ticket:
            ticket_data = self._redeem_ticket(ticket, channel)
            response = (
                {
                    "sub": ticket_data["user_id"],
                    "user": {"tenant_id": ticket_data["tenant_id"]},
                    "permissions": ticket_data["permissions"],
                    "roles": ticket_data["roles"],
                }
                if ticket_data is not None
                else None
            )
        else:
            response = await self._resolve_auth(request)

        # Auth OBLIGATOIRE : sans jeton valide (backend absent, token absent
        # ou invalide), on refuse — l'ancien code continuait avec
        # response=None et connectait le socket en anonyme (user_id=None),
        # exposant tous les broadcasts du canal à n'importe qui.
        if not response or not response.get("sub"):
            logger.warning(
                "connexion websocket anonyme refusée",
                channel=channel,
            )
            await ws.accept()
            await ws.close(code=4401)
            return

        # Sécurité
        if not self.configuration or channel not in self.configuration.channel:
            logger.warning(
                "accès canal websocket refusé",
                user_id=response.get("sub"),
                channel=channel,
            )
            await ws.accept()
            await ws.close(code=4003)
            return

        client_id = str(uuid.uuid4())

        inf: dict[str, str] = {
            "sub": str(response.get("sub")),
            "channel": str(channel),
        }
        self.client_ids[client_id] = inf

        if self.ws is None:
            # Normalement init() a déjà créé self.ws, mais on garde safe.
            await ws.close(code=1011)
            self.client_ids.pop(client_id, None)
            return

        await self.ws.connect(
            channel=channel, client_id=client_id, ws=ws,
            user_id=response.get("sub"),
            claims={
                "tenant_id": (response.get("user") or {}).get("tenant_id"),
                "permissions": frozenset(response.get("permissions") or [])
                | frozenset(response.get("roles") or []),
            },
        )

        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            await self.ws.disconnect(channel=channel, client_id=client_id)
            self.client_ids.pop(client_id, None)
        except Exception as e:
            logger.warning("déconnexion websocket inattendue", client_id=client_id[:8], channel=channel, error=str(e))
            await self.ws.disconnect(channel=channel, client_id=client_id)
            self.client_ids.pop(client_id, None)

    async def _resolve_auth(self, request: Request | WebSocket) -> AuthPayload | None:
        backend = get_auth_backend()
        if backend is None:
            return None
        token = await backend.extract_token(request)
        if token is None:
            return None
        return await backend.decode_token(token)

    async def broadcast(self, channel: str, event: str, data: Any):
        if self.ws is not None:
            await self.ws.broadcast(channel, event, data)

    async def send_to_user(self, user_id: str, channel: str, event: str, data: Any) -> int:
        if self.ws is None:
            return 0
        return await self.ws.send_to_user(user_id, channel, event, data)

    async def send_where(self, channel: str, event: str, data: Any, predicate) -> int:
        if self.ws is None:
            return 0
        return await self.ws.send_where(channel, event, data, predicate)

    async def health_check(
        self,
    ):

        if self.ws:
            return True, "Service running"
        return False, "Service stopped"

    async def status(
        self,
    ):

        return {}

    async def info(
        self,
    ):
        return {
            "client_count": len(self.client_ids),
            "channels": list(self.configuration.channel) if self.configuration else [],
            "client_ids": self.client_ids,
        }
