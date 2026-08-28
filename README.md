# xwebsocket

Extension XCore de gestion WebSocket multi-canal avec authentification JWT.

## Fonctionnalités

- Canaux configurables (user, admin, broadcast, platform, …)
- Authentification JWT obligatoire via `get_current_user` à la connexion
- Broadcast par canal en parallèle avec nettoyage automatique des clients morts
- Événements de présence (`JOIN` / `LEAVE`) émis automatiquement
- Accès via `self.get_service("ext.websocket")` depuis n'importe quel plugin

## Configuration

```yaml
services:
  extensions:
    web_socket:
      module: extensions.xwebsocket.main:WsManager
      config:
        channel:
          - user
          - admin
          - broadcast
          - platform
```

## Utilisation depuis un plugin

```python
ws_manager = self.get_service("ext.websocket")

# Broadcast vers tous les clients d'un canal
await ws_manager.broadcast(channel="user", event="NOTIFICATION", data={"message": "Nouveau plugin publié"})

# Infos de connexion
info = await ws_manager.info()
# → {"client_count": 3, "channels": ["user", "admin"], "client_ids": {...}}
```

### Enregistrer l'endpoint WebSocket

Dans le `on_load()` du plugin, exposer la route via XCore :

```python
from extensions.xwebsocket.main import WsManager

ws: WsManager = self.get_service("ext.websocket")
self.router.add_api_websocket_route(
    "/ws/{channel}",
    ws.ws_endpoint,
)
```

## Format des messages émis

```json
{
  "channel": "user",
  "action": "NOTIFICATION",
  "payload": { "message": "..." },
  "id": "...",
  "timestamp": "2026-05-22T10:00:00.000000"
}
```

## Structure

```
xwebsocket/
├── main.py       # WsManager (BaseService) — lifecycle + sécurité
├── ws.py         # WebSocketManager — connexions, broadcast, présence
└── service.yaml  # Manifeste de l'extension
```
