# Intégration — xwebsocket

## 1. Déclarer l'extension dans `integration.yaml`

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

Les noms de canaux sont libres. Un client qui tente de se connecter à un canal non déclaré reçoit un code `4003` et est déconnecté.

## 2. Exposer l'endpoint WebSocket depuis un plugin

```python
from extensions.xwebsocket.main import WsManager

class MyPlugin(XCorePlugin):
    async def on_load(self):
        ws: WsManager = self.get_service("ext.websocket")

        self.router.add_api_websocket_route(
            "/ws/{channel}",
            ws.ws_endpoint,
        )
```

Le client se connecte ensuite à `ws://localhost:8000/ws/user` en passant son JWT dans le header ou le cookie (selon la config XCore).

## 3. Broadcaster depuis n'importe quel plugin

```python
ws = self.get_service("ext.websocket")

await ws.broadcast(
    channel="user",
    event="SUBMISSION_UPDATE",
    data={"submission_id": "uuid", "status": "approved"},
)
```

Format du message reçu côté client :

```json
{
  "channel": "user",
  "action": "SUBMISSION_UPDATE",
  "payload": { "submission_id": "uuid", "status": "approved" },
  "id": null,
  "timestamp": "2026-05-22T10:00:00.000000"
}
```

## 4. Obtenir des statistiques

```python
info = await ws.info()
# {
#   "client_count": 3,
#   "channels": ["user", "admin", "broadcast", "platform"],
#   "client_ids": { "uuid1": {"sub": "42", "channel": "user"}, ... }
# }
```

## 5. Événements de présence

À chaque connexion/déconnexion d'un utilisateur authentifié, un événement `PRESENCE` est broadcasté automatiquement sur le canal :

```json
{ "action": "JOIN",  "userId": "42" }
{ "action": "LEAVE", "userId": "42" }
```

Les connexions anonymes (sans `sub` dans le JWT) ne génèrent pas d'événement de présence.

## 6. Sécurité

- Chaque connexion WebSocket passe par `get_current_user` (JWT RS256).
- Un canal non déclaré dans la config → fermeture immédiate (`code 4003`).
- Les clients morts sont détectés à chaque broadcast et retirés proprement.
