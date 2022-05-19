from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[id] = websocket

    def disconnect(self, id: str):
        if id in self.active_connections:
            del self.active_connections[id]

    async def send_personal_message(self, id: str, message):
        if id in self.active_connections:
            await self.active_connections[id].send_json(message)

    # async def broadcast(self, message: str):
    #     for _, connection in self.active_connections:
    #         await connection.send_text(message)
