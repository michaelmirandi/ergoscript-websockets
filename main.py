import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from connection_manager import ConnectionManager
from store import Store
from sync_service import SyncService

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

app = FastAPI()

html = ""
with open("index.html", "r") as fin:
    html = fin.read()

manager = ConnectionManager()
store = Store()
sync_service = SyncService(store, manager)

sync_service.start()


@app.get("/")
async def get():
    return HTMLResponse(html)


class RegisterRequest(BaseModel):
    url: str


@app.post("/register/{client_id}")
async def register(client_id: str, register: RegisterRequest):
    try:
        sync_service.add_task(client_id, register.url)
        return {
            "status": "ok",
            "client_id": client_id,
            "url": register.url
        }
    except Exception as e:
        logging.error(f"register::{str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(client_id, websocket)
    try:
        while True:
            url = sync_service.get_last_task(client_id)
            if url:
                await manager.send_personal_message(client_id, str(store.get(url)))
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(client_id)
        sync_service.remove_task(client_id)
