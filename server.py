import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}

async def safe_send(ws, data):
    try: await ws.send(json.dumps(data))
    except: pass

async def handler(websocket):
    user_id = None
    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "register":
                user_id = str(data.get("user_id"))
                connected_users[user_id] = websocket
                print(f"User {user_id} registered.")
                await safe_send(websocket, {"type": "status", "code": "connected"})

            elif msg_type == "message":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))
                
                # Relay to receiver if they are currently on a socket
                if receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "message",
                        "from_user_id": sender,
                        "message": data.get("message"),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    await safe_send(websocket, {"type": "status", "code": "delivered"})
                else:
                    # Python tells JS: "I couldn't find them, rely on the DB save."
                    await safe_send(websocket, {"type": "status", "code": "offline"})

    finally:
        if user_id in connected_users:
            del connected_users[user_id]

async def main():
    port = int(os.environ.get("PORT", 5000))
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

asyncio.run(main())
