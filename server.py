import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}  # user_id -> websocket

def now():
    return datetime.utcnow().strftime("%H:%M")

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

async def handler(websocket):
    user_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except:
                continue

            msg_type = data.get("type")

            # ---------------- REGISTER ----------------
            if msg_type == "register":
                user_id = str(data.get("user_id"))

                if not user_id:
                    continue

                connected_users[user_id] = websocket
                print(f"{user_id} connected")

                await safe_send(websocket, {
                    "type": "status",
                    "status": "connected"
                })

            # ---------------- MESSAGE ----------------
            elif msg_type == "message":
                sender = str(data.get("from_user_id"))
                receiver = str(data.get("to_user_id"))
                msg = data.get("message")

                if not sender or not receiver or not msg:
                    continue

                timestamp = now()

                print(f"{sender} -> {receiver}: {msg}")

                if receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "message",
                        "from": sender,
                        "message": msg,
                        "time": timestamp
                    })

                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered",
                        "time": timestamp
                    })
                else:
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline"
                    })

            # ---------------- TYPING ----------------
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))

                if receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "typing",
                        "from": sender
                    })

    except Exception as e:
        print("Error:", e)

    finally:
        if user_id and user_id in connected_users:
            del connected_users[user_id]
            print(f"{user_id} disconnected")


PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("Running on port", PORT)
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
