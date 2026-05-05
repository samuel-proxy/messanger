import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}  # user_id -> websocket

def now():
    return datetime.utcnow().strftime("%H:%M:%S")

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print("Send error:", e)

async def unregister(user_id):
    if user_id in connected_users:
        del connected_users[user_id]
        print(f"❌ {user_id} disconnected")

async def handler(websocket):
    user_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception as e:
                print("JSON parse error:", e)
                continue

            msg_type = data.get("type")

            # ---------------- REGISTER ----------------
            if msg_type == "register":
                user_id = str(data.get("user_id"))

                print("REGISTER RAW:", data)

                if not user_id or user_id == "null":
                    print("❌ Invalid user ID on register")
                    continue

                connected_users[user_id] = websocket
                print(f"✅ {user_id} connected")
                print("ONLINE USERS:", list(connected_users.keys()))

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

                print(f"📩 {sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    # Deliver message if receiver is online
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from": sender,
                        "message": msg,
                        "time": timestamp
                    })
                    # Inform sender that message was delivered
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered",
                        "time": timestamp
                    })
                else:
                    print("❌ Receiver offline:", receiver)
                    # Inform sender that receiver is offline
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline"
                    })

            # ---------------- TYPING ----------------
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "typing",
                        "from": sender
                    })

    except Exception as e:
        print("Error:", e)

    finally:
        if user_id:
            await unregister(user_id)


PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("🚀 Server running on port", PORT)

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

asyncio.run(main())
