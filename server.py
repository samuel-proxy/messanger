import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}

def now():
    return datetime.utcnow().isoformat()

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print("SEND ERROR:", e)

# ✅ FIXED: Render-safe HTTP handler
async def process_request(path, request_headers):
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return (200, [("Content-Type", "text/plain")], b"OK")
    return None

# ---------------- MAIN HANDLER ----------------
async def handler(websocket):
    user_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except:
                continue

            msg_type = data.get("type")

            # -------- REGISTER --------
            if msg_type == "register":
                user_id = str(data.get("user_id"))

                if not user_id or user_id == "null":
                    continue

                connected_users[user_id] = websocket

                print(f"✅ CONNECTED: {user_id}")
                print("ONLINE USERS:", list(connected_users.keys()))

                await safe_send(websocket, {
                    "type": "status",
                    "code": "connected"
                })

            # -------- MESSAGE --------
            elif msg_type == "message":
                sender = str(data.get("from_user_id"))
                receiver = str(data.get("to_user_id"))
                msg = data.get("message")

                print(f"📨 {sender} -> {receiver}: {msg}")
                print("CURRENT USERS:", list(connected_users.keys()))

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    print(f"✅ DELIVERING to {receiver}")

                    # SEND TO RECEIVER
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender,
                        "to_user_id": receiver,
                        "message": msg,
                        "timestamp": now()
                    })

                    # CONFIRM DELIVERY TO SENDER
                    await safe_send(websocket, {
                        "type": "status",
                        "code": "delivered",
                        "timestamp": now()
                    })

                else:
                    print(f"❌ {receiver} OFFLINE")

                    await safe_send(websocket, {
                        "type": "status",
                        "code": "offline",
                        "timestamp": now()
                    })

            # -------- TYPING --------
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))

                if receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "typing",
                        "from_user_id": sender
                    })

            # -------- PING --------
            elif msg_type == "ping":
                await safe_send(websocket, {
                    "type": "pong"
                })

    except Exception as e:
        print("ERROR:", e)

    finally:
        if user_id and user_id in connected_users:
            del connected_users[user_id]
            print(f"❌ DISCONNECTED: {user_id}")
            print("ONLINE USERS:", list(connected_users.keys()))

# ---------------- START ----------------
PORT = int(os.environ.get("PORT", 5000))

async def main():
    print(f"🚀 Running on port {PORT}")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        process_request=process_request,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

asyncio.run(main())
