import asyncio
import websockets
import json
import os
import requests
from datetime import datetime

connected_users = {}  # user_id -> websocket

PHP_BASE_URL = "https://onana.free.nf"  # 🔥 CHANGE THIS

# ---------------- TIME ----------------

def now():
    return datetime.utcnow().strftime("%H:%M:%S")

# ---------------- SAFE SEND ----------------

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print("Send error:", e)

# ---------------- PHP STATUS UPDATE ----------------

def set_status(user_id, status):
    try:
        requests.post(
            f"{PHP_BASE_URL}/set_status.php",
            data={
                "user_id": user_id,
                "status": status
            },
            timeout=3
        )
        print(f"DB STATUS UPDATE: {user_id} -> {status}")
    except Exception as e:
        print("PHP status error:", e)

# ---------------- UNREGISTER USER ----------------

def mark_offline(user_id):
    if not user_id:
        return

    if user_id in connected_users:
        del connected_users[user_id]

    set_status(user_id, "offline")
    print(f"❌ {user_id} OFFLINE")

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

            # ---------------- REGISTER ----------------
            if msg_type == "register":
                user_id = str(data.get("user_id"))

                if not user_id or user_id == "null":
                    print("❌ Invalid user_id")
                    continue

                connected_users[user_id] = websocket

                print(f"✅ {user_id} ONLINE")
                print("ONLINE USERS:", list(connected_users.keys()))

                # 🔥 UPDATE DB
                set_status(user_id, "online")

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

                time = now()

                print(f"📩 {sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)

                # ---------------- IF ONLINE ----------------
                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from": sender,
                        "message": msg,
                        "time": time
                    })

                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered"
                    })

                # ---------------- IF OFFLINE ----------------
                else:
                    print("❌ Receiver offline:", receiver)

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
        print("Connection error:", e)

    finally:
        # ---------------- CLEAN DISCONNECT ----------------
        if user_id:
            mark_offline(user_id)


# ---------------- SERVER START ----------------

PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("🚀 WebSocket running on port", PORT)

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

asyncio.run(main())
