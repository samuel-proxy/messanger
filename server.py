import asyncio
import websockets
import json
import os
import urllib.request
import urllib.parse
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
    except:
        pass

# ---------------- PHP STATUS UPDATE (NO REQUESTS LIB) ----------------

def update_status(user_id, status):
    try:
        data = urllib.parse.urlencode({
            "user_id": user_id,
            "status": status
        }).encode()

        req = urllib.request.Request(
            f"{PHP_BASE_URL}/set_status.php",
            data=data
        )

        urllib.request.urlopen(req, timeout=3)

        print(f"STATUS: {user_id} -> {status}")

    except Exception as e:
        print("Status update error:", e)

# ---------------- USER DISCONNECT ----------------

def mark_offline(user_id):
    if not user_id:
        return

    connected_users.pop(user_id, None)
    update_status(user_id, "offline")
    print(f"❌ OFFLINE: {user_id}")

# ---------------- HANDLER ----------------

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
                    continue

                connected_users[user_id] = websocket

                print(f"✅ ONLINE: {user_id}")
                print("USERS:", list(connected_users.keys()))

                update_status(user_id, "online")

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

                receiver_ws = connected_users.get(receiver)

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
        print("Connection error:", e)

    finally:
        mark_offline(user_id)


# ---------------- SERVER START ----------------

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
