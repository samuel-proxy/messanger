import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}

# ---------------- TIME ----------------

def now():
    return datetime.utcnow().strftime("%H:%M:%S")

# ---------------- SAFE SEND ----------------

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

# ---------------- HTTP HANDLER (CRITICAL FIX) ----------------

async def process_request(path, request_headers):
    """
    Handles HTTP requests like HEAD/GET from Render.
    Prevents WebSocket handshake crashes.
    """
    return (
        200,
        [("Content-Type", "text/plain")],
        b"OK"
    )

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

                print(f"✅ CONNECTED: {user_id}")
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

                print(f"{sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from": sender,
                        "message": msg,
                        "time": now()
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
        print("ERROR:", e)

    finally:
        if user_id and user_id in connected_users:
            del connected_users[user_id]
            print(f"❌ DISCONNECTED: {user_id}")
            print("ONLINE USERS:", list(connected_users.keys()))

# ---------------- SERVER START ----------------

PORT = int(os.environ.get("PORT", 5000))

async def main():
    print(f"🚀 Running on port {PORT}")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        process_request=process_request,  # 🔥 FIX HERE
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

asyncio.run(main())
