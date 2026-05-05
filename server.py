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
    except Exception as e:
        print("safe_send error:", e)

# ---------------- RENDER HEALTH CHECK ----------------
async def process_request(path, request_headers):
    if request_headers.get("Upgrade", "").lower() == "websocket":
        return None
    return (
        200,
        [("Content-Type", "text/plain")],
        b"WebSocket server running"
    )

# ---------------- HANDLER ----------------
async def handler(websocket):
    user_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            # ---------------- REGISTER ----------------
            if msg_type == "register":
                user_id = str(data.get("user_id")) if data.get("user_id") else None

                if not user_id or user_id == "null":
                    await safe_send(websocket, {
                        "type": "error",
                        "message": "Invalid user_id"
                    })
                    continue

                # Replace stale connection if same user reconnects
                if user_id in connected_users:
                    old_ws = connected_users[user_id]
                    if old_ws is not websocket and not old_ws.closed:
                        try:
                            await old_ws.close()
                        except:
                            pass

                connected_users[user_id] = websocket

                print(f"✅ CONNECTED: {user_id}")
                print("ONLINE USERS:", list(connected_users.keys()))

                await safe_send(websocket, {
                    "type": "status",
                    "status": "connected",
                    "user_id": user_id
                })

            # ---------------- PING ----------------
            elif msg_type == "ping":
                await safe_send(websocket, {
                    "type": "pong",
                    "time": now()
                })

            # ---------------- MESSAGE ----------------
            elif msg_type == "message":
                sender = str(data.get("from_user_id")) if data.get("from_user_id") else None
                receiver = str(data.get("to_user_id")) if data.get("to_user_id") else None
                msg = data.get("message")
                msg_id = data.get("message_id")
                timestamp = data.get("timestamp") or now()

                if not sender or not receiver:
                    await safe_send(websocket, {
                        "type": "error",
                        "message": "Missing sender or receiver"
                    })
                    continue

                print(f"{sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    # Forward to receiver with field names the client expects
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender,
                        "from_name": sender,
                        "to_user_id": receiver,
                        "message": msg,
                        "message_id": msg_id,
                        "timestamp": timestamp
                    })

                    # Confirm delivery to sender
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered",
                        "message_id": msg_id,
                        "user_id": receiver,
                        "timestamp": now()
                    })
                else:
                    # Receiver is offline
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline",
                        "user_id": receiver,
                        "message_id": msg_id,
                        "timestamp": now()
                    })

            # ---------------- TYPING ----------------
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id")) if data.get("to_user_id") else None
                sender = str(data.get("from_user_id")) if data.get("from_user_id") else None

                if receiver and sender and receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "typing",
                        "from_user_id": sender
                    })

            # ---------------- STATUS RECEIPT (read/delivered) ----------------
            elif msg_type == "status":
                target = str(data.get("to_user_id")) if data.get("to_user_id") else None
                if target and target in connected_users:
                    await safe_send(connected_users[target], {
                        "type": "status",
                        "status": data.get("code") or data.get("status"),
                        "message_id": data.get("message_id"),
                        "user_id": user_id
                    })

    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        print("ERROR:", e)

    finally:
        if user_id and user_id in connected_users:
            # Only remove if this is the current socket (not a stale disconnect)
            if connected_users[user_id] is websocket:
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
