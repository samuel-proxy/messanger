import asyncio
import websockets
import json
import os
import urllib.request
import urllib.parse
from datetime import datetime

# ---------------- STATE ----------------

connected_users = {}   # user_id -> websocket
debug_clients = set()  # websockets that receive debug logs

PHP_BASE_URL = "https://onana.free.nf"  # CHANGE THIS

# ---------------- LOGGING ----------------

def log(msg):
    print("[SERVER]", msg)

async def debug_broadcast(message):
    """Send logs to frontend console"""
    dead = set()

    for ws in debug_clients:
        try:
            await ws.send(json.dumps({
                "type": "debug",
                "message": message
            }))
        except:
            dead.add(ws)

    debug_clients.difference_update(dead)

# ---------------- PHP STATUS UPDATE ----------------

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

        urllib.request.urlopen(req, timeout=2)

        log(f"DB UPDATE: {user_id} -> {status}")

    except Exception as e:
        log(f"PHP ERROR: {e}")

# ---------------- SAFE SEND ----------------

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        log(f"SEND FAIL: {e}")

# ---------------- CLEAN DISCONNECT ----------------

def remove_user(user_id):
    if user_id in connected_users:
        del connected_users[user_id]
        update_status(user_id, "offline")
        log(f"OFFLINE: {user_id}")

# ---------------- HANDLER ----------------

async def handler(websocket):

    user_id = None
    is_debug = False

    await debug_broadcast("New connection attempt")

    try:
        async for message in websocket:

            # ---------------- PARSE ----------------
            try:
                data = json.loads(message)
            except:
                await debug_broadcast("Invalid JSON received")
                continue

            msg_type = data.get("type")

            # ---------------- DEBUG MODE ----------------
            if msg_type == "debug":
                debug_clients.add(websocket)
                is_debug = True
                await safe_send(websocket, {
                    "type": "debug",
                    "message": "Debug mode enabled"
                })
                continue

            # ---------------- REGISTER ----------------
            if msg_type == "register":

                user_id = str(data.get("user_id"))

                if not user_id or user_id == "null":
                    await debug_broadcast("Invalid user_id on register")
                    continue

                connected_users[user_id] = websocket

                log(f"CONNECTED: {user_id}")
                log(f"ONLINE USERS: {list(connected_users.keys())}")

                await debug_broadcast(f"{user_id} connected")

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

                await debug_broadcast(f"MSG {sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from": sender,
                        "message": msg
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
        log(f"ERROR: {e}")
        await debug_broadcast(f"Server error: {e}")

    finally:
        if user_id:
            remove_user(user_id)
            await debug_broadcast(f"{user_id} disconnected")


# ---------------- START SERVER ----------------

PORT = int(os.environ.get("PORT", 5000))

async def main():
    log(f"Starting WebSocket server on {PORT}")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**20,
        compression=None
    ):
        await asyncio.Future()

asyncio.run(main())
