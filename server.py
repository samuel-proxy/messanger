import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}      # user_id -> websocket
user_metadata = {}        # user_id -> {username, status, last_seen} (populated by client)

def now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print(f"safe_send error: {e}")

async def handler(websocket, path):
    print(f"🔌 NEW CONNECTION from {websocket.remote_address}")
    user_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")
            print(f"🎯 TYPE: {msg_type}")

            # ---------------- REGISTER ----------------
            if msg_type == "register":
                user_id = str(data.get("user_id")) if data.get("user_id") else None
                username = data.get("username")  # Client sends this from PHP

                if not user_id or user_id == "null":
                    await safe_send(websocket, {"type": "error", "message": "Invalid user_id"})
                    continue

                # Store metadata from client
                if username:
                    user_metadata[user_id] = {
                        "username": username,
                        "status": "online",
                        "last_seen": now()
                    }

                # Replace stale connection
                if user_id in connected_users:
                    old_ws = connected_users[user_id]
                    if old_ws is not websocket and not old_ws.closed:
                        try:
                            await old_ws.close()
                        except:
                            pass

                connected_users[user_id] = websocket
                print(f"✅ REGISTERED: {user_id} (@{username}) | Total: {len(connected_users)}")

                # Confirm to sender
                await safe_send(websocket, {
                    "type": "status",
                    "status": "connected",
                    "user_id": user_id,
                    "username": username
                })

                # Notify others this user is online
                for uid, ws in connected_users.items():
                    if uid != user_id and not ws.closed:
                        await safe_send(ws, {
                            "type": "status",
                            "status": "online",
                            "user_id": user_id,
                            "username": username
                        })

            # ---------------- MESSAGE ----------------
            elif msg_type == "message":
                sender_id = str(data.get("from_user_id")) if data.get("from_user_id") else None
                receiver_id = str(data.get("to_user_id")) if data.get("to_user_id") else None
                sender_name = data.get("from_name")  # Client resolves this via PHP
                msg = data.get("message")
                msg_id = data.get("message_id")
                timestamp = data.get("timestamp") or now()

                if not sender_id or not receiver_id:
                    await safe_send(websocket, {"type": "error", "message": "Missing sender or receiver"})
                    continue

                print(f"💬 {sender_name or sender_id} -> {receiver_id}: {msg}")

                receiver_ws = connected_users.get(receiver_id)

                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender_id,
                        "from_name": sender_name or sender_id,
                        "to_user_id": receiver_id,
                        "message": msg,
                        "message_id": msg_id,
                        "timestamp": timestamp
                    })

                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered",
                        "message_id": msg_id,
                        "user_id": receiver_id
                    })
                    print(f"✅ DELIVERED to {receiver_id}")
                else:
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline",
                        "user_id": receiver_id,
                        "message_id": msg_id
                    })
                    print(f"⚠️ OFFLINE: {receiver_id}")

            # ---------------- TYPING ----------------
            elif msg_type == "typing":
                receiver_id = str(data.get("to_user_id")) if data.get("to_user_id") else None
                sender_id = str(data.get("from_user_id")) if data.get("from_user_id") else None
                sender_name = data.get("from_name")

                if receiver_id and sender_id and receiver_id in connected_users:
                    await safe_send(connected_users[receiver_id], {
                        "type": "typing",
                        "from_user_id": sender_id,
                        "from_name": sender_name or sender_id
                    })

            # ---------------- PING ----------------
            elif msg_type == "ping":
                await safe_send(websocket, {"type": "pong", "timestamp": now()})

            # ---------------- STATUS RECEIPT ----------------
            elif msg_type == "status":
                target_id = str(data.get("to_user_id")) if data.get("to_user_id") else None
                if target_id and target_id in connected_users:
                    await safe_send(connected_users[target_id], {
                        "type": "status",
                        "status": data.get("code") or data.get("status"),
                        "message_id": data.get("message_id"),
                        "user_id": user_id
                    })

            # ---------------- USER METADATA UPDATE ----------------
            elif msg_type == "user_meta":
                # Client sends resolved usernames for offline users
                meta_user_id = str(data.get("user_id"))
                meta_username = data.get("username")
                meta_status = data.get("status")
                if meta_user_id and meta_username:
                    user_metadata[meta_user_id] = {
                        "username": meta_username,
                        "status": meta_status or "offline",
                        "last_seen": now()
                    }

    except websockets.ConnectionClosed as e:
        print(f"🔌 ConnectionClosed: code={e.code}")
    except Exception as e:
        print(f"💥 Handler error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if user_id and user_id in connected_users:
            if connected_users[user_id] is websocket:
                del connected_users[user_id]
                # Mark as offline in metadata
                if user_id in user_metadata:
                    user_metadata[user_id]["status"] = "offline"
                print(f"❌ DISCONNECTED: {user_id}")
                
                # Notify others
                for uid, ws in connected_users.items():
                    if not ws.closed:
                        await safe_send(ws, {
                            "type": "status",
                            "status": "offline",
                            "user_id": user_id,
                            "username": user_metadata.get(user_id, {}).get("username", user_id)
                        })

PORT = int(os.environ.get("PORT", 10000))

async def main():
    print(f"🚀 Starting server on 0.0.0.0:{PORT}")
    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):
        print("✅ Server is listening")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
