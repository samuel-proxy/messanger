import asyncio
import websockets
import json
import os
from datetime import datetime

connected_users = {}

def now():
    return datetime.utcnow().strftime("%H:%M:%S")

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print(f"safe_send error: {e}")

async def handler(websocket, path):
    print(f"🔌 NEW CONNECTION from {websocket.remote_address}, path={path}")
    user_id = None

    try:
        async for message in websocket:
            print(f"📦 RAW: {message[:200]}")

            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                print(f"❌ JSON error: {e}")
                continue

            msg_type = data.get("type")
            print(f"🎯 TYPE: {msg_type}")

            if msg_type == "register":
                user_id = str(data.get("user_id")) if data.get("user_id") else None
                print(f"Register attempt: user_id={user_id}")

                if not user_id or user_id == "null":
                    await safe_send(websocket, {"type": "error", "message": "Invalid user_id"})
                    continue

                if user_id in connected_users:
                    old_ws = connected_users[user_id]
                    if old_ws is not websocket and not old_ws.closed:
                        try:
                            await old_ws.close()
                            print(f"Closed stale connection for {user_id}")
                        except:
                            pass

                connected_users[user_id] = websocket
                print(f"✅ REGISTERED: {user_id} | Total: {len(connected_users)}")

                await safe_send(websocket, {
                    "type": "status",
                    "status": "connected",
                    "user_id": user_id
                })

            elif msg_type == "message":
                sender = str(data.get("from_user_id")) if data.get("from_user_id") else None
                receiver = str(data.get("to_user_id")) if data.get("to_user_id") else None
                msg = data.get("message")
                msg_id = data.get("message_id")

                print(f"💬 {sender} -> {receiver}: {msg}")

                if not sender or not receiver:
                    await safe_send(websocket, {"type": "error", "message": "Missing sender or receiver"})
                    continue

                receiver_ws = connected_users.get(receiver)

                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender,
                        "from_name": sender,
                        "to_user_id": receiver,
                        "message": msg,
                        "message_id": msg_id,
                        "timestamp": now()
                    })

                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered",
                        "message_id": msg_id,
                        "user_id": receiver
                    })
                    print(f"✅ DELIVERED to {receiver}")
                else:
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline",
                        "user_id": receiver,
                        "message_id": msg_id
                    })
                    print(f"⚠️ OFFLINE: {receiver} not in {list(connected_users.keys())}")

            elif msg_type == "typing":
                receiver = str(data.get("to_user_id")) if data.get("to_user_id") else None
                sender = str(data.get("from_user_id")) if data.get("from_user_id") else None

                if receiver and sender and receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "typing",
                        "from_user_id": sender
                    })

            elif msg_type == "ping":
                await safe_send(websocket, {"type": "pong", "time": now()})
                print("🏓 PONG sent")

            elif msg_type == "status":
                target = str(data.get("to_user_id")) if data.get("to_user_id") else None
                if target and target in connected_users:
                    await safe_send(connected_users[target], {
                        "type": "status",
                        "status": data.get("code") or data.get("status"),
                        "message_id": data.get("message_id"),
                        "user_id": user_id
                    })

    except websockets.ConnectionClosed as e:
        print(f"🔌 ConnectionClosed: code={e.code}, reason={e.reason}")
    except Exception as e:
        print(f"💥 Handler error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if user_id and user_id in connected_users:
            if connected_users[user_id] is websocket:
                del connected_users[user_id]
                print(f"❌ CLEANED UP: {user_id} | Remaining: {list(connected_users.keys())}")

PORT = int(os.environ.get("PORT", 10000))

print(f"🚀 Starting server on 0.0.0.0:{PORT}")

# OLD STYLE: No async with, no process_request — works everywhere
start_server = websockets.serve(
    handler,
    "0.0.0.0",
    PORT,
    ping_interval=20,
    ping_timeout=20
)

asyncio.get_event_loop().run_until_complete(start_server)
print("✅ Server is listening")
asyncio.get_event_loop().run_forever()
