import asyncio
import websockets
import json
import os
from datetime import datetime

# Dictionary to map user_id strings to websocket objects
connected_users = {}

def now():
    return datetime.utcnow().isoformat()

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception:
        # Connection likely closed
        pass

async def process_request(path, request_headers):
    # Respond to Render/Uptime health checks
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return (200, [("Content-Type", "text/plain")], b"OK")
    return None

async def handler(websocket):
    user_id = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            # --- REGISTRATION ---
            if msg_type == "register":
                user_id = str(data.get("user_id", ""))
                if user_id and user_id != "null":
                    connected_users[user_id] = websocket
                    print(f"✅ CONNECTED: {user_id}")
                    await safe_send(websocket, {"type": "status", "code": "connected"})

            # --- PRIVATE MESSAGE ---
            elif msg_type == "message":
                sender = str(data.get("from_user_id"))
                receiver = str(data.get("to_user_id"))
                text = data.get("message")

                receiver_ws = connected_users.get(receiver)
                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender,
                        "message": text,
                        "timestamp": now()
                    })
                    await safe_send(websocket, {"type": "status", "code": "delivered"})
                else:
                    await safe_send(websocket, {"type": "status", "code": "offline", "user_id": receiver})

            # --- TYPING INDICATOR ---
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))
                if receiver in connected_users:
                    await safe_send(connected_users[receiver], {
                        "type": "typing",
                        "from_user_id": sender
                    })

    except Exception as e:
        print(f"Handler Error: {e}")
    finally:
        if user_id in connected_users:
            del connected_users[user_id]
            print(f"❌ DISCONNECTED: {user_id}")

async def main():
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Server starting on port {port}")
    async with websockets.serve(
        handler, "0.0.0.0", port,
        process_request=process_request,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
