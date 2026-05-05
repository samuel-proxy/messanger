import asyncio
import sys
import traceback

# Event loop fix for compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import websockets
import json
import os
from datetime import datetime

# Global state
connected_users = {}

def now():
    return datetime.utcnow().strftime("%H:%M:%S")

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception as e:
        print(f"safe_send error: {e}")

async def process_request(path, request_headers):
    """Handle Render health checks and HTTP requests"""
    print(f"HTTP request: {path}, Upgrade: {request_headers.get('Upgrade', 'none')}")
    
    if request_headers.get("Upgrade", "").lower() == "websocket":
        return None  # Let websockets handle the upgrade
    
    # Respond to Render HTTP health checks
    return (
        200,
        [("Content-Type", "text/plain")],
        b"WebSocket server running"
    )

async def handler(websocket):
    print(f"🔌 NEW CONNECTION from {websocket.remote_address}")
    user_id = None

    try:
        print(f"🤝 Handshake complete. Path: {websocket.path}")
        
        async for message in websocket:
            print(f"📦 RAW: {message[:200]}")
            
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                print(f"❌ JSON error: {e}")
                continue

            msg_type = data.get("type")
            print(f"🎯 TYPE: {msg_type}, DATA: {data}")

            if msg_type == "register":
                user_id = str(data.get("user_id")) if data.get("user_id") else None
                print(f"Register attempt: user_id={user_id}")

                if not user_id or user_id == "null":
                    await safe_send(websocket, {"type": "error", "message": "Invalid user_id"})
                    continue

                # Remove stale connection if same user reconnects
                if user_id in connected_users:
                    old_ws = connected_users[user_id]
                    if old_ws is not websocket and not old_ws.closed:
                        try:
                            await old_ws.close()
                            print(f"Closed stale connection for {user_id}")
                        except:
                            pass

                connected_users[user_id] = websocket
                print(f"✅ REGISTERED: {user_id} | Total online: {len(connected_users)}")

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

            elif msg_type == "status":
                # Forward read/delivered receipts
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
        traceback.print_exc()

    finally:
        if user_id and user_id in connected_users:
            if connected_users[user_id] is websocket:
                del connected_users[user_id]
                print(f"❌ CLEANED UP: {user_id} | Remaining: {list(connected_users.keys())}")

PORT = int(os.environ.get("PORT", 5000))

async def main():
    print(f"🚀 Starting server on 0.0.0.0:{PORT}")
    print(f"Python version: {sys.version}")
    print(f"websockets version: {websockets.__version__}")

    try:
        async with websockets.serve(
            handler,
            "0.0.0.0",
            PORT,
            process_request=process_request,
            ping_interval=20,
            ping_timeout=20
        ):
            print(f"✅ Server is listening on port {PORT}")
            await asyncio.Future()  # Run forever
    except Exception as e:
        print(f"💥 SERVER CRASH: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Shut down by user")
    except Exception as e:
        print(f"💥 FATAL: {type(e).__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)
