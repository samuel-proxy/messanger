async def handler(websocket):
    print(f"🔌 NEW CONNECTION from {websocket.remote_address}")
    user_id = None

    try:
        # Test if the handshake completed
        print(f"🤝 Handshake done. Path: {websocket.path}")
        print(f"📋 Headers: {dict(websocket.request_headers)}")

        async for message in websocket:
            print(f"📦 RAW MESSAGE: {message[:200]}...")  # truncate long msgs
            try:
                data = json.loads(message)
                print(f"📨 PARSED: {data}")
            except json.JSONDecodeError as e:
                print(f"❌ JSON ERROR: {e}")
                continue

            msg_type = data.get("type")
            print(f"🎯 TYPE: {msg_type}")

            if msg_type == "register":
                user_id = str(data.get("user_id"))
                if not user_id or user_id == "null":
                    print("⚠️ Invalid user_id, skipping")
                    continue
                connected_users[user_id] = websocket
                print(f"✅ REGISTERED: {user_id}")
                await safe_send(websocket, {
                    "type": "status",
                    "status": "connected",
                    "user_id": user_id
                })

            elif msg_type == "message":
                sender = str(data.get("from_user_id"))
                receiver = str(data.get("to_user_id"))
                msg = data.get("message")
                print(f"💬 MESSAGE: {sender} -> {receiver}: {msg}")

                receiver_ws = connected_users.get(receiver)
                if receiver_ws:
                    await safe_send(receiver_ws, {
                        "type": "message",
                        "from_user_id": sender,
                        "message": msg,
                        "time": now()
                    })
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "delivered"
                    })
                    print(f"✅ DELIVERED to {receiver}")
                else:
                    await safe_send(websocket, {
                        "type": "status",
                        "status": "offline",
                        "user_id": receiver
                    })
                    print(f"⚠️ OFFLINE: {receiver} not found")

            elif msg_type == "ping":
                await safe_send(websocket, {"type": "pong"})
                print("🏓 PONG sent")

    except websockets.ConnectionClosed as e:
        print(f"🔌 CONNECTION CLOSED: code={e.code}, reason={e.reason}")
    except Exception as e:
        print(f"💥 FATAL ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if user_id and user_id in connected_users:
            if connected_users[user_id] is websocket:
                del connected_users[user_id]
                print(f"❌ CLEANED UP: {user_id}")
