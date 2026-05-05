# server.py
import asyncio
import websockets
import json
import os

connected_users = {}  # user_id -> websocket

async def broadcast_presence(user_id, online):
    for uid, ws in connected_users.items():
        if uid != user_id:
            try:
                await ws.send(json.dumps({
                    "type": "presence",
                    "user_id": user_id,
                    "online": online
                }))
            except:
                pass


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

                if not user_id:
                    continue

                connected_users[user_id] = websocket
                print(f"{user_id} connected")

                await broadcast_presence(user_id, True)

            # ---------------- MESSAGE ----------------
            elif msg_type == "message":
                sender = str(data.get("from_user_id"))
                receiver = str(data.get("to_user_id"))
                msg = data.get("message")

                if not sender or not receiver or not msg:
                    continue

                print(f"{sender} -> {receiver}: {msg}")

                if receiver in connected_users:
                    try:
                        await connected_users[receiver].send(json.dumps({
                            "type": "message",
                            "from": sender,
                            "message": msg
                        }))

                        await websocket.send(json.dumps({
                            "type": "status",
                            "status": "delivered"
                        }))

                    except:
                        await websocket.send(json.dumps({
                            "type": "status",
                            "status": "failed"
                        }))
                else:
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "offline"
                    }))

            # ---------------- TYPING ----------------
            elif msg_type == "typing":
                receiver = str(data.get("to_user_id"))
                sender = str(data.get("from_user_id"))

                if receiver in connected_users:
                    await connected_users[receiver].send(json.dumps({
                        "type": "typing",
                        "from": sender
                    }))

    except Exception as e:
        print("Error:", e)

    finally:
        if user_id and user_id in connected_users:
            del connected_users[user_id]
            print(f"{user_id} disconnected")
            await broadcast_presence(user_id, False)


PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("Server running on", PORT)
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
