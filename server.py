import asyncio
import websockets
import json
import os

# user_id -> websocket
connected_users = {}

async def handler(websocket):
    user_id = None

    try:
        async for message in websocket:
            data = json.loads(message)

            # 🔐 REGISTER USER
            if data["type"] == "register":
                user_id = data["user_id"]
                connected_users[user_id] = websocket
                print(f"{user_id} connected")

            # 💬 SEND MESSAGE
            elif data["type"] == "message":
                sender = data["from_user_id"]
                receiver = data["to_user_id"]
                msg = data["message"]

                if receiver in connected_users:
                    # send to receiver
                    await connected_users[receiver].send(json.dumps({
                        "type": "message",
                        "from": sender,
                        "message": msg
                    }))

                    # confirm delivery
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "delivered"
                    }))

                else:
                    # receiver offline
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "offline"
                    }))

    except Exception as e:
        print("Error:", e)

    # 🔌 CLEANUP ON DISCONNECT
    if user_id and user_id in connected_users:
        del connected_users[user_id]
        print(f"{user_id} disconnected")


# 🔥 REQUIRED FOR RENDER
PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("Running on port", PORT)
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
