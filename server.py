import asyncio
import websockets
import json
import os

connected_users = {}

async def handler(websocket):
    phone = None

    try:
        async for message in websocket:
            data = json.loads(message)
    if data["type"] == "register":
    user_id = data["user_id"]
    connected_users[user_id] = websocket
    
                if receiver in connected_users:
                    await connected_users[receiver].send(json.dumps({
                        "type": "message",
                        "from": sender,
                        "message": msg
                    }))
                else:
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "offline"
                    }))

    except:
        pass

    if phone and phone in connected_users:
        del connected_users[phone]
        print(phone, "disconnected")


# 🔥 IMPORTANT: use Render port
PORT = int(os.environ.get("PORT", 5000))

async def main():
    print("Running on port", PORT)
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
