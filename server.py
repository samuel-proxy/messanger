import asyncio
import websockets
import json

connected_users = {}  # phone -> websocket

async def handler(websocket):
    phone = None

    try:
        async for message in websocket:
            data = json.loads(message)

            # REGISTER USER
            if data["type"] == "register":
                phone = data["phone"]
                connected_users[phone] = websocket
                print(f"{phone} connected")

            # CHECK USER PRESENCE
            elif data["type"] == "check":
                receiver = data["to"]

                await websocket.send(json.dumps({
                    "type": "presence",
                    "online": receiver in connected_users
                }))

            # SEND MESSAGE
            elif data["type"] == "message":
                sender = data["from"]
                receiver = data["to"]
                msg = data["message"]

                if receiver in connected_users:
                    # deliver message
                    await connected_users[receiver].send(json.dumps({
                        "type": "message",
                        "from": sender,
                        "message": msg
                    }))

                    # notify sender
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "delivered"
                    }))
                else:
                    # offline
                    await websocket.send(json.dumps({
                        "type": "status",
                        "status": "offline",
                        "message": f"{receiver} is offline"
                    }))

    except:
        pass

    # CLEANUP
    if phone and phone in connected_users:
        del connected_users[phone]
        print(f"{phone} disconnected")


async def main():
    print("WebSocket server running...")
    async with websockets.serve(handler, "0.0.0.0", 5000):
        await asyncio.Future()

asyncio.run(main())
