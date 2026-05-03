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

            # SEND MESSAGE
            elif data["type"] == "message":
                sender = data["from"]
                receiver = data["to"]
                msg = data["message"]

                if receiver in connected_users:
                    await connected_users[receiver].send(json.dumps({
                        "from": sender,
                        "message": msg
                    }))
                else:
                    print("User offline:", receiver)

    except:
        pass

    # cleanup on disconnect
    if phone and phone in connected_users:
        del connected_users[phone]
        print(f"{phone} disconnected")


async def main():
    print("WebSocket server running on port 5000...")
    async with websockets.serve(handler, "0.0.0.0", 5000):
        await asyncio.Future()

asyncio.run(main())
