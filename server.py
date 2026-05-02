import asyncio
import websockets
import os

PORT = int(os.environ.get("PORT", 10000))
clients = set()

async def handler(websocket):
    clients.add(websocket)
    print("Client connected")

    try:
        async for message in websocket:
            print("Received:", message)

            # send to everyone EXCEPT sender
            for client in clients:
                if client != websocket:
                    await client.send(message)

    except:
        pass
    finally:
        clients.remove(websocket)
        print("Client disconnected")

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        print("Server running on port", PORT)
        await asyncio.Future()

asyncio.run(main())
