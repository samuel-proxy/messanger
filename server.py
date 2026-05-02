import asyncio
import websockets
import os

PORT = int(os.environ.get("PORT", 12345))

clients = set()

async def handler(websocket):
    clients.add(websocket)
    try:
        async for message in websocket:
            for c in clients:
                await c.send(message)
    finally:
        clients.remove(websocket)

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        print("Server running on port", PORT)
        await asyncio.Future()

asyncio.run(main())
