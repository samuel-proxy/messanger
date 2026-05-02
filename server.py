import asyncio
import websockets
import os

PORT = int(os.environ.get("PORT", 10000))
clients = set()

async def handler(websocket):
    clients.add(websocket)
    print(f"✅ Client connected | Total: {len(clients)}")

    try:
        async for message in websocket:
            print(f"📩 Received: {message}")

            # send to all OTHER clients (no echo)
            for client in clients:
                if client != websocket:
                    try:
                        await client.send(message)
                        print("➡️ Sent to another client")
                    except:
                        pass

    except Exception as e:
        print("❌ Error:", e)

    finally:
        clients.remove(websocket)
        print(f"❌ Client disconnected | Total: {len(clients)}")

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        print(f"🚀 Server running on port {PORT}")
        await asyncio.Future()

asyncio.run(main())
