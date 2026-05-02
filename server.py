from fastapi import FastAPI, WebSocket
import uvicorn
import os

app = FastAPI()
clients = []

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.append(ws)

    try:
        while True:
            data = await ws.receive_text()
            print("Received:", data)

            # send to everyone EXCEPT sender
            for c in clients:
                if c != ws:
                    await c.send_text(data)

    except:
        clients.remove(ws)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
