"""
WebSocket Chat Server for Render.com
Properly handles HTTP health checks AND WebSocket connections
"""

import asyncio
import json
import os
import logging
from datetime import datetime
from typing import Dict, Optional, Set

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import websockets
from websockets.asyncio.server import serve

# ================ STATE MANAGEMENT ================
class ConnectionManager:
    """Manages active WebSocket connections."""
    
    def __init__(self):
        self.connections: Dict[str, any] = {}
    
    def register(self, user_id: str, ws) -> bool:
        """Register user connection."""
        if user_id in self.connections:
            logger.warning(f"Duplicate registration: {user_id}")
            return False
        self.connections[user_id] = ws
        logger.info(f"✅ Connected: {user_id} ({len(self.connections)} online)")
        return True
    
    def unregister(self, user_id: str):
        """Unregister user connection."""
        if user_id in self.connections:
            del self.connections[user_id]
            logger.info(f"❌ Disconnected: {user_id} ({len(self.connections)} online)")
    
    def is_online(self, user_id: str) -> bool:
        """Check if user is online."""
        return user_id in self.connections
    
    def get_connection(self, user_id: str):
        """Get user's connection."""
        return self.connections.get(user_id)
    
    def get_online_count(self) -> int:
        """Get number of online users."""
        return len(self.connections)

# ================ GLOBAL STATE ================
manager = ConnectionManager()

# ================ UTILITIES ================
def get_timestamp() -> str:
    """Get ISO 8601 timestamp."""
    return datetime.utcnow().isoformat() + "Z"

def validate_user_id(user_id: str) -> bool:
    """Validate user_id."""
    return (
        isinstance(user_id, str) and 
        0 < len(user_id.strip()) <= 256 and 
        user_id.strip() != "null"
    )

async def send_safe(ws, data: dict) -> bool:
    """Safely send JSON."""
    try:
        await ws.send(json.dumps(data))
        return True
    except:
        return False

# ================ REQUEST ROUTING ================
async def process_request(path: str, request_headers):
    """
    Handle both HTTP and WebSocket requests.
    This is called BEFORE the WebSocket upgrade.
    - Return None to allow WebSocket upgrade
    - Return (status, headers, body) for HTTP responses
    """
    
    # Check if this is a WebSocket upgrade request
    upgrade = request_headers.get("Upgrade", "").lower()
    connection = request_headers.get("Connection", "").lower()
    
    if upgrade == "websocket" and "upgrade" in connection:
        # Allow WebSocket upgrade to proceed
        return None
    
    # Handle as HTTP health check
    if path in ("/", "/health", "/status", "/ping"):
        body = json.dumps({
            "status": "ok",
            "timestamp": get_timestamp(),
            "online_users": manager.get_online_count()
        }).encode()
        
        return (
            200,
            [
                ("Content-Type", "application/json"),
                ("Content-Length", str(len(body))),
                ("Access-Control-Allow-Origin", "*"),
            ],
            body
        )
    
    # Default 404
    return (
        404,
        [("Content-Type", "text/plain")],
        b"Not Found"
    )

# ================ MESSAGE HANDLERS ================
async def handle_register(user_id: str, ws) -> bool:
    """Handle registration message."""
    if not validate_user_id(user_id):
        await send_safe(ws, {
            "type": "error",
            "code": "INVALID_USER_ID",
            "message": "Invalid user ID"
        })
        return False
    
    if not manager.register(user_id, ws):
        await send_safe(ws, {
            "type": "error",
            "code": "ALREADY_CONNECTED",
            "message": "User already connected"
        })
        return False
    
    await send_safe(ws, {
        "type": "status",
        "code": "connected",
        "online_count": manager.get_online_count(),
        "timestamp": get_timestamp()
    })
    return True

async def handle_message(sender: str, receiver: str, message: str, ws) -> None:
    """Handle message delivery."""
    message = message.strip()
    
    if not message:
        await send_safe(ws, {
            "type": "error",
            "code": "EMPTY_MESSAGE",
            "message": "Message cannot be empty"
        })
        return
    
    if len(message) > 10000:
        await send_safe(ws, {
            "type": "error",
            "code": "MESSAGE_TOO_LONG",
            "message": "Message exceeds 10000 characters"
        })
        return
    
    logger.info(f"📨 {sender} → {receiver}: {message[:50]}...")
    
    # Send to receiver if online
    if manager.is_online(receiver):
        receiver_ws = manager.get_connection(receiver)
        success = await send_safe(receiver_ws, {
            "type": "message",
            "from_user_id": sender,
            "message": message,
            "timestamp": get_timestamp()
        })
        
        # Notify sender
        await send_safe(ws, {
            "type": "status",
            "code": "delivered" if success else "delivery_failed",
            "timestamp": get_timestamp()
        })
    else:
        # Notify sender that receiver is offline
        await send_safe(ws, {
            "type": "status",
            "code": "offline",
            "timestamp": get_timestamp()
        })

async def handle_typing(sender: str, receiver: str) -> None:
    """Handle typing indicator."""
    if manager.is_online(receiver):
        receiver_ws = manager.get_connection(receiver)
        await send_safe(receiver_ws, {
            "type": "typing",
            "from_user_id": sender,
            "timestamp": get_timestamp()
        })

# ================ WEBSOCKET HANDLER ================
async def websocket_handler(websocket):
    """Main WebSocket connection handler."""
    user_id: Optional[str] = None
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await send_safe(websocket, {
                    "type": "error",
                    "code": "INVALID_JSON",
                    "message": "Invalid JSON"
                })
                continue
            
            msg_type = data.get("type", "").strip().lower()
            
            # ---- REGISTER ----
            if msg_type == "register":
                user_id = str(data.get("user_id", "")).strip()
                await handle_register(user_id, websocket)
            
            # ---- MESSAGE ----
            elif msg_type == "message":
                if not user_id:
                    await send_safe(websocket, {
                        "type": "error",
                        "code": "NOT_REGISTERED",
                        "message": "Must register first"
                    })
                else:
                    sender = str(data.get("from_user_id", "")).strip()
                    receiver = str(data.get("to_user_id", "")).strip()
                    message = data.get("message", "")
                    
                    if sender and receiver:
                        await handle_message(sender, receiver, message, websocket)
            
            # ---- TYPING ----
            elif msg_type == "typing":
                if user_id:
                    sender = str(data.get("from_user_id", "")).strip()
                    receiver = str(data.get("to_user_id", "")).strip()
                    if sender and receiver:
                        await handle_typing(sender, receiver)
            
            # ---- PING/PONG ----
            elif msg_type == "ping":
                await send_safe(websocket, {
                    "type": "pong",
                    "timestamp": get_timestamp()
                })
    
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed: {user_id}")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    
    finally:
        if user_id:
            manager.unregister(user_id)

# ================ SERVER STARTUP ================
async def main():
    """Start WebSocket server."""
    port = int(os.environ.get("PORT", 5000))
    host = "0.0.0.0"
    
    logger.info(f"🚀 Starting on {host}:{port}")
    
    try:
        async with serve(
            websocket_handler,
            host,
            port,
            process_request=process_request,
            ping_interval=30,
            ping_timeout=10,
            max_size=100_000,
            compression=None,
            close_timeout=10,
        ):
            logger.info("✅ Server ready")
            await asyncio.Future()  # Run forever
    
    except OSError as e:
        logger.error(f"Bind error: {e}")
        raise

# ================ ENTRY POINT ================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal: {e}")
        exit(1)
