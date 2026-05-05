import asyncio
import websockets
import json
import os
import logging
from datetime import datetime
from typing import Dict, Set, Optional
from contextlib import asynccontextmanager

# ================ LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================ STATE MANAGEMENT ================
class ConnectionManager:
    """Manages active WebSocket connections and user registration."""
    
    def __init__(self):
        self.active_connections: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.user_tasks: Dict[str, Set[asyncio.Task]] = {}
    
    def register(self, user_id: str, websocket: websockets.WebSocketServerProtocol) -> bool:
        """Register a user connection. Returns False if already connected."""
        if user_id in self.active_connections:
            logger.warning(f"User {user_id} attempted duplicate registration")
            return False
        
        self.active_connections[user_id] = websocket
        self.user_tasks[user_id] = set()
        logger.info(f"✅ REGISTERED: {user_id} | Online: {len(self.active_connections)}")
        return True
    
    def unregister(self, user_id: str) -> bool:
        """Unregister a user and cleanup tasks."""
        if user_id not in self.active_connections:
            return False
        
        # Cancel all pending tasks for this user
        for task in self.user_tasks.get(user_id, set()):
            if not task.done():
                task.cancel()
        
        del self.active_connections[user_id]
        self.user_tasks.pop(user_id, None)
        logger.info(f"❌ UNREGISTERED: {user_id} | Online: {len(self.active_connections)}")
        return True
    
    def get_connection(self, user_id: str) -> Optional[websockets.WebSocketServerProtocol]:
        """Get a user's WebSocket connection."""
        return self.active_connections.get(user_id)
    
    def is_online(self, user_id: str) -> bool:
        """Check if a user is online."""
        return user_id in self.active_connections
    
    def get_online_users(self) -> list:
        """Get list of online user IDs."""
        return list(self.active_connections.keys())
    
    async def broadcast_to_user(self, user_id: str, data: dict) -> bool:
        """Send data to a specific user. Returns False if user offline or error."""
        ws = self.get_connection(user_id)
        if not ws:
            return False
        
        try:
            await ws.send(json.dumps(data))
            return True
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"Connection closed for {user_id}, unregistering")
            self.unregister(user_id)
            return False
        except Exception as e:
            logger.error(f"Error broadcasting to {user_id}: {e}")
            return False

# ================ GLOBAL STATE ================
manager = ConnectionManager()

# ================ UTILITIES ================
def get_timestamp() -> str:
    """Get current UTC timestamp."""
    return datetime.utcnow().isoformat() + "Z"

def validate_user_id(user_id: str) -> bool:
    """Validate user_id format."""
    return (
        isinstance(user_id, str) and 
        user_id.strip() and 
        user_id != "null" and 
        len(user_id) <= 256
    )

async def send_safe(ws: websockets.WebSocketServerProtocol, data: dict) -> bool:
    """Safely send JSON to a WebSocket."""
    try:
        await ws.send(json.dumps(data))
        return True
    except websockets.exceptions.ConnectionClosed:
        logger.debug("Connection already closed")
        return False
    except Exception as e:
        logger.error(f"Error sending data: {e}")
        return False

# ================ REQUEST HANDLER (RENDER HEALTH CHECKS) ================
async def process_request(path: str, request_headers) -> Optional[tuple]:
    """
    Handle HTTP requests before WebSocket upgrade.
    Allows WebSocket upgrade and responds to Render health checks.
    """
    # Allow WebSocket upgrade handshake
    if request_headers.get("Upgrade", "").lower() == "websocket":
        return None  # Continue with WebSocket handshake
    
    # Handle HTTP health checks (Render uses HEAD and GET)
    method = request_headers.get("Method", "").upper()
    path = request_headers.get("Path", "/")
    
    logger.info(f"HTTP {method} {path}")
    
    if path == "/" or path == "/health":
        return (
            200,
            [
                ("Content-Type", "application/json"),
                ("Access-Control-Allow-Origin", "*"),
            ],
            json.dumps({
                "status": "ok",
                "timestamp": get_timestamp(),
                "online_users": len(manager.get_online_users())
            }).encode()
        )
    
    return (
        404,
        [("Content-Type", "text/plain")],
        b"Not Found"
    )

# ================ MESSAGE HANDLERS ================
async def handle_register(user_id: str, websocket: websockets.WebSocketServerProtocol) -> bool:
    """Handle user registration."""
    if not validate_user_id(user_id):
        logger.warning(f"Invalid user_id format: {user_id}")
        await send_safe(websocket, {
            "type": "error",
            "code": "INVALID_USER_ID",
            "message": "Invalid user_id format"
        })
        return False
    
    if not manager.register(user_id, websocket):
        logger.warning(f"User {user_id} already registered")
        await send_safe(websocket, {
            "type": "error",
            "code": "ALREADY_CONNECTED",
            "message": "User already connected from another session"
        })
        return False
    
    await send_safe(websocket, {
        "type": "status",
        "code": "connected",
        "online_count": len(manager.get_online_users()),
        "timestamp": get_timestamp()
    })
    return True

async def handle_message(data: dict, websocket: websockets.WebSocketServerProtocol) -> None:
    """Handle message delivery."""
    sender = data.get("from_user_id", "").strip()
    receiver = data.get("to_user_id", "").strip()
    message = data.get("message", "").strip()
    
    # Validation
    if not sender or not receiver or not message:
        await send_safe(websocket, {
            "type": "error",
            "code": "INVALID_MESSAGE",
            "message": "Missing required fields: from_user_id, to_user_id, message"
        })
        return
    
    if len(message) > 10000:
        await send_safe(websocket, {
            "type": "error",
            "code": "MESSAGE_TOO_LONG",
            "message": "Message exceeds 10000 character limit"
        })
        return
    
    logger.info(f"📨 {sender} → {receiver}: {message[:50]}...")
    
    if manager.is_online(receiver):
        success = await manager.broadcast_to_user(receiver, {
            "type": "message",
            "from_user_id": sender,
            "message": message,
            "timestamp": get_timestamp()
        })
        
        if success:
            await send_safe(websocket, {
                "type": "status",
                "code": "delivered",
                "timestamp": get_timestamp()
            })
        else:
            await send_safe(websocket, {
                "type": "status",
                "code": "delivery_failed",
                "timestamp": get_timestamp()
            })
    else:
        logger.info(f"User {receiver} is offline")
        await send_safe(websocket, {
            "type": "status",
            "code": "offline",
            "timestamp": get_timestamp()
        })

async def handle_typing(data: dict) -> None:
    """Handle typing notifications."""
    sender = data.get("from_user_id", "").strip()
    receiver = data.get("to_user_id", "").strip()
    
    if not sender or not receiver:
        return
    
    if manager.is_online(receiver):
        await manager.broadcast_to_user(receiver, {
            "type": "typing",
            "from_user_id": sender,
            "timestamp": get_timestamp()
        })

# ================ MAIN MESSAGE ROUTER ================
async def handle_connection(websocket: websockets.WebSocketServerProtocol, path: str) -> None:
    """Main WebSocket connection handler."""
    user_id: Optional[str] = None
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON received")
                await send_safe(websocket, {
                    "type": "error",
                    "code": "INVALID_JSON",
                    "message": "Invalid JSON format"
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
                    await handle_message(data, websocket)
            
            # ---- TYPING ----
            elif msg_type == "typing":
                if user_id:
                    await handle_typing(data)
            
            # ---- PING (for keepalive) ----
            elif msg_type == "ping":
                await send_safe(websocket, {
                    "type": "pong",
                    "timestamp": get_timestamp()
                })
            
            else:
                logger.warning(f"Unknown message type: {msg_type}")
    
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed for user: {user_id}")
    except asyncio.CancelledError:
        logger.info(f"Connection cancelled for user: {user_id}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    
    finally:
        if user_id:
            manager.unregister(user_id)

# ================ GRACEFUL SHUTDOWN ================
async def shutdown_event(sig_num, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {sig_num}, initiating graceful shutdown...")
    logger.info(f"Disconnecting {len(manager.get_online_users())} users")
    # Allow time for connections to close properly
    await asyncio.sleep(0.1)

# ================ SERVER STARTUP ================
async def main() -> None:
    """Start the WebSocket server."""
    port = int(os.environ.get("PORT", 5000))
    host = "0.0.0.0"
    
    logger.info(f"🚀 Starting WebSocket server on {host}:{port}")
    
    try:
        async with websockets.serve(
            handle_connection,
            host,
            port,
            process_request=process_request,
            # Ping/pong to detect dead connections
            ping_interval=30,
            ping_timeout=10,
            # Limit message size to 100KB
            max_size=100_000,
            # Compression disabled for better latency
            compression=None,
            # Graceful connection closure
            close_timeout=10,
        ):
            logger.info("✅ Server ready, waiting for connections...")
            await asyncio.Future()  # Run forever
    
    except OSError as e:
        logger.error(f"Failed to bind to {host}:{port}: {e}")
        raise
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        raise

# ================ ENTRY POINT ================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
