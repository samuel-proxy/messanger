import asyncio
import websockets
import json
import mysql.connector

connected_users = {}

db = mysql.connector.connect(
    host="sql111.infinityfree.com",
    user="if0_41819704",
    password="5dAV1g7hIZ",
    database="if0_41819704_chat_app"
)

cursor = db.cursor()


# SAVE MESSAGE TO DATABASE
def save_message(sender, receiver, message):
    sql = "INSERT INTO messages (sender, receiver, message) VALUES (%s, %s, %s)"
    cursor.execute(sql, (sender, receiver, message))
    db.commit()


# GET OFFLINE MESSAGES
def get_offline_messages(phone):
    sql = "SELECT id, sender, message FROM messages WHERE receiver=%s AND status='sent'"
    cursor.execute(sql, (phone,))
    return cursor.fetchall()


# MARK AS DELIVERED
def mark_delivered(msg_id):
    sql = "UPDATE messages SET status='delivered' WHERE id=%s"
    cursor.execute(sql, (msg_id,))
    db.commit()


async def handler(websocket):

    phone = None

    try:
        async for message in websocket:
            data = json.loads(message)

            # REGISTER USER
            if data["type"] == "register":
                phone = data["phone"]
                connected_users[phone] = websocket
                print(phone, "connected")

                # SEND OFFLINE MESSAGES
                offline_msgs = get_offline_messages(phone)

                for msg in offline_msgs:
                    msg_id, sender, text = msg

                    await websocket.send(json.dumps({
                        "from": sender,
                        "message": text
                    }))

                    mark_delivered(msg_id)

            # SEND MESSAGE
            elif data["type"] == "message":
                sender = data["from"]
                receiver = data["to"]
                msg = data["message"]

                # IF ONLINE
                if receiver in connected_users:
                    await connected_users[receiver].send(json.dumps({
                        "from": sender,
                        "message": msg
                    }))
                else:
                    # OFFLINE → SAVE
                    save_message(sender, receiver, msg)
                    print("Stored offline message")

    except:
        pass

    if phone and phone in connected_users:
        del connected_users[phone]
        print(phone, "disconnected")


async def main():
    print("WebSocket server running...")
    async with websockets.serve(handler, "0.0.0.0", 5000):
        await asyncio.Future()

asyncio.run(main())
