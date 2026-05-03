import socket
import threading
import json

HOST = "0.0.0.0"
PORT = 5000

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen()

print("Server started...")

connected_users = {}  # phone -> socket


def handle_client(client):
    phone = None

    try:
        while True:
            msg = client.recv(1024).decode()
            if not msg:
                break

            data = json.loads(msg)

            # 1. Register user
            if data["type"] == "register":
                phone = data["phone"]
                connected_users[phone] = client
                print(f"{phone} connected")

            # 2. Send message
            elif data["type"] == "message":
                to = data["to"]
                message = data["message"]
                sender = data["from"]

                if to in connected_users:
                    connected_users[to].send(json.dumps({
                        "from": sender,
                        "message": message
                    }).encode())
                else:
                    print("User offline:", to)

    except:
        pass

    if phone and phone in connected_users:
        del connected_users[phone]

    client.close()


while True:
    client, addr = server.accept()
    threading.Thread(target=handle_client, args=(client,)).start()
