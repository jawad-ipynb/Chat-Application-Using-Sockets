import socket
import threading
import json
import uuid

SERVER_IP = "127.0.0.1"
SERVER_PORT = 12345


def send_json(sock, data):
    packet = json.dumps(data) + "\n"
    sock.sendall(packet.encode("utf-8"))


def receive_loop(sock):
    buffer = ""

    while True:
        try:
            data = sock.recv(4096)

            if not data:
                print("\n[DISCONNECTED FROM SERVER]")
                break

            buffer += data.decode("utf-8")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                if line.strip() == "":
                    continue

                message = json.loads(line)
                message_type = message.get("type")

                if message_type == "chat_message":
                    print(f"\n[{message.get('from')}] {message.get('text')}")

                    send_json(sock, {
                        "type": "delivery_ack",
                        "msg_id": message.get("msg_id")
                    })

                    print(f"[ACK SENT] Message ID: {message.get('msg_id')}")

                elif message_type == "server_received":
                    print(f"\n[SERVER RECEIVED] Message ID: {message.get('msg_id')}")

                elif message_type == "delivery_update":
                    print(f"\n[DELIVERY] {message.get('text')} Message ID: {message.get('msg_id')}")

                elif message_type == "users":
                    print(f"\n[ONLINE USERS] {', '.join(message.get('users', []))}")

                elif message_type == "system":
                    print(f"\n[SERVER] {message.get('text')}")

                elif message_type == "join_ok":
                    print(f"\n[CONNECTED] {message.get('text')}")

        except:
            break


def main():
    print("=" * 50)
    print("TERMINAL TCP CHAT CLIENT")
    print("=" * 50)

    server_ip = input("Enter server IP [127.0.0.1]: ").strip()

    if server_ip == "":
        server_ip = SERVER_IP

    username = input("Enter username: ").strip()

    if username == "":
        username = "TerminalUser"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, SERVER_PORT))

    send_json(sock, {
        "type": "join",
        "username": username
    })

    thread = threading.Thread(
        target=receive_loop,
        args=(sock,),
        daemon=True
    )
    thread.start()

    print("\nType messages. Type /quit to exit.\n")

    while True:
        text = input()

        if text.lower() == "/quit":
            send_json(sock, {
                "type": "disconnect"
            })
            break

        msg_id = str(uuid.uuid4())[:8]

        send_json(sock, {
            "type": "message",
            "msg_id": msg_id,
            "text": text
        })

        print(f"[SENT] Message ID: {msg_id}")

    sock.close()


if __name__ == "__main__":
    main()