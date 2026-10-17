import socket
import threading
import json
import csv
import os
import uuid
from datetime import datetime

HOST = "0.0.0.0"
PORT = 12345

clients = {}
delivery_tracking = {}

lock = threading.RLock()
log_lock = threading.RLock()

shutdown_event = threading.Event()
shutdown_event.set()

LOG_FILE = "proof_log.csv"


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def create_log_file():
    if not os.path.exists(LOG_FILE):
        with log_lock:
            with open(LOG_FILE, "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([
                    "timestamp",
                    "event",
                    "message_id",
                    "username",
                    "client_address",
                    "details",
                    "active_clients"
                ])


def get_active_clients_count():
    with lock:
        return len(clients)


def write_log(event, message_id="", username="", client_address="", details=""):
    with log_lock:
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([
                now(),
                event,
                message_id,
                username,
                client_address,
                details,
                get_active_clients_count()
            ])


def send_json(client_socket, data):
    try:
        packet = json.dumps(data, ensure_ascii=False) + "\n"
        client_socket.sendall(packet.encode("utf-8"))
        return True
    except OSError:
        return False


def get_online_users():
    with lock:
        return [info["username"] for info in clients.values()]


def broadcast_users():
    users = get_online_users()

    with lock:
        sockets = list(clients.keys())

    for client_socket in sockets:
        ok = send_json(client_socket, {
            "type": "users",
            "users": users,
            "timestamp": now()
        })

        if not ok:
            remove_client(client_socket)


def broadcast_system_message(text, exclude_socket=None):
    with lock:
        sockets = list(clients.keys())

    for client_socket in sockets:
        if client_socket == exclude_socket:
            continue

        ok = send_json(client_socket, {
            "type": "system",
            "text": text,
            "timestamp": now()
        })

        if not ok:
            remove_client(client_socket)


def cleanup_delivery_records_for_disconnected_client(disconnected_socket):
    updates_for_senders = []

    with lock:
        for message_id in list(delivery_tracking.keys()):
            record = delivery_tracking[message_id]

            sender_socket = record["sender_socket"]

            if sender_socket == disconnected_socket:
                delivery_tracking.pop(message_id, None)
                continue

            if disconnected_socket in record["expected_recipients"]:
                record["expected_recipients"].discard(disconnected_socket)
                record["acked_recipients"].discard(disconnected_socket)

                delivered = len(record["acked_recipients"])
                total = len(record["expected_recipients"])

                updates_for_senders.append((
                    sender_socket,
                    message_id,
                    delivered,
                    total
                ))

                if delivered == total:
                    delivery_tracking.pop(message_id, None)

    for sender_socket, message_id, delivered, total in updates_for_senders:
        send_json(sender_socket, {
            "type": "delivery_update",
            "msg_id": message_id,
            "delivered": delivered,
            "total": total,
            "text": f"Delivered to {delivered}/{total} active clients. One receiver disconnected.",
            "timestamp": now()
        })


def remove_client(client_socket):
    with lock:
        if client_socket not in clients:
            return

        username = clients[client_socket]["username"]
        address = clients[client_socket]["address"]

        del clients[client_socket]

    cleanup_delivery_records_for_disconnected_client(client_socket)

    try:
        client_socket.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

    try:
        client_socket.close()
    except OSError:
        pass

    print(f"[DISCONNECTED] {username} {address}")

    write_log(
        event="CLIENT_DISCONNECTED",
        username=username,
        client_address=str(address),
        details=f"{username} disconnected"
    )

    broadcast_system_message(f"{username} left the chat.")
    broadcast_users()


def make_unique_username(username, client_address):
    username = str(username).strip()

    if username == "":
        username = f"User_{client_address[1]}"

    username = username[:20]

    with lock:
        existing_names = {
            info["username"].lower()
            for info in clients.values()
        }

    original_username = username
    counter = 2

    while username.lower() in existing_names:
        username = f"{original_username}_{counter}"
        counter += 1

    return username


def handle_join(client_socket, client_address, data):
    username = make_unique_username(data.get("username", ""), client_address)

    with lock:
        clients[client_socket] = {
            "username": username,
            "address": client_address,
            "joined_at": now()
        }

    print(f"[CONNECTED] {username} from {client_address}")

    write_log(
        event="CLIENT_CONNECTED",
        username=username,
        client_address=str(client_address),
        details=f"{username} joined the chat"
    )

    send_json(client_socket, {
        "type": "join_ok",
        "username": username,
        "text": f"Connected as {username}",
        "timestamp": now()
    })

    broadcast_system_message(f"{username} joined the chat.")
    broadcast_users()


def generate_message_id():
    return str(uuid.uuid4())[:8]


def handle_chat_message(sender_socket, data):
    with lock:
        if sender_socket not in clients:
            send_json(sender_socket, {
                "type": "error",
                "text": "You must join before sending messages.",
                "timestamp": now()
            })
            return

        sender_username = clients[sender_socket]["username"]
        sender_address = clients[sender_socket]["address"]

    message_text = str(data.get("text", "")).strip()

    if message_text == "":
        return

    if len(message_text) > 1000:
        send_json(sender_socket, {
            "type": "error",
            "text": "Message too long. Maximum 1000 characters allowed.",
            "timestamp": now()
        })
        return

    message_id = data.get("msg_id") or generate_message_id()
    message_id = str(message_id)[:20]

    timestamp = now()

    with lock:
        if message_id in delivery_tracking:
            message_id = generate_message_id()

        recipients = [
            client_socket
            for client_socket in clients.keys()
            if client_socket != sender_socket
        ]

        delivery_tracking[message_id] = {
            "sender_socket": sender_socket,
            "expected_recipients": set(recipients),
            "acked_recipients": set(),
            "sender_username": sender_username,
            "message_text": message_text,
            "created_at": timestamp
        }

    print(f"[MESSAGE RECEIVED] {sender_username}: {message_text}")

    write_log(
        event="SERVER_RECEIVED_MESSAGE",
        message_id=message_id,
        username=sender_username,
        client_address=str(sender_address),
        details=message_text
    )

    send_json(sender_socket, {
        "type": "server_received",
        "msg_id": message_id,
        "text": "Server received your message.",
        "expected_receivers": len(recipients),
        "timestamp": timestamp
    })

    failed_recipients = []

    for receiver_socket in recipients:
        ok = send_json(receiver_socket, {
            "type": "chat_message",
            "msg_id": message_id,
            "from": sender_username,
            "text": message_text,
            "timestamp": timestamp
        })

        if not ok:
            failed_recipients.append(receiver_socket)

    if failed_recipients:
        with lock:
            if message_id in delivery_tracking:
                for failed_socket in failed_recipients:
                    delivery_tracking[message_id]["expected_recipients"].discard(failed_socket)

        for failed_socket in failed_recipients:
            remove_client(failed_socket)

    with lock:
        if message_id in delivery_tracking:
            total = len(delivery_tracking[message_id]["expected_recipients"])
            delivered = len(delivery_tracking[message_id]["acked_recipients"])
        else:
            total = 0
            delivered = 0

    if total == 0:
        send_json(sender_socket, {
            "type": "delivery_update",
            "msg_id": message_id,
            "delivered": 0,
            "total": 0,
            "text": "No other active clients online.",
            "timestamp": now()
        })

        with lock:
            delivery_tracking.pop(message_id, None)

        write_log(
            event="NO_RECEIVERS_ONLINE",
            message_id=message_id,
            username=sender_username,
            client_address=str(sender_address),
            details="Message had no active receivers"
        )


def handle_delivery_ack(receiver_socket, data):
    message_id = data.get("msg_id")

    if not message_id:
        return

    with lock:
        if receiver_socket not in clients:
            return

        receiver_username = clients[receiver_socket]["username"]
        receiver_address = clients[receiver_socket]["address"]

        if message_id not in delivery_tracking:
            return

        record = delivery_tracking[message_id]

        if receiver_socket in record["expected_recipients"]:
            record["acked_recipients"].add(receiver_socket)

        delivered = len(record["acked_recipients"])
        total = len(record["expected_recipients"])
        sender_socket = record["sender_socket"]
        sender_username = record["sender_username"]

    write_log(
        event="RECEIVER_DELIVERY_ACK",
        message_id=message_id,
        username=receiver_username,
        client_address=str(receiver_address),
        details=f"{receiver_username} displayed message {message_id}"
    )

    print(f"[DELIVERY ACK] {receiver_username} received message {message_id}")

    send_json(sender_socket, {
        "type": "delivery_update",
        "msg_id": message_id,
        "delivered": delivered,
        "total": total,
        "text": f"Delivered to {delivered}/{total} clients.",
        "timestamp": now()
    })

    if delivered == total:
        write_log(
            event="MESSAGE_FULLY_DELIVERED",
            message_id=message_id,
            username=sender_username,
            details=f"Message delivered to all {total} recipients"
        )

        with lock:
            delivery_tracking.pop(message_id, None)


def process_json_message(client_socket, client_address, message):
    message_type = message.get("type")

    if message_type == "join":
        handle_join(client_socket, client_address, message)

    elif message_type == "message":
        handle_chat_message(client_socket, message)

    elif message_type == "delivery_ack":
        handle_delivery_ack(client_socket, message)

    elif message_type == "disconnect":
        raise ConnectionAbortedError("Client requested disconnect")

    else:
        send_json(client_socket, {
            "type": "error",
            "text": f"Unknown message type: {message_type}",
            "timestamp": now()
        })


def handle_client(client_socket, client_address):
    buffer = ""

    try:
        while shutdown_event.is_set():
            data = client_socket.recv(4096)

            if not data:
                break

            buffer += data.decode("utf-8", errors="replace")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                if line.strip() == "":
                    continue

                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    send_json(client_socket, {
                        "type": "error",
                        "text": "Invalid JSON packet received.",
                        "timestamp": now()
                    })
                    continue

                process_json_message(client_socket, client_address, message)

    except ConnectionAbortedError:
        pass

    except ConnectionResetError:
        pass

    except OSError:
        pass

    except Exception as error:
        print(f"[ERROR] {client_address}: {error}")

        write_log(
            event="CLIENT_ERROR",
            client_address=str(client_address),
            details=str(error)
        )

    finally:
        remove_client(client_socket)


def admin_console():
    print()
    print("Admin commands:")
    print("shutdown / stop / exit / quit  -> close TCP server")
    print("users                           -> show online users")
    print("help                            -> show commands")
    print()

    while shutdown_event.is_set():
        try:
            command = input().strip().lower()
        except EOFError:
            break
        except KeyboardInterrupt:
            shutdown_event.clear()
            break

        if command in ["shutdown", "stop", "exit", "quit"]:
            print("[ADMIN] Shutdown command received.")
            shutdown_event.clear()
            break

        elif command == "users":
            users = get_online_users()

            if users:
                print("[ONLINE USERS]", ", ".join(users))
            else:
                print("[ONLINE USERS] No users connected.")

        elif command == "help":
            print("Commands:")
            print("shutdown / stop / exit / quit  -> close TCP server")
            print("users                           -> show online users")
            print("help                            -> show commands")

        elif command != "":
            print("[ADMIN] Unknown command. Type help.")


def close_all_clients():
    with lock:
        sockets = list(clients.keys())

    for client_socket in sockets:
        send_json(client_socket, {
            "type": "system",
            "text": "Server is shutting down.",
            "timestamp": now()
        })

    for client_socket in sockets:
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            client_socket.close()
        except OSError:
            pass

    with lock:
        clients.clear()
        delivery_tracking.clear()


def start_server():
    create_log_file()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server_socket.bind((HOST, PORT))

    server_socket.listen(20)

    server_socket.settimeout(1.0)

    print("=" * 70)
    print("RAW TCP SOCKET CHAT SERVER STARTED")
    print("=" * 70)
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print("Protocol: TCP")
    print("Message Format: JSON over TCP using newline framing")
    print("Proof Log:", LOG_FILE)
    print("Waiting for clients...")
    print("=" * 70)

    write_log(
        event="SERVER_STARTED",
        details=f"TCP server started on {HOST}:{PORT}"
    )

    admin_thread = threading.Thread(
        target=admin_console,
        daemon=True
    )

    admin_thread.start()

    try:
        while shutdown_event.is_set():
            try:
                client_socket, client_address = server_socket.accept()
            except socket.timeout:
                continue

            print(f"[NEW CONNECTION] {client_address}")

            thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True
            )

            thread.start()

            print(f"[ACTIVE THREADS] {threading.active_count() - 1}")

    except KeyboardInterrupt:
        print("\n[SERVER STOPPING] Ctrl + C received.")
        shutdown_event.clear()

    finally:
        print("[SERVER STOPPING] Closing all client connections...")

        shutdown_event.clear()

        close_all_clients()

        try:
            server_socket.close()
        except OSError:
            pass

        write_log(
            event="SERVER_STOPPED",
            details="TCP server stopped"
        )

        print("[SERVER CLOSED]")


if __name__ == "__main__":
    start_server()