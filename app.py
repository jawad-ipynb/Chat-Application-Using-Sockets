from flask import Flask, render_template, request, jsonify
import socket
import threading
import json
import queue
import uuid
from datetime import datetime

app = Flask(__name__)

bridge_clients = {}
bridge_lock = threading.RLock()


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_client_id(data=None):
    if data is None:
        data = request.get_json(silent=True) or {}

    client_id = data.get("client_id") or request.args.get("client_id")

    if not client_id:
        client_id = str(uuid.uuid4())

    return str(client_id)


class SocketBridgeClient:
    def __init__(self, username, server_ip, server_port):
        self.username = username
        self.server_ip = server_ip
        self.server_port = int(server_port)
        self.socket = None
        self.connected = False
        self.inbox = queue.Queue()
        self.receiver_thread = None
        self.send_lock = threading.RLock()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.socket.connect((self.server_ip, self.server_port))
        self.socket.settimeout(None)

        self.connected = True

        self.receiver_thread = threading.Thread(
            target=self.receive_loop,
            daemon=True
        )
        self.receiver_thread.start()

        self.send_json({
            "type": "join",
            "username": self.username
        })

    def send_json(self, data):
        with self.send_lock:
            if not self.connected or self.socket is None:
                return False

            try:
                packet = json.dumps(data, ensure_ascii=False) + "\n"
                self.socket.sendall(packet.encode("utf-8"))
                return True
            except OSError:
                self.connected = False
                return False

    def send_chat(self, text):
        msg_id = str(uuid.uuid4())[:8]

        ok = self.send_json({
            "type": "message",
            "msg_id": msg_id,
            "text": text
        })

        if not ok:
            return None

        return msg_id

    def receive_loop(self):
        buffer = ""

        try:
            while self.connected:
                data = self.socket.recv(4096)

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
                        continue

                    if message.get("type") == "chat_message":
                        self.inbox.put(message)

                        self.send_json({
                            "type": "delivery_ack",
                            "msg_id": message.get("msg_id")
                        })

                        self.inbox.put({
                            "type": "ack_sent",
                            "msg_id": message.get("msg_id"),
                            "text": f"Display ACK sent for message {message.get('msg_id')}",
                            "timestamp": now()
                        })

                    else:
                        self.inbox.put(message)

        except OSError:
            pass

        finally:
            self.connected = False

            self.inbox.put({
                "type": "system",
                "text": "Disconnected from TCP server.",
                "timestamp": now()
            })

    def get_messages(self):
        messages = []

        while True:
            try:
                messages.append(self.inbox.get_nowait())
            except queue.Empty:
                break

        return messages

    def disconnect(self):
        try:
            self.send_json({
                "type": "disconnect"
            })
        except Exception:
            pass

        self.connected = False

        try:
            if self.socket:
                self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            if self.socket:
                self.socket.close()
        except OSError:
            pass


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/connect", methods=["POST"])
def connect():
    data = request.get_json(silent=True) or {}

    client_id = get_client_id(data)
    username = str(data.get("username", "")).strip()[:20]
    server_ip = str(data.get("server_ip", "127.0.0.1")).strip()
    server_port = data.get("server_port", 12345)

    if username == "":
        return jsonify({
            "ok": False,
            "error": "Username is required."
        }), 400

    try:
        server_port = int(server_port)

        if server_port < 1 or server_port > 65535:
            raise ValueError

    except ValueError:
        return jsonify({
            "ok": False,
            "error": "Invalid port number."
        }), 400

    try:
        new_bridge = SocketBridgeClient(username, server_ip, server_port)
        new_bridge.connect()

        with bridge_lock:
            old_bridge = bridge_clients.get(client_id)

            if old_bridge:
                old_bridge.disconnect()

            bridge_clients[client_id] = new_bridge

        return jsonify({
            "ok": True,
            "client_id": client_id,
            "username": username,
            "message": "Connected to TCP socket server."
        })

    except Exception as error:
        return jsonify({
            "ok": False,
            "error": str(error)
        }), 500


@app.route("/api/send", methods=["POST"])
def send_message():
    data = request.get_json(silent=True) or {}

    client_id = get_client_id(data)
    text = str(data.get("text", "")).strip()

    if text == "":
        return jsonify({
            "ok": False,
            "error": "Message is empty."
        }), 400

    if len(text) > 1000:
        return jsonify({
            "ok": False,
            "error": "Message too long. Maximum 1000 characters."
        }), 400

    with bridge_lock:
        bridge = bridge_clients.get(client_id)

    if bridge is None or not bridge.connected:
        return jsonify({
            "ok": False,
            "error": "Not connected."
        }), 400

    msg_id = bridge.send_chat(text)

    if msg_id is None:
        return jsonify({
            "ok": False,
            "error": "TCP send failed."
        }), 500

    return jsonify({
        "ok": True,
        "msg_id": msg_id,
        "timestamp": now()
    })


@app.route("/api/poll")
def poll():
    client_id = request.args.get("client_id", "")

    with bridge_lock:
        bridge = bridge_clients.get(client_id)

    if bridge is None:
        return jsonify({
            "ok": False,
            "connected": False,
            "messages": []
        })

    return jsonify({
        "ok": True,
        "connected": bridge.connected,
        "messages": bridge.get_messages()
    })


@app.route("/api/disconnect", methods=["POST"])
def disconnect():
    data = request.get_json(silent=True) or {}

    client_id = get_client_id(data)

    with bridge_lock:
        bridge = bridge_clients.pop(client_id, None)

    if bridge:
        bridge.disconnect()

    return jsonify({
        "ok": True,
        "message": "Disconnected."
    })


@app.route("/api/status")
def status():
    with bridge_lock:
        active_web_clients = sum(
            1 for bridge in bridge_clients.values()
            if bridge.connected
        )

    return jsonify({
        "ok": True,
        "active_web_clients": active_web_clients,
        "timestamp": now()
    })


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        threaded=True,
        use_reloader=False
    )