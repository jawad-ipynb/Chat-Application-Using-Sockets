let connected = false;
let pollTimer = null;
let myUsername = "";

function createClientId() {
    if (window.crypto && crypto.randomUUID) {
        return crypto.randomUUID();
    }

    return "client-" + Date.now() + "-" + Math.floor(Math.random() * 100000);
}

const clientId = sessionStorage.getItem("tcp_chat_client_id") || createClientId();
sessionStorage.setItem("tcp_chat_client_id", clientId);

const messagesDiv = document.getElementById("messages");
const proofLog = document.getElementById("proofLog");
const usersList = document.getElementById("usersList");

const connectBtn = document.getElementById("connectBtn");
const disconnectBtn = document.getElementById("disconnectBtn");
const sendBtn = document.getElementById("sendBtn");
const messageBox = document.getElementById("messageBox");

const statusText = document.getElementById("connectionStatus");
const statusPill = document.getElementById("statusPill");


function safeText(value) {
    return String(value ?? "");
}


function addMessage(type, sender, text, msgId = "") {
    const div = document.createElement("div");
    div.className = `message ${type}`;

    if (type === "system") {
        div.textContent = safeText(text);
    } else {
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = `${sender}${msgId ? " | ID: " + msgId : ""}`;

        const body = document.createElement("div");
        body.textContent = safeText(text);

        div.appendChild(meta);
        div.appendChild(body);
    }

    messagesDiv.appendChild(div);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
}


function addProof(text, status = "success") {
    const div = document.createElement("div");
    div.className = `proof-item ${status}`;

    const body = document.createElement("div");
    body.textContent = safeText(text);

    const time = document.createElement("div");
    time.className = "time";
    time.textContent = new Date().toLocaleTimeString();

    div.appendChild(body);
    div.appendChild(time);

    proofLog.prepend(div);
}


function updateConnectionUI(isConnected) {
    connected = isConnected;

    connectBtn.disabled = isConnected;
    disconnectBtn.disabled = !isConnected;
    sendBtn.disabled = !isConnected;
    messageBox.disabled = !isConnected;

    statusText.innerText = isConnected
        ? `Connected as ${myUsername}`
        : "Not connected";

    statusPill.innerText = isConnected ? "ONLINE" : "OFFLINE";

    if (isConnected) {
        statusPill.classList.add("status-online");
        messageBox.focus();
    } else {
        statusPill.classList.remove("status-online");
    }
}


async function connectToServer() {
    const serverIp = document.getElementById("serverIp").value.trim();
    const serverPort = document.getElementById("serverPort").value.trim();
    const username = document.getElementById("username").value.trim();

    if (!username) {
        alert("Enter username first.");
        return;
    }

    try {
        const response = await fetch("/api/connect", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                client_id: clientId,
                username: username,
                server_ip: serverIp,
                server_port: serverPort
            })
        });

        const result = await response.json();

        if (!result.ok) {
            alert("Connection failed: " + result.error);
            return;
        }

        myUsername = result.username || username;

        updateConnectionUI(true);

        addMessage("system", "SERVER", `Connected to TCP server as ${myUsername}`);

        addProof(
            "TCP CONNECTION ✓ Browser UI connected to Flask bridge, and Flask bridge connected to raw TCP socket server.",
            "success"
        );

        if (pollTimer) {
            clearInterval(pollTimer);
        }

        pollTimer = setInterval(pollServer, 500);

        await pollServer();

    } catch (error) {
        addProof("Connection error: " + error.message, "error");
    }
}


async function disconnectFromServer() {
    try {
        await fetch("/api/disconnect", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                client_id: clientId
            })
        });
    } catch (error) {
        addProof("Disconnect request failed: " + error.message, "warning");
    }

    updateConnectionUI(false);

    if (pollTimer) {
        clearInterval(pollTimer);
    }

    addMessage("system", "SERVER", "Disconnected from server.");

    addProof(
        "DISCONNECTED ✓ Web client disconnected from TCP socket server.",
        "warning"
    );
}


async function sendMessage() {
    const text = messageBox.value.trim();

    if (!text || !connected) {
        return;
    }

    messageBox.value = "";

    try {
        const response = await fetch("/api/send", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                client_id: clientId,
                text: text
            })
        });

        const result = await response.json();

        if (!result.ok) {
            addProof("Send failed: " + result.error, "error");
            return;
        }

        addMessage("me", myUsername, text, result.msg_id);

        addProof(
            `SENT ✓ Message ${result.msg_id} sent from website UI to Flask bridge.`,
            "success"
        );

    } catch (error) {
        addProof("Send error: " + error.message, "error");
    }
}


async function pollServer() {
    if (!connected) {
        return;
    }

    try {
        const response = await fetch(`/api/poll?client_id=${encodeURIComponent(clientId)}`);

        const result = await response.json();

        if (!result.connected) {
            updateConnectionUI(false);

            addProof(
                "CONNECTION LOST ⚠ TCP server connection closed.",
                "warning"
            );

            if (pollTimer) {
                clearInterval(pollTimer);
            }

            return;
        }

        result.messages.forEach(handleIncomingMessage);

    } catch (error) {
        addProof("Polling error: " + error.message, "warning");
    }
}


function handleIncomingMessage(message) {
    const type = message.type;

    if (type === "join_ok") {
        if (message.username) {
            myUsername = message.username;
        }

        updateConnectionUI(true);

        addProof(
            `TCP SERVER CONFIRMED ✓ ${message.text}`,
            "success"
        );
    }

    else if (type === "system") {
        addMessage("system", "SERVER", message.text);
    }

    else if (type === "users") {
        usersList.innerHTML = "";

        message.users.forEach(user => {
            const li = document.createElement("li");
            li.textContent = user;
            usersList.appendChild(li);
        });
    }

    else if (type === "server_received") {
        addProof(
            `SERVER RECEIVED ✓ Message ${message.msg_id}. Expected receivers: ${message.expected_receivers}.`,
            "success"
        );
    }

    else if (type === "delivery_update") {
        addProof(
            `DELIVERY STATUS ✓ Message ${message.msg_id}: ${message.delivered}/${message.total} clients received.`,
            "success"
        );
    }

    else if (type === "chat_message") {
        addMessage("other", message.from, message.text, message.msg_id);

        addProof(
            `RECEIVED ✓ Message ${message.msg_id} from ${message.from}. Display ACK sent automatically.`,
            "success"
        );
    }

    else if (type === "ack_sent") {
        addProof(
            `ACK SENT ✓ ${message.text}`,
            "success"
        );
    }

    else if (type === "error") {
        addProof(
            `SERVER ERROR: ${message.text}`,
            "error"
        );
    }
}


messageBox.addEventListener("keydown", function(event) {
    if (event.key === "Enter") {
        sendMessage();
    }
});


window.addEventListener("beforeunload", function() {
    if (connected) {
        navigator.sendBeacon(
            "/api/disconnect",
            new Blob(
                [JSON.stringify({client_id: clientId})],
                {type: "application/json"}
            )
        );
    }
});