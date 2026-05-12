const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const wss = new WebSocket.Server({ port: PORT });

const peers = new Map();

function broadcastPeers() {

    const peerList = [...peers.keys()];

    const packet = JSON.stringify({
        type: "peers",
        peers: peerList
    });

    for (const ws of peers.values()) {

        if (ws.readyState === WebSocket.OPEN) {
            ws.send(packet);
        }

    }

}

wss.on("connection", (ws) => {

    let peerId = null;

    console.log("[WS] connected");

    ws.on("message", (message) => {

        try {

            const data = JSON.parse(message);

            if (data.type === "join") {

                peerId = data.peerId;

                peers.set(peerId, ws);

                console.log("[Peer] joined:", peerId);

                broadcastPeers();

            }

        } catch (err) {

            console.error("[WS ERROR]", err);

        }

    });

    ws.on("close", () => {

        if (peerId) {

            peers.delete(peerId);

            console.log("[Peer] left:", peerId);

            broadcastPeers();

        }

    });

});

console.log("[Whisper Signaling] running");