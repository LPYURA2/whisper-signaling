const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const wss = new WebSocket.Server({
    port: PORT
});

const peers = new Map();

function broadcastPeers() {

    const peerList = [
        ...peers.keys()
    ];

    const packet = JSON.stringify({

        type: "peers",

        peers: peerList

    });

    for (const ws of peers.values()) {

        if (
            ws.readyState ===
            WebSocket.OPEN
        ) {

            ws.send(packet);

        }

    }

}

wss.on("connection", (ws) => {

    let peerId = null;

    console.log(
        "[WS] connected"
    );

    ws.on("message", (message) => {

        try {

            const data =
                JSON.parse(message);

            if (data.type !== "join") {
                // Replaced/unauthenticated sockets must not inject stale signals.
                if (!peerId || peers.get(peerId) !== ws) {
                    return;
                }

                // The sender identity is established by `join`, not by packet input.
                if (data.from !== undefined && data.from !== peerId) {
                    console.warn("[WS] rejected sender ID mismatch:", peerId);
                    return;
                }
            }

            if (data.type === "join") {

                if (
                    typeof data.peerId !== "string" ||
                    data.peerId.trim().length === 0 ||
                    data.peerId.length > 128
                ) {
                    ws.send(JSON.stringify({
                        type: "signaling-error",
                        code: "invalid-peer-id"
                    }));
                    return;
                }

                const requestedPeerId = data.peerId.trim();

                // If this socket changes identity, remove only its own old mapping.
                if (
                    peerId &&
                    peerId !== requestedPeerId &&
                    peers.get(peerId) === ws
                ) {
                    peers.delete(peerId);
                }

                peerId = requestedPeerId;
                peers.set(peerId, ws);

                console.log("[Peer] joined:", peerId);

                // The client must not start WebRTC until this acknowledgement arrives.
                ws.send(JSON.stringify({
                    type: "signaling-ready",
                    peerId
                }));

                broadcastPeers();

            }

            if (data.type === "message") {

                const target =
                    peers.get(data.to);

                if (!target) {
                    return;
                }

                if (
                    target.readyState ===
                    WebSocket.OPEN
                ) {

                    target.send(
                        JSON.stringify({

                            type: "message",

                            from: data.from,

                            text: data.text

                        })
                    );

                }

            }

            if (data.type === "offer") {

                const target =
                    peers.get(data.target);

                if (!target) {
                    return;
                }

                if (
                    target.readyState ===
                    WebSocket.OPEN
                ) {

                    target.send(
                        JSON.stringify({

                            type: "offer",

                            from: data.from,

                            offer: data.offer

                        })
                    );

                }

            }

            if (data.type === "answer") {

                const target =
                    peers.get(data.target);

                if (!target) {
                    return;
                }

                if (
                    target.readyState ===
                    WebSocket.OPEN
                ) {

                    target.send(
                        JSON.stringify({

                            type: "answer",

                            from: data.from,

                            answer: data.answer

                        })
                    );

                }

            }

            if (
                data.type ===
                "ice-candidate"
            ) {

                const target =
                    peers.get(data.target);

                if (!target) {
                    return;
                }

                if (
                    target.readyState ===
                    WebSocket.OPEN
                ) {

                    target.send(
                        JSON.stringify({

                            type:
                                "ice-candidate",

                            from:
                                data.from,

                            candidate:
                                data.candidate

                        })
                    );

                }

            }

        } catch (err) {

            console.error(
                "[WS ERROR]",
                err
            );

        }

    });

    ws.on("close", () => {

        // An older socket may close after a newer socket has replaced it.
        // Only the currently registered socket is allowed to remove this peer.
        if (peerId && peers.get(peerId) === ws) {

            peers.delete(peerId);

            console.log(
                "[Peer] left:",
                peerId
            );

            broadcastPeers();

        }

    });

});

console.log(
    "[Whisper Signaling] running"
);
