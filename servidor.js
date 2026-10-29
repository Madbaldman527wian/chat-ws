// Servidor de chat: repassa cada mensagem para todos os conectados.
// Requer Node.js e o pacote "ws"  ->  npm install ws
const { WebSocketServer } = require("ws");

const wss = new WebSocketServer({ port: process.env.PORT || 8080 });

wss.on("connection", (ws) => {
  ws.on("message", (data) => {
    const msg = data.toString();
    if (msg.length > 500) return; // limite de tamanho
    for (const client of wss.clients) {
      if (client.readyState === 1) client.send(msg);
    }
  });
});

console.log("Servidor de chat rodando!");
