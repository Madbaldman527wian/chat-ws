const http = require("http");
const { WebSocketServer } = require("ws");

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("OK");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.on("message", (data) => {
    const msg = data.toString();
    if (msg.length > 500) return;
    for (const client of wss.clients) {
      if (client.readyState === 1) client.send(msg);
    }
  });
});

server.listen(process.env.PORT || 8080);
