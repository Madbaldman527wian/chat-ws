const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const CYCLE_MS = 5 * 60 * 1000;   // intervalo do ciclo leave/rejoin (0 = desligado)
const HEARTBEAT_MS = 15 * 1000;

const wss = new WebSocketServer({ port: PORT });
const bots = new Map(); // userId -> { ws, placeId, jobId, lastSeen, cycleTimer }

const send = (ws, obj) => ws.readyState === 1 && ws.send(JSON.stringify(obj));

function scheduleCycle(bot) {
  clearInterval(bot.cycleTimer);
  if (!CYCLE_MS) return;
  bot.cycleTimer = setInterval(() => {
    console.log(`[cycle] ${bot.userId} -> rejoin ${bot.jobId}`);
    send(bot.ws, { type: "rejoin", placeId: bot.placeId, jobId: bot.jobId });
  }, CYCLE_MS);
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  let bot = null;

  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === "hello") {
      const prev = bots.get(msg.userId);
      bot = {
        userId: msg.userId,
        ws,
        placeId: msg.placeId,
        jobId: msg.jobId,
        target: prev?.target || { placeId: msg.placeId, jobId: msg.jobId },
        lastSeen: Date.now(),
        cycleTimer: null,
      };
      if (prev) clearInterval(prev.cycleTimer);
      bots.set(msg.userId, bot);

      // Entrou em outro servidor? Manda voltar para o alvo guardado.
      if (prev && bot.target.jobId !== msg.jobId) {
        console.log(`[return] ${msg.userId} -> ${bot.target.jobId}`);
        send(ws, { type: "rejoin", ...bot.target });
      }
      scheduleCycle(bot);
      send(ws, { type: "ready" });
    }

    if (msg.type === "setTarget" && bot) {
      bot.target = { placeId: msg.placeId, jobId: msg.jobId };
    }
    if (msg.type === "ping" && bot) bot.lastSeen = Date.now();
  });

  ws.on("close", () => {
    if (bot) clearInterval(bot.cycleTimer); // mantém o registro p/ lembrar o alvo
  });
});

// heartbeat: derruba conexões mortas
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_MS);

// Controle pelo terminal: "cycle <userId>" | "list"
process.stdin.on("data", (d) => {
  const [cmd, id] = d.toString().trim().split(" ");
  if (cmd === "list") console.log([...bots.keys()]);
  if (cmd === "cycle" && bots.get(id)) {
    const b = bots.get(id);
    send(b.ws, { type: "rejoin", ...b.target });
  }
});

console.log(`WebSocket bot server na porta ${PORT}`);
