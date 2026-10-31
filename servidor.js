'use strict';

const { WebSocketServer } = require('ws');
const { EventEmitter } = require('events');

const SERVER_SIGNALS = { RECEIVE_CUSTOM_SIGNAL_FROM_CLIENT: 'CUSTOM_CLIENT_SIGNAL_RECEIVED' };
const SERVER_REQUESTS = {
  GET_SUBSCRIPTION_CACHE: 'GET_SUBSCRIPTION_CACHE',
  RECEIVE_CUSTOM_REQUEST_FROM_CLIENT: 'CUSTOM_CLIENT_REQUEST_RECEIVED',
};
const CLIENT_SIGNALS = {
  CREATE_SUBSCRIPTION: 'CREATE_SUBSCRIPTION',
  UPDATE_SUBSCRIPTION: 'UPDATE_SUBSCRIPTION',
  UPDATE_SUBSCRIPTION_PACKAGE: 'UPDATE_SUBSCRIPTION_PACKAGE',
  REMOVE_SUBSCRIPTION: 'REMOVE_SUBSCRIPTION',
  RECEIVE_CUSTOM_SIGNAL_FROM_SERVER: 'CUSTOM_SERVER_SIGNAL_RECEIVED',
};
const CLIENT_REQUESTS = { RECEIVE_CUSTOM_REQUEST_FROM_SERVER: 'CUSTOM_SERVER_REQUEST_RECEIVED' };
const CLIENT_TABLE_FILTER_TYPES = Object.freeze({ WHITELIST: 'WHITELIST', BLACKLIST: 'BLACKLIST' });

const REQUEST_TIMEOUT_MS = 30000;
const HELLO_TIMEOUT_MS = 5000;
const HEARTBEAT_MS = 30000;

function enc(v) {
  if (v === undefined || v === null) return { $: 'nil' };
  switch (typeof v) {
    case 'boolean':
    case 'string':
      return v;
    case 'number':
      if (Number.isNaN(v)) return { $: 'n', v: 'nan' };
      if (v === Infinity) return { $: 'n', v: 'inf' };
      if (v === -Infinity) return { $: 'n', v: '-inf' };
      return v;
    case 'bigint':
      return Number(v);
    case 'object': {
      if (typeof v.$ === 'string') return v;
      const e = [];
      if (Array.isArray(v)) {
        v.forEach((x, i) => {
          if (x !== undefined && x !== null) e.push([i + 1, enc(x)]);
        });
      } else if (v instanceof Map) {
        for (const [k, x] of v) {
          if (x !== undefined && x !== null) e.push([enc(k), enc(x)]);
        }
      } else {
        for (const [k, x] of Object.entries(v)) {
          if (x !== undefined && x !== null) e.push([k, enc(x)]);
        }
      }
      return { $: 't', e };
    }
  }
  throw new TypeError('unsupported value type: ' + typeof v);
}

function dec(v) {
  if (v === null || typeof v !== 'object') return v;
  switch (v.$) {
    case 'nil':
      return undefined;
    case 'n':
      return v.v === 'inf' ? Infinity : v.v === '-inf' ? -Infinity : NaN;
    case 't':
      return decTable(v.e || []);
    default:
      return v;
  }
}

function decTable(e) {
  const entries = [];
  for (const [k, x] of e) {
    const key = dec(k);
    if (key !== undefined) entries.push([key, dec(x)]);
  }
  if (entries.length === 0) return {};
  const n = entries.length;
  const arr = new Array(n);
  let isArray = true;
  const seen = new Set();
  for (const [k, x] of entries) {
    if (!Number.isInteger(k) || k < 1 || k > n || seen.has(k)) {
      isArray = false;
      break;
    }
    seen.add(k);
    arr[k - 1] = x;
  }
  if (isArray) return arr;
  if (entries.every(([k]) => typeof k === 'string')) return Object.fromEntries(entries);
  return new Map(entries);
}

function packArgs(args) {
  return { a: args.map(enc), n: args.length };
}

function unpackArgs(m) {
  const n = Number.isInteger(m.n) && m.n >= 0 ? m.n : 0;
  const a = Array.isArray(m.a) ? m.a : [];
  const out = [];
  for (let i = 0; i < n; i++) out.push(dec(a[i]));
  return out;
}

class Signal {
  constructor() {
    this.emitter = new EventEmitter();
    this.emitter.setMaxListeners(0);
  }
  connect(fn) {
    this.emitter.on('e', fn);
    return { disconnect: () => this.emitter.off('e', fn) };
  }
  fire(...args) {
    for (const fn of this.emitter.listeners('e')) {
      try {
        const r = fn(...args);
        if (r && typeof r.catch === 'function') r.catch((err) => console.error(err));
      } catch (err) {
        console.error(err);
      }
    }
  }
}

class Player {
  constructor(ws, id, info) {
    this.ws = ws;
    this.id = id;
    this.userId = info.userId;
    this.name = info.name;
    this.displayName = info.displayName;
    this.pending = new Map();
    this.nextId = 0;
  }
  send(obj) {
    if (this.ws.readyState !== 1) return false;
    try {
      this.ws.send(JSON.stringify(obj));
      return true;
    } catch (err) {
      return false;
    }
  }
}

class Subscription {
  constructor(replicator, name, props) {
    this.replicator = replicator;
    this.name = name;
    this.properties = props || {};
    this.storeTablePropertyAsPointer = false;
    this.updateAllSubsOnPropChanged = true;
    this.clientTableFilterType = CLIENT_TABLE_FILTER_TYPES.WHITELIST;
    this.clientTable = [];
    this.listeners = new Map();
    this.propertyAdded = new Signal();
    this.propertyChanged = new Signal();
    this.propertyRemoved = new Signal();
    this.playerAddedToClientTbl = new Signal();
    this.playerRemovedFromClientTbl = new Signal();

    this.propertyChanged.connect((propIndex, propVal) => {
      const msg = [CLIENT_SIGNALS.UPDATE_SUBSCRIPTION, this.name, propIndex, propVal];
      if (!this.updateAllSubsOnPropChanged) {
        this.iterateThroughFilteredCTbl((player) => replicator._fire(player, msg));
      } else {
        for (const player of replicator.players.values()) replicator._fire(player, msg);
      }
    });

    this.playerAddedToClientTbl.connect((player) => {
      let send = false;
      if (!this.updateAllSubsOnPropChanged) {
        send = this.getFilteredClientTbl().includes(player);
      } else {
        send = true;
      }
      if (send) {
        replicator._fire(player, [CLIENT_SIGNALS.UPDATE_SUBSCRIPTION_PACKAGE, this.name, this.properties]);
      }
    });
  }

  _prepare(v) {
    if (v !== null && typeof v === 'object' && !this.storeTablePropertyAsPointer) return structuredClone(v);
    return v;
  }

  addProperty(propIndex, propVal) {
    if (Object.hasOwn(this.properties, propIndex)) {
      throw new Error(`[INVALID PROPERTY] A property with the name '${propIndex}' already exists in this subscription!`);
    }
    propVal = this._prepare(propVal);
    this.properties[propIndex] = propVal;
    const l = this.listeners.get(propIndex);
    if (l) l.fire(propVal);
    this.propertyAdded.fire(propIndex, propVal);
    this.propertyChanged.fire(propIndex, propVal);
  }

  setProperty(propIndex, propVal) {
    if (!Object.hasOwn(this.properties, propIndex)) {
      throw new Error(`[INVALID PROPERTY] '${propIndex}' is not a valid property of this subscription!`);
    }
    propVal = this._prepare(propVal);
    if (propVal === undefined || propVal === null) delete this.properties[propIndex];
    else this.properties[propIndex] = propVal;
    const l = this.listeners.get(propIndex);
    if (l) l.fire(propVal);
    this.propertyChanged.fire(propIndex, propVal);
  }

  removeProperty(propIndex) {
    if (!Object.hasOwn(this.properties, propIndex)) {
      throw new Error(`[INVALID PROPERTY] '${propIndex}' is not a valid property of this subscription!`);
    }
    delete this.properties[propIndex];
    const l = this.listeners.get(propIndex);
    if (l) l.fire(undefined);
    this.propertyChanged.fire(propIndex, undefined);
    this.propertyRemoved.fire(propIndex);
  }

  getProperty(propIndex) {
    if (!Object.hasOwn(this.properties, propIndex)) {
      throw new Error(`[INVALID PROPERTY] '${propIndex}' is not a valid property in the subscription!`);
    }
    return this.properties[propIndex];
  }

  getPropertyChangedSignal(propIndex) {
    if (!Object.hasOwn(this.properties, propIndex)) {
      throw new Error(`[INVALID PROPERTY] '${propIndex}' is not a valid property in the subscription!`);
    }
    let l = this.listeners.get(propIndex);
    if (!l) {
      l = new Signal();
      this.listeners.set(propIndex, l);
    }
    return l;
  }

  addPlayerToClientTbl(player) {
    if (this.clientTable.includes(player)) return;
    this.clientTable.push(player);
    this.playerAddedToClientTbl.fire(player);
  }

  removePlayerFromClientTbl(player) {
    const i = this.clientTable.indexOf(player);
    if (i === -1) return;
    this.clientTable.splice(i, 1);
    this.playerRemovedFromClientTbl.fire(player);
  }

  getClientTbl() {
    return [...this.clientTable];
  }

  getFilteredClientTbl() {
    if (this.clientTableFilterType === CLIENT_TABLE_FILTER_TYPES.WHITELIST) return [...this.clientTable];
    if (this.clientTableFilterType === CLIENT_TABLE_FILTER_TYPES.BLACKLIST) {
      return [...this.replicator.players.values()].filter((p) => !this.clientTable.includes(p));
    }
    return undefined;
  }

  iterateThroughFilteredCTbl(fn) {
    const list = this.getFilteredClientTbl();
    if (list) for (const p of list) fn(p);
  }
}

class EZReplicator {
  constructor(options = {}) {
    this.CLIENT_TABLE_FILTER_TYPES = CLIENT_TABLE_FILTER_TYPES;
    this.token = options.token || '';
    this.players = new Map();
    this.subscriptions = new Map();
    this.signalListeners = new Map();
    this.requestHandlers = new Map();
    this.subscriptionAdded = new Signal();
    this.subscriptionRemoved = new Signal();
    this.playerAdded = new Signal();
    this.playerRemoving = new Signal();
    this._nextPlayerId = 0;

    this.wss = new WebSocketServer({
      port: options.port,
      host: options.host,
      server: options.server,
      maxPayload: options.maxPayload || 1024 * 1024,
    });
    this.wss.on('connection', (ws) => this._onConnection(ws));
    this.wss.on('listening', () => {
      const a = this.wss.address();
      if (a && typeof a === 'object') console.log(`EZReplicator websocket listening on ${a.address}:${a.port}`);
    });
    this._heartbeat = setInterval(() => {
      for (const ws of this.wss.clients) {
        if (ws.isAlive === false) {
          ws.terminate();
          continue;
        }
        ws.isAlive = false;
        try {
          ws.ping();
        } catch (err) {}
      }
    }, HEARTBEAT_MS);
    this._heartbeat.unref();
  }

  close() {
    clearInterval(this._heartbeat);
    this.wss.close();
    for (const ws of this.wss.clients) ws.terminate();
  }

  _fire(player, argsList) {
    const { a, n } = packArgs(argsList);
    return player.send({ k: 'S', a, n });
  }

  _onConnection(ws) {
    let player = null;
    ws.isAlive = true;
    ws.on('pong', () => {
      ws.isAlive = true;
    });
    const helloTimer = setTimeout(() => {
      if (!player) ws.close(4001, 'hello timeout');
    }, HELLO_TIMEOUT_MS);

    ws.on('message', (raw) => {
      let m;
      try {
        m = JSON.parse(raw.toString());
      } catch (err) {
        return;
      }
      if (!m || typeof m !== 'object') return;
      if (!player) {
        if (m.k !== 'hello') return;
        if (this.token !== '' && m.token !== this.token) {
          ws.close(4003, 'bad token');
          return;
        }
        clearTimeout(helloTimer);
        player = new Player(ws, ++this._nextPlayerId, {
          userId: m.userId,
          name: String(m.name),
          displayName: String(m.displayName),
        });
        this.players.set(player.id, player);
        this.playerAdded.fire(player);
        return;
      }
      try {
        this._onMessage(player, m);
      } catch (err) {
        console.error(err);
      }
    });

    ws.on('close', () => {
      clearTimeout(helloTimer);
      if (!player) return;
      this.players.delete(player.id);
      for (const [, p] of player.pending) {
        clearTimeout(p.timer);
        p.resolve([false, 'connection closed']);
      }
      player.pending.clear();
      this.playerRemoving.fire(player);
      for (const sub of this.subscriptions.values()) sub.removePlayerFromClientTbl(player);
    });

    ws.on('error', () => {});
  }

  _onMessage(player, m) {
    if (m.k === 'S') {
      const args = unpackArgs(m);
      if (args[0] === SERVER_SIGNALS.RECEIVE_CUSTOM_SIGNAL_FROM_CLIENT) {
        const listener = this.signalListeners.get(args[1]);
        if (listener) listener.fire(player, ...args.slice(2));
      }
    } else if (m.k === 'Q') {
      this._onRequest(player, m);
    } else if (m.k === 'R') {
      const p = player.pending.get(m.id);
      if (p) {
        player.pending.delete(m.id);
        clearTimeout(p.timer);
        if (m.ok) {
          const r = unpackArgs(m);
          p.resolve([true, r[0]]);
        } else {
          p.resolve([false, String(m.e)]);
        }
      }
    }
  }

  async _onRequest(player, m) {
    const args = unpackArgs(m);
    const reply = (o) => player.send({ k: 'R', id: m.id, ...o });
    try {
      let result;
      if (args[0] === SERVER_REQUESTS.GET_SUBSCRIPTION_CACHE) {
        result = {};
        for (const [name, sub] of this.subscriptions) result[name] = { Properties: sub.properties };
      } else if (args[0] === SERVER_REQUESTS.RECEIVE_CUSTOM_REQUEST_FROM_CLIENT) {
        const handler = this.requestHandlers.get(args[1]);
        if (handler) {
          result = await handler(player, ...args.slice(2));
        } else {
          console.warn(`[SERVER REQUEST HANDLER UNDEFINED] Did not find server handler for dataKey '${args[1]}'`);
        }
      } else {
        reply({ ok: true, a: [], n: 0 });
        return;
      }
      if (result === undefined) reply({ ok: true, a: [], n: 0 });
      else reply({ ok: true, ...packArgs([result]) });
    } catch (err) {
      reply({ ok: false, e: String((err && err.message) || err) });
    }
  }

  createSubscription(subscriptionName, propTable) {
    if (this.subscriptions.has(subscriptionName)) {
      throw new Error(`[INVALID SUBSCRIPTION NAME] Subscription '${subscriptionName}' has already been created!`);
    }
    const sub = new Subscription(this, subscriptionName, propTable);
    this.subscriptions.set(subscriptionName, sub);
    for (const player of this.players.values()) {
      this._fire(player, [CLIENT_SIGNALS.CREATE_SUBSCRIPTION, subscriptionName, propTable]);
    }
    this.subscriptionAdded.fire(sub);
    return sub;
  }

  removeSubscription(subscriptionName) {
    const sub = this.subscriptions.get(subscriptionName);
    if (!sub) {
      throw new Error(`[INVALID SUBSCRIPTION] '${subscriptionName}' is not a valid subscription`);
    }
    this.subscriptions.delete(subscriptionName);
    for (const player of this.players.values()) {
      this._fire(player, [CLIENT_SIGNALS.REMOVE_SUBSCRIPTION, subscriptionName]);
    }
    this.subscriptionRemoved.fire(sub);
  }

  getSubscription(subscriptionName) {
    return this.subscriptions.get(subscriptionName);
  }

  sendSignalToClient(player, signalName, ...args) {
    return this._fire(player, [CLIENT_SIGNALS.RECEIVE_CUSTOM_SIGNAL_FROM_SERVER, signalName, ...args]);
  }

  sendSignalToAllClientsExcept(plrs, signalName, ...args) {
    const list = Array.isArray(plrs) ? plrs : [plrs];
    const result = new Map();
    for (const player of this.players.values()) {
      if (!list.includes(player)) {
        result.set(player, this._fire(player, [CLIENT_SIGNALS.RECEIVE_CUSTOM_SIGNAL_FROM_SERVER, signalName, ...args]));
      }
    }
    return result;
  }

  sendSignalToAllClients(signalName, ...args) {
    for (const player of this.players.values()) {
      this._fire(player, [CLIENT_SIGNALS.RECEIVE_CUSTOM_SIGNAL_FROM_SERVER, signalName, ...args]);
    }
  }

  getClientSignal(signalName) {
    let l = this.signalListeners.get(signalName);
    if (!l) {
      l = new Signal();
      this.signalListeners.set(signalName, l);
    }
    return l;
  }

  requestDataFromClient(player, dataKey, ...args) {
    return new Promise((resolve) => {
      const id = ++player.nextId;
      const timer = setTimeout(() => {
        player.pending.delete(id);
        resolve([false, 'request timed out']);
      }, REQUEST_TIMEOUT_MS);
      player.pending.set(id, { resolve, timer });
      const { a, n } = packArgs([CLIENT_REQUESTS.RECEIVE_CUSTOM_REQUEST_FROM_SERVER, dataKey, ...args]);
      if (!player.send({ k: 'Q', id, a, n })) {
        player.pending.delete(id);
        clearTimeout(timer);
        resolve([false, 'connection closed']);
      }
    });
  }

  setServerRequestHandler(dataKey, func) {
    this.requestHandlers.set(dataKey, func);
  }
}

module.exports = { EZReplicator, Subscription, Player, CLIENT_TABLE_FILTER_TYPES };

if (require.main === module) {
  const port = Number(process.env.PORT) || 8080;
  const replicator = new EZReplicator({ port, token: process.env.TOKEN || '' });

  const demo = replicator.createSubscription('Demo', { Counter: 0 });
  setInterval(() => demo.setProperty('Counter', demo.getProperty('Counter') + 1), 1000);

  replicator.playerAdded.connect((player) => console.log(`+ ${player.name} (${player.userId})`));
  replicator.playerRemoving.connect((player) => console.log(`- ${player.name}`));

  replicator.getClientSignal('Ping').connect((player, ...args) => {
    replicator.sendSignalToClient(player, 'Pong', ...args);
  });
  replicator.setServerRequestHandler('Echo', (player, ...args) => args);
}
