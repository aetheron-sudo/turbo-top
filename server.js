// Turbo Top - serveur : fichiers statiques + salles multijoueur (WebSocket) + circuits + admin
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = +process.env.PORT || 3000;
const ROOT = __dirname;
const DATA = path.join(ROOT, "data", "tracks.json");
let PW = process.env.ADMIN_PASSWORD;
if (!PW) { PW = crypto.randomBytes(6).toString("hex"); console.log("ADMIN_PASSWORD non défini -> mot de passe temporaire :", PW); }
const sha = (s) => crypto.createHash("sha256").update(String(s)).digest();
const same = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));

// ---------- circuits ----------
// Stockage PERMANENT si UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN sont définis (base Redis gratuite d'Upstash),
// sinon simple fichier data/tracks.json (effacé à chaque redémarrage sur les hébergeurs gratuits comme Render).
const UP_URL = process.env.UPSTASH_REDIS_REST_URL, UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN, KEY = "turbotop:tracks";
const useUp = !!(UP_URL && UP_TOKEN);
async function up(cmd) {
  const r = await fetch(UP_URL, { method: "POST", headers: { Authorization: "Bearer " + UP_TOKEN }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (!r.ok || j.error) throw new Error(j.error || "HTTP " + r.status);
  return j.result;
}
let tracks = [];
async function loadTracks() {
  try {
    if (useUp) { const v = await up(["GET", KEY]); if (v) tracks = JSON.parse(v); console.log("Circuits chargés depuis Upstash :", tracks.length); }
    else { tracks = JSON.parse(fs.readFileSync(DATA, "utf8")); console.log("Circuits chargés depuis data/tracks.json :", tracks.length); }
  } catch (e) { console.log("Aucun circuit chargé (" + e.message + ")"); }
  broadcastTracks();
}
async function saveTracks() {
  if (useUp) return up(["SET", KEY, JSON.stringify(tracks)]);
  fs.mkdirSync(path.dirname(DATA), { recursive: true });
  const tmp = DATA + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(tracks));
  fs.renameSync(tmp, DATA);
}
function commit(ack) {
  saveTracks().then(() => { broadcastTracks(); ack(true); })
    .catch((e) => { console.log("Sauvegarde impossible :", e.message); ack(false, { err: "sauvegarde impossible" }); });
}

// ---------- HTTP statique (index.html + sprites/*.png uniquement) ----------
const httpServer = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent((req.url || "/").split("?")[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === "/") p = "/index.html";
  if (!(p === "/index.html" || /^\/sprites\/[\w.-]+\.png$/.test(p))) { res.writeHead(404); return res.end("404"); }
  fs.readFile(path.join(ROOT, p), (err, buf) => {
    if (err) { res.writeHead(404); return res.end("404"); }
    res.writeHead(200, {
      "Content-Type": p.endsWith(".png") ? "image/png" : "text/html; charset=utf-8",
      "Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff",
    });
    res.end(buf);
  });
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server: httpServer, path: "/ws", maxPayload: 16384 });
const clients = new Map(), rooms = new Map(), tokens = new Set(), fails = new Map();
let nid = 0, dirtyLobby = false;
const MAX_PER_ROOM = 8, MAX_ROOMS = 100;
const ITEMS = new Set(["turbo", "oil", "missile", "spikes", "ghost", "bird"]);
const TARGETED = new Set(["steal", "give", "splat"]); // évènements envoyés à UN joueur de la salle

const num = (v, m = 1e5) => (typeof v === "number" && isFinite(v) && Math.abs(v) <= m ? v : undefined);
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

function cleanRoomPresence(p, cur) {
  if (!p || typeof p !== "object") return;
  for (const k of ["x", "y", "a", "vx", "vy", "prog", "sk"]) if (k in p) { const v = num(p[k]); if (v !== undefined) cur[k] = v; }
  if ("fin" in p) cur.fin = p.fin === null ? null : (num(p.fin) ?? null);
  if ("v" in p) cur.v = typeof p.v === "string" && /^[\w-]{1,32}$/.test(p.v) ? p.v : null; // vote pour la prochaine map
  if ("it" in p) cur.it = typeof p.it === "string" && ITEMS.has(p.it) ? p.it : null;
  if (typeof p.name === "string") cur.name = p.name.slice(0, 12);
  if (typeof p.color === "string" && /^#[0-9a-f]{6}$/i.test(p.color)) cur.color = p.color;
  for (const k of ["o", "m"]) if (Array.isArray(p[k])) cur[k] = p[k].slice(0, 12).map((v) => num(v)).filter((v) => v !== undefined);
}
function validTrack(d) {
  if (!d || typeof d !== "object") return;
  const arr = (a, max, step) =>
    Array.isArray(a) && a.length <= max && a.length % step === 0 && a.every((v) => num(v, 20000) !== undefined) ? a.map(Math.round) : undefined;
  const pf = arr(d.pf, 200, 2), cf = arr(d.cf || [], 200, 2), bf = arr(d.bf || [], 200, 2), of = arr(d.of || [], 200, 2), sf = arr(d.sf || [], 450, 3);
  const w = num(d.w, 400), laps = num(d.laps, 50);
  if (!pf || pf.length < 8 || !cf || !bf || !of || !sf || !w || w < 60 || !laps || laps < 1) return;
  return { name: String(d.name || "Circuit").slice(0, 24), w: Math.round(w), laps: Math.round(laps), pf, cf, bf, of, sf };
}

function leaveRoom(c) {
  const r = c.room;
  if (!r) return;
  r.m.delete(c); c.room = null; c.rp = {}; r.dirty = true; dirtyLobby = true;
  if (!r.m.size) rooms.delete(r.name);
}
// Le lobby ne montre que les salles « visibles » (le code d'une salle cachée n'est jamais envoyé aux autres)
const lobbyRooms = () => [...rooms.values()].filter((r) => r.pub && r.meta).map((r) => ({
  c: r.name.slice(2), t: r.meta.t, n: r.meta.n, k: r.m.size, lock: !!r.pw, h: (([...r.m][0] || {}).rp || {}).name || "?",
}));
const broadcastTracks = () => { for (const c of clients.values()) send(c.ws, { t: "tracks", list: tracks }); };

setInterval(() => {
  if (dirtyLobby) { dirtyLobby = false; const s = { t: "lobby", rooms: lobbyRooms() }; for (const c of clients.values()) send(c.ws, s); }
  for (const r of rooms.values()) if (r.dirty) {
    r.dirty = false;
    const s = { t: "room", peers: [...r.m].map((c) => ({ id: c.id, p: c.rp })) };
    for (const c of r.m) send(c.ws, s);
  }
}, 50);

wss.on("connection", (ws, req) => {
  const c = {
    ws, id: "p" + (++nid).toString(36) + crypto.randomBytes(2).toString("hex"),
    rp: {}, room: null, admin: false, n: 0, t: Date.now(),
    ip: req.headers["cf-connecting-ip"] || req.socket.remoteAddress,
  };
  clients.set(c.id, c);
  send(ws, { t: "welcome", id: c.id });
  send(ws, { t: "tracks", list: tracks });
  send(ws, { t: "lobby", rooms: lobbyRooms() });

  const tooMany = (key, now) => {
    const f = fails.get(key) || { n: 0, t: 0 };
    if (now - f.t > 60000) f.n = 0;
    fails.set(key, f);
    return f;
  };

  ws.on("message", (raw) => {
    const now = Date.now();
    if (now - c.t > 1000) { c.t = now; c.n = 0; }
    if (++c.n > 80) return; // anti-spam
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    const ack = (ok, x) => { if (m.r) send(ws, { t: "ack", r: m.r, ok, ...x }); };

    switch (m.t) {
      case "join": {
        const name = String(m.room || "");
        if (!/^r-[A-Z0-9]{1,8}$/.test(name)) return ack(false, { err: "nom de salle invalide" });
        leaveRoom(c);
        let r = rooms.get(name);
        if (m.create) {
          if (r) return ack(false, { err: "code déjà utilisé" });
          if (rooms.size >= MAX_ROOMS) return ack(false, { err: "serveur plein" });
          const mt = m.meta;
          if (!mt || !/^[\w-]{1,32}$/.test(String(mt.t))) return ack(false, { err: "circuit invalide" });
          r = { name, m: new Set(), dirty: false, pub: m.pub !== false, meta: { t: String(mt.t), n: String(mt.n || "").slice(0, 24) },
                pw: typeof m.pw === "string" && m.pw ? m.pw.slice(0, 16) : null };
          rooms.set(name, r);
        } else {
          if (!r) return ack(false, { err: "salle introuvable" });
          if (r.pw) {
            if (typeof m.pw !== "string" || m.pw === "") return ack(false, { err: "mot de passe requis" });
            const f = tooMany("r" + c.ip, now);
            if (f.n >= 8) return ack(false, { err: "trop d'essais, réessaie dans 1 min" });
            if (!same(m.pw, r.pw)) { f.n++; f.t = now; return ack(false, { err: "mot de passe incorrect" }); }
          }
        }
        if (r.m.size >= MAX_PER_ROOM) return ack(false, { err: "salle pleine" });
        r.m.add(c); c.room = r; c.rp = {}; r.dirty = true; dirtyLobby = true;
        ack(true, { meta: r.meta }); break;
      }
      case "rp": if (c.room) { cleanRoomPresence(m.p, c.rp); c.room.dirty = true; } break;
      case "leave": leaveRoom(c); break;
      case "emit": {
        if (!c.room) break;
        if (m.topic === "start") { // seul le chef de salle (1er arrivé) lance le départ
          if ([...c.room.m][0] !== c) break;
          const d = num(m.data && m.data.d, 60) || 4, data = { d }, tid = m.data && m.data.t;
          if (typeof tid === "string" && /^[\w-]{1,32}$/.test(tid)) { // le chef peut changer de circuit (map votée)
            data.t = tid; data.n = String(m.data.n || "").slice(0, 24);
            c.room.meta = { t: tid, n: data.n }; dirtyLobby = true;
          }
          for (const o of c.room.m) send(o.ws, { t: "ev", topic: "start", data, from: c.id });
        } else if (TARGETED.has(m.topic)) {
          const to = m.to || (m.data && m.data.to); // l'id du joueur visé peut être dans data.to
          const o = [...c.room.m].find((x) => x.id === to && x !== c);
          if (!o) break;
          const data = m.topic === "give" ? { item: typeof m.data?.item === "string" && ITEMS.has(m.data.item) ? m.data.item : null } : {};
          send(o.ws, { t: "ev", topic: m.topic, data, from: c.id });
        }
        break;
      }
      case "auth": {
        const f = tooMany(c.ip, now);
        if (f.n >= 5) return ack(false, { err: "trop d'essais, réessaie dans 1 min" });
        let ok = false, tok;
        if (typeof m.tok === "string" && tokens.has(m.tok)) ok = true;
        else if (typeof m.pw === "string") {
          ok = same(m.pw, PW);
          if (ok) { tok = crypto.randomBytes(24).toString("hex"); tokens.add(tok); }
          else { f.n++; f.t = now; }
        }
        c.admin = ok;
        ack(ok, { tok }); break;
      }
      case "track_set": {
        if (!c.admin) return ack(false, { err: "admin requis" });
        const d = validTrack(m.doc);
        if (!d) return ack(false, { err: "circuit invalide" });
        const id = /^[a-z0-9]{1,24}$/i.test(m.id || "") ? m.id : "t" + Date.now().toString(36);
        const i = tracks.findIndex((t) => t.id === id);
        if (i < 0 && tracks.length >= 100) return ack(false, { err: "trop de circuits" });
        if (i >= 0) tracks[i] = { id, ...d }; else tracks.push({ id, ...d });
        commit(ack); break;
      }
      case "track_del": {
        if (!c.admin) return ack(false, { err: "admin requis" });
        tracks = tracks.filter((t) => t.id !== m.id);
        commit(ack); break;
      }
    }
  });
  ws.on("close", () => { leaveRoom(c); clients.delete(c.id); });
  ws.on("error", () => {});
});

httpServer.listen(PORT, () => console.log("Turbo Top sur http://localhost:" + PORT));
loadTracks();
