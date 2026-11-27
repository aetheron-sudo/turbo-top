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
if (PW.length < 12) console.warn("⚠ ADMIN_PASSWORD trop court : prends 12+ caractères aléatoires.");

// ---------- sécurité : IP du client, limites de connexions, durée des jetons ----------
// TRUST_PROXY_HOPS = nombre de proxys de confiance devant le serveur (Render : 1). 0 = pas de proxy (test en local).
// On lit X-Forwarded-For en partant de la DROITE : ce qu'un client écrit à gauche est ignoré, donc impossible à falsifier.
const HOPS = Math.max(0, +(process.env.TRUST_PROXY_HOPS ?? 1) || 0);
const MAX_CONN_PER_IP = +process.env.MAX_CONN_PER_IP || 10;      // connexions WebSocket simultanées par IP
const MAX_CLIENTS = +process.env.MAX_CLIENTS || 500;              // connexions totales
const TOKEN_TTL = (+process.env.TOKEN_TTL_HOURS || 12) * 3600e3;  // durée de vie d'une session admin
const MAX_TOKENS = 20;
const GLOBAL_PW_FAILS = 20, GLOBAL_WINDOW = 10 * 60e3;            // filet de sécurité : max d'échecs de mot de passe admin, toutes IP confondues
const gFail = { n: 0, t: 0 };
function clientIp(req) {
  const sock = req.socket.remoteAddress || "?";
  if (!HOPS) return sock;
  const xff = String(req.headers["x-forwarded-for"] || "").split(",").map((x) => x.trim()).filter(Boolean);
  return String(xff[xff.length - HOPS] || sock).slice(0, 64);
}


// ---------- publicité (Adsterra) : config publique lue dans les variables d'environnement ----------
// ADSTERRA_SRC  = l'URL du fichier invoke.js de TON code Adsterra (ex. //xxxx.com/0123...cdef/invoke.js). Vide = aucune pub, aucun bandeau.
// ADSTERRA_SIZE = taille du bloc créé dans Adsterra : 320x50 (défaut), 468x60, 728x90 ou 300x250.
// ADS_TXT       = contenu du fichier /ads.txt si Adsterra t'en donne un (\n pour les retours à la ligne).
// LEGAL_NAME / CONTACT_EMAIL = éditeur et contact affichés dans les mentions légales.
const AD_RE = /^(?:https?:)?\/\/([a-z0-9-]+(?:\.[a-z0-9-]+)+)\/([a-f0-9]{32})\/invoke\.js$/i;
const AD_SIZES = ["320x50", "468x60", "728x90", "300x250"];
function adsConfig() {
  const raw = (process.env.ADSTERRA_SRC || "").trim();
  if (!raw) return null;
  const m = AD_RE.exec(raw), size = (process.env.ADSTERRA_SIZE || "320x50").trim();
  if (!m || !AD_SIZES.includes(size)) { console.warn("⚠ ADSTERRA_SRC / ADSTERRA_SIZE invalide : publicité désactivée."); return null; }
  const [w, h] = size.split("x").map(Number);
  return { src: "https://" + m[1] + "/" + m[2] + "/invoke.js", w, h };
}
const ADS = adsConfig();
const SITE = { editor: (process.env.LEGAL_NAME || "").slice(0, 80), email: (process.env.CONTACT_EMAIL || "").slice(0, 80) };
const ADS_TXT = (process.env.ADS_TXT || "").replace(/\\n/g, "\n");
console.log(ADS ? "Publicité Adsterra activée (" + ADS.w + "x" + ADS.h + ")" : "Publicité désactivée (ADSTERRA_SRC non défini)");

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
  if (p === "/site-config.json") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff" });
    return res.end(JSON.stringify({ ads: ADS, ...SITE }));
  }
  if (p === "/ads.txt") {
    if (!ADS_TXT) { res.writeHead(404); return res.end("404"); }
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-cache" });
    return res.end(ADS_TXT + "\n");
  }
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
const wss = new WebSocketServer({ server: httpServer, path: "/ws", maxPayload: 65536 });
const clients = new Map(), rooms = new Map(), tokens = new Map(), fails = new Map(), ipConns = new Map();
let nid = 0, dirtyLobby = false;
const MAX_PER_ROOM = 8, MAX_ROOMS = 100;
const ITEMS = new Set(["turbo", "oil", "missile", "spikes", "ghost", "bird"]);
const isAdmin = (c) => c.admin && Date.now() < c.until;
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
  const pf = arr(d.pf, 200, 2), cf = arr(d.cf || [], 200, 2), bf = arr(d.bf || [], 200, 2), of = arr(d.of || [], 1200, 2), sf = arr(d.sf || [], 450, 3);
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

// nettoyage : jetons expirés, compteurs d'essais anciens
setInterval(() => {
  const now = Date.now();
  for (const [k, exp] of tokens) if (exp <= now) tokens.delete(k);
  for (const [k, f] of fails) if (now - f.t > 120000) fails.delete(k);
}, 60000);
// heartbeat : coupe les connexions mortes (sinon elles restent comptées dans la limite par IP)
setInterval(() => {
  for (const c of clients.values()) {
    if (c.ws.isAlive === false) { c.ws.terminate(); continue; }
    c.ws.isAlive = false; try { c.ws.ping(); } catch (e) {}
  }
}, 30000);

wss.on("connection", (ws, req) => {
  ws.on("error", () => {});
  const ip = clientIp(req);
  if (process.env.DEBUG_IP) console.log("DEBUG_IP", { xff: req.headers["x-forwarded-for"], cf: req.headers["cf-connecting-ip"], socket: req.socket.remoteAddress, utilisee: ip });
  const nIp = ipConns.get(ip) || 0;
  if (nIp >= MAX_CONN_PER_IP || clients.size >= MAX_CLIENTS) return ws.close(1013, "trop de connexions");
  ipConns.set(ip, nIp + 1);
  ws.isAlive = true; ws.on("pong", () => { ws.isAlive = true; });

  const c = {
    ws, id: "p" + (++nid).toString(36) + crypto.randomBytes(2).toString("hex"),
    rp: {}, room: null, admin: false, until: 0, n: 0, t: Date.now(), ip,
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
        let ok = false, tok, until = 0;
        if (typeof m.tok === "string" && (tokens.get(m.tok) || 0) > now) { ok = true; until = tokens.get(m.tok); } // session existante : jamais bloquée
        else if (typeof m.pw === "string") {
          const f = tooMany(c.ip, now);
          if (now - gFail.t > GLOBAL_WINDOW) gFail.n = 0;
          if (f.n >= 5 || gFail.n >= GLOBAL_PW_FAILS) return ack(false, { err: "trop d'essais, réessaie plus tard" });
          ok = same(m.pw, PW);
          if (ok) {
            if (tokens.size >= MAX_TOKENS) tokens.delete(tokens.keys().next().value); // on retire la plus ancienne
            tok = crypto.randomBytes(24).toString("hex"); until = now + TOKEN_TTL; tokens.set(tok, until);
          } else { f.n++; f.t = now; gFail.n++; gFail.t = now; }
        }
        c.admin = ok; c.until = until;
        ack(ok, { tok }); break;
      }
      case "track_set": {
        if (!isAdmin(c)) return ack(false, { err: "admin requis" });
        const d = validTrack(m.doc);
        if (!d) return ack(false, { err: "circuit invalide" });
        const id = /^[a-z0-9]{1,24}$/i.test(m.id || "") ? m.id : "t" + Date.now().toString(36);
        const i = tracks.findIndex((t) => t.id === id);
        if (i < 0 && tracks.length >= 100) return ack(false, { err: "trop de circuits" });
        if (i >= 0) tracks[i] = { id, ...d }; else tracks.push({ id, ...d });
        commit(ack); break;
      }
      case "track_del": {
        if (!isAdmin(c)) return ack(false, { err: "admin requis" });
        tracks = tracks.filter((t) => t.id !== m.id);
        commit(ack); break;
      }
    }
  });
  ws.on("close", () => {
    leaveRoom(c); clients.delete(c.id);
    const k = (ipConns.get(ip) || 1) - 1; if (k > 0) ipConns.set(ip, k); else ipConns.delete(ip);
  });
});

httpServer.listen(PORT, () => console.log("Turbo Top sur http://localhost:" + PORT));
loadTracks();
