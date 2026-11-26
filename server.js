import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers,
  makeCacheableSignalKeyStore,
  proto,
  jidDecode,
  jidEncode,
  jidNormalizedUser,
  areJidsSameUser,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import express from "express";
import cors from "cors";
import qrcodeTerminal from "qrcode-terminal";
import QRCode from "qrcode";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load .env file locally if present
const envPath = path.join(__dirname, ".env");
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, "utf8");
  for (const line of envContent.split("\n")) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith("#")) {
      const idx = trimmed.indexOf("=");
      if (idx !== -1) {
        const k = trimmed.slice(0, idx).trim();
        const v = trimmed.slice(idx + 1).trim();
        if (!process.env[k]) process.env[k] = v;
      }
    }
  }
}

const PORT = process.env.PORT || 3001;
const AUTH_DIR = path.join(__dirname, "auth_info");
const SESSION_ROW_ID = "baileys_session";
const MSG_CACHE_ROW_ID = "baileys_msg_cache";
const REPAIR_MARKER_FILE = ".session_repair_v2";
const MAX_MEMORY_MESSAGES = 500;
const MAX_PERSISTED_MESSAGES = 200;

const SUPABASE_URL = process.env.SUPABASE_URL || "https://gspxzpzgdgmnydkjshjs.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("WARNING: SUPABASE_SERVICE_ROLE_KEY environment variable is not set.");
}

const supabase = SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const app = express();
app.use(cors());
app.use(express.json());

let sock = null;
let activeSignalKeyStore = null;
let currentQr = null;
let connectionStatus = "initializing"; // 'connecting' | 'open' | 'close' | 'waiting_for_scan' | 'reconnecting' | 'logged_out'
let isConnecting = false;
let reconnectTimer = null;
let suppressBackup = false;

const runtimeLogs = [];
function pushRuntimeLog(type, msg) {
  runtimeLogs.unshift({ time: new Date().toISOString(), type, msg });
  if (runtimeLogs.length > 150) runtimeLogs.pop();
}

const origLog = console.log;
const origWarn = console.warn;
const origErr = console.error;
console.log = (...args) => {
  origLog(...args);
  pushRuntimeLog("log", args.map((a) => (typeof a === "object" ? JSON.stringify(a) : String(a))).join(" "));
};
console.warn = (...args) => {
  origWarn(...args);
  pushRuntimeLog("warn", args.map((a) => (typeof a === "object" ? JSON.stringify(a) : String(a))).join(" "));
};
console.error = (...args) => {
  origErr(...args);
  pushRuntimeLog("error", args.map((a) => (typeof a === "object" ? JSON.stringify(a) : String(a))).join(" "));
};

const recentMessages = [];
function logMessageEvent(event, data) {
  recentMessages.unshift({ time: new Date().toISOString(), event, data });
  if (recentMessages.length > 60) recentMessages.pop();
}

// Lightweight zero-dep TTL CacheStore compatible with Baileys SocketConfig caches
function createTtlCache(ttlMs, maxEntries = 1000) {
  const store = new Map();
  return {
    get(key) {
      const entry = store.get(key);
      if (!entry) return undefined;
      if (Date.now() > entry.expiresAt) {
        store.delete(key);
        return undefined;
      }
      return entry.value;
    },
    set(key, value) {
      if (store.size >= maxEntries && !store.has(key)) {
        const oldestKey = store.keys().next().value;
        if (oldestKey !== undefined) store.delete(oldestKey);
      }
      store.set(key, { value, expiresAt: Date.now() + ttlMs });
    },
    del(key) {
      store.delete(key);
    },
    flushAll() {
      store.clear();
    },
  };
}

const msgRetryCounterCache = createTtlCache(60 * 60 * 1000, 1000);
const userDevicesCache = createTtlCache(5 * 60 * 1000, 500);

// Bidirectional LID <-> PN user maps for retry session healing
const lidToPnUser = new Map();
const pnToLidUser = new Map();

function recordLidPnPair(lidJid, pnJid) {
  if (!lidJid || !pnJid) return;
  const lidDecoded = jidDecode(lidJid);
  const pnDecoded = jidDecode(pnJid);
  if (!lidDecoded?.user || !pnDecoded?.user) return;
  if (lidDecoded.server === "lid" && pnDecoded.server === "s.whatsapp.net") {
    lidToPnUser.set(lidDecoded.user, pnDecoded.user);
    pnToLidUser.set(pnDecoded.user, lidDecoded.user);
  } else if (pnDecoded.server === "lid" && lidDecoded.server === "s.whatsapp.net") {
    lidToPnUser.set(pnDecoded.user, lidDecoded.user);
    pnToLidUser.set(lidDecoded.user, pnDecoded.user);
  }
}

// Persistent message store for Baileys getMessage() retry handler
const messageStore = new Map(); // msgId -> base64 encoded proto.Message
let msgCacheLoaded = false;
let msgCacheTimer = null;

function rememberMessage(msgId, message) {
  if (!msgId || !message || message.protocolMessage) return;
  try {
    const protoMsg = message instanceof proto.Message ? message : proto.Message.fromObject(message);
    const bytes = proto.Message.encode(protoMsg).finish();
    const b64 = Buffer.from(bytes).toString("base64");
    if (messageStore.has(msgId)) {
      messageStore.delete(msgId);
    }
    messageStore.set(msgId, b64);
    while (messageStore.size > MAX_MEMORY_MESSAGES) {
      const oldestKey = messageStore.keys().next().value;
      if (oldestKey !== undefined) messageStore.delete(oldestKey);
      else break;
    }
    scheduleMsgCacheBackup();
  } catch (err) {
    console.warn(`[Baileys] Failed to cache message ${msgId}:`, err.message);
  }
}

function getStoredMessage(msgId) {
  if (!msgId) return undefined;
  const b64 = messageStore.get(msgId);
  if (!b64) return undefined;
  try {
    return proto.Message.decode(Buffer.from(b64, "base64"));
  } catch (err) {
    console.warn(`[Baileys] Failed to decode cached message ${msgId}:`, err.message);
    messageStore.delete(msgId);
    return undefined;
  }
}

async function loadMessageCacheFromSupabase() {
  if (msgCacheLoaded || !supabase) return;
  msgCacheLoaded = true;
  try {
    const { data, error } = await supabase
      .from("app_settings")
      .select("whatsapp_gateway_key")
      .eq("id", MSG_CACHE_ROW_ID)
      .maybeSingle();

    if (error || !data?.whatsapp_gateway_key) return;
    const entries = JSON.parse(data.whatsapp_gateway_key);
    if (Array.isArray(entries)) {
      for (const [id, b64] of entries) {
        if (typeof id === "string" && typeof b64 === "string") {
          messageStore.set(id, b64);
        }
      }
      console.log(`[Baileys] Restored ${messageStore.size} cached message(s) for retry recovery.`);
    }
  } catch (err) {
    console.warn("[Baileys] Could not load message cache from Supabase:", err.message);
  }
}

async function flushMessageCacheToSupabase() {
  clearTimeout(msgCacheTimer);
  msgCacheTimer = null;
  if (!supabase || suppressBackup) return;
  try {
    const entries = Array.from(messageStore.entries()).slice(-MAX_PERSISTED_MESSAGES);
    await supabase.from("app_settings").upsert({
      id: MSG_CACHE_ROW_ID,
      whatsapp_gateway_key: JSON.stringify(entries),
      whatsapp_gateway_enabled: true,
      updated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.warn("[Baileys] Error saving message cache to Supabase:", err.message);
  }
}

function scheduleMsgCacheBackup() {
  clearTimeout(msgCacheTimer);
  msgCacheTimer = setTimeout(() => {
    flushMessageCacheToSupabase().catch(() => {});
  }, 2500);
}

// Pino stream that writes to stdout and forwards Baileys warnings/errors into /logs
const pinoLogStream = {
  write(chunk) {
    process.stdout.write(chunk);
    try {
      const parsed = JSON.parse(chunk);
      if (parsed && parsed.level >= 40) {
        const msgText = parsed.msg || "Baileys warning";
        const extra = {};
        for (const [k, v] of Object.entries(parsed)) {
          if (!["level", "time", "pid", "hostname", "msg"].includes(k)) {
            extra[k] = v;
          }
        }
        const suffix = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
        pushRuntimeLog(parsed.level >= 50 ? "error" : "warn", `[Baileys:pino] ${msgText}${suffix}`);
      }
    } catch {}
  },
};

// Restore session from Supabase ONLY on cold boot (when local creds.json does not exist)
async function restoreSessionFromSupabase({ force = false } = {}) {
  if (!supabase) return;
  const credsPath = path.join(AUTH_DIR, "creds.json");
  if (!force && fs.existsSync(credsPath)) {
    return;
  }

  try {
    const { data, error } = await supabase
      .from("app_settings")
      .select("whatsapp_gateway_key")
      .eq("id", SESSION_ROW_ID)
      .maybeSingle();

    if (error || !data?.whatsapp_gateway_key) return;

    const files = JSON.parse(data.whatsapp_gateway_key);
    const entries = Object.entries(files || {});
    if (entries.length === 0) return;

    fs.rmSync(AUTH_DIR, { recursive: true, force: true });
    fs.mkdirSync(AUTH_DIR, { recursive: true });

    for (const [filename, content] of entries) {
      const safeName = path.basename(filename);
      if (!safeName || typeof content !== "string" || content.length === 0) continue;
      fs.writeFileSync(path.join(AUTH_DIR, safeName), content, "utf-8");
    }
    console.log(`[Baileys] Restored ${entries.length} session files from Supabase.`);
  } catch (err) {
    console.error("[Baileys] Could not restore session from Supabase:", err.message);
  }
}

// Serialized backup to Supabase so concurrent Signal ratchet writes never overwrite newer snapshots
let saveDebounceTimer = null;
let backupInFlight = false;
let backupQueued = false;

async function performBackupToSupabase() {
  if (!supabase || suppressBackup) return;
  if (backupInFlight) {
    backupQueued = true;
    return;
  }

  backupInFlight = true;
  try {
    do {
      backupQueued = false;
      if (suppressBackup || !fs.existsSync(AUTH_DIR)) break;

      const fileNames = fs.readdirSync(AUTH_DIR);
      if (fileNames.length === 0 || !fileNames.includes("creds.json")) break;

      const files = {};
      for (const name of fileNames) {
        const filePath = path.join(AUTH_DIR, name);
        try {
          if (fs.statSync(filePath).isFile()) {
            const content = fs.readFileSync(filePath, "utf-8");
            if (content.length > 0) {
              files[name] = content;
            }
          }
        } catch {}
      }

      if (!files["creds.json"]) break;

      await supabase.from("app_settings").upsert({
        id: SESSION_ROW_ID,
        whatsapp_gateway_key: JSON.stringify(files),
        whatsapp_gateway_enabled: true,
        updated_at: new Date().toISOString(),
      });
      console.log(`[Baileys] Saved ${Object.keys(files).length} session files to Supabase.`);
    } while (backupQueued && !suppressBackup);
  } catch (err) {
    console.error("[Baileys] Error backing up session to Supabase:", err.message);
  } finally {
    backupInFlight = false;
  }
}

function backupSessionToSupabase(delayMs = 1500) {
  if (suppressBackup) return;
  clearTimeout(saveDebounceTimer);
  saveDebounceTimer = setTimeout(() => {
    saveDebounceTimer = null;
    performBackupToSupabase().catch(() => {});
  }, delayMs);
}

async function flushBackupToSupabase() {
  clearTimeout(saveDebounceTimer);
  saveDebounceTimer = null;
  await performBackupToSupabase();
}

function clearPairwiseSessionFiles() {
  if (!fs.existsSync(AUTH_DIR)) return 0;
  let clearedCount = 0;
  for (const name of fs.readdirSync(AUTH_DIR)) {
    if (name.startsWith("session-") || name.startsWith("sender-key-")) {
      try {
        fs.unlinkSync(path.join(AUTH_DIR, name));
        clearedCount++;
      } catch {}
    }
  }
  return clearedCount;
}

async function repairStaleSessionsIfNeeded() {
  const credsPath = path.join(AUTH_DIR, "creds.json");
  const markerPath = path.join(AUTH_DIR, REPAIR_MARKER_FILE);
  if (!fs.existsSync(credsPath) || fs.existsSync(markerPath)) {
    return;
  }

  const clearedCount = clearPairwiseSessionFiles();
  fs.writeFileSync(markerPath, new Date().toISOString(), "utf-8");
  console.log(
    `[Baileys] One-time Signal session repair completed: cleared ${clearedCount} stale session/sender-key files while preserving login creds.`
  );
  await flushBackupToSupabase();
}

function ensureRepairMarker() {
  try {
    if (fs.existsSync(AUTH_DIR) && fs.existsSync(path.join(AUTH_DIR, "creds.json"))) {
      const markerPath = path.join(AUTH_DIR, REPAIR_MARKER_FILE);
      if (!fs.existsSync(markerPath)) {
        fs.writeFileSync(markerPath, new Date().toISOString(), "utf-8");
      }
    }
  } catch {}
}

function cleanupSocket(targetSock) {
  if (!targetSock) return;
  try {
    targetSock.ev.removeAllListeners();
  } catch {}
  try {
    targetSock.end(undefined);
  } catch {}
}

async function connectToWhatsApp() {
  if (isConnecting) return;
  isConnecting = true;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  suppressBackup = false;

  try {
    await loadMessageCacheFromSupabase();
    await restoreSessionFromSupabase();
    await repairStaleSessionsIfNeeded();

    const logger = pino({ level: "warn" }, pinoLogStream);
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version, isLatest } = await fetchLatestBaileysVersion();

    console.log(`[Baileys] Using WhatsApp Web v${version.join(".")} (latest: ${isLatest})`);

    // Wrap state.keys so every Signal session ratchet & pre-key update is backed up to Supabase
    const trackedKeys = {
      get: (type, ids) => state.keys.get(type, ids),
      set: async (data) => {
        await state.keys.set(data);
        backupSessionToSupabase(1500);
      },
    };

    const signalKeyStore = makeCacheableSignalKeyStore(trackedKeys, logger);
    activeSignalKeyStore = signalKeyStore;

    cleanupSocket(sock);

    const thisSock = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        keys: signalKeyStore,
      },
      logger,
      printQRInTerminal: false,
      browser: Browsers.ubuntu("Chrome"),
      syncFullHistory: false,
      msgRetryCounterCache,
      userDevicesCache,
      getMessage: async (key) => {
        const msg = getStoredMessage(key?.id);
        const requester = key?.participant || key?.remoteJid || "";
        console.log(
          `[Baileys] getMessage retry lookup: id=${key?.id} remoteJid=${key?.remoteJid} participant=${key?.participant} found=${Boolean(msg)}`
        );

        // If retry arrived from an @lid JID, also refresh the corresponding @s.whatsapp.net session
        // because Baileys 6.7.x relayMessage to PN addresses uses PN Signal sessions.
        if (requester.includes("@lid") && sock === thisSock && connectionStatus === "open") {
          try {
            const decoded = jidDecode(requester);
            let mappedPnUser = decoded?.user ? lidToPnUser.get(decoded.user) : undefined;
            if (!mappedPnUser && thisSock.user?.lid && areJidsSameUser(requester, thisSock.user.lid)) {
              mappedPnUser = jidDecode(thisSock.user.id)?.user;
            }
            if (mappedPnUser) {
              const pnJid = jidEncode(mappedPnUser, "s.whatsapp.net", decoded?.device);
              console.log(`[Baileys] Refreshing mapped PN session ${pnJid} for LID retry ${requester}`);
              await thisSock.assertSessions([pnJid], true);
            }
          } catch (err) {
            console.warn(`[Baileys] Failed refreshing mapped PN session for ${requester}:`, err.message);
          }
        }

        return msg;
      },
    });

    sock = thisSock;

    if (state.creds?.me?.lid && state.creds?.me?.id) {
      recordLidPnPair(state.creds.me.lid, state.creds.me.id);
    }

    // Track retry receipts and error acks explicitly for diagnostics
    thisSock.ws.on("CB:receipt", (node) => {
      if (sock !== thisSock) return;
      if (node?.attrs?.type === "retry") {
        const info = {
          id: node.attrs.id,
          from: node.attrs.from,
          participant: node.attrs.participant,
          recipient: node.attrs.recipient,
        };
        console.warn(`[Baileys] Received retry receipt:`, JSON.stringify(info));
        logMessageEvent("retry", info);
      }
    });

    thisSock.ws.on("CB:ack,class:message", (node) => {
      if (sock !== thisSock) return;
      if (node?.attrs?.error) {
        console.warn(`[Baileys] Received message error ack:`, JSON.stringify(node.attrs));
        logMessageEvent("ack_error", node.attrs);
      }
    });

    thisSock.ev.on("chats.phoneNumberShare", ({ lid, jid }) => {
      if (sock !== thisSock) return;
      recordLidPnPair(lid, jid);
    });

    thisSock.ev.on("contacts.upsert", (contacts) => {
      if (sock !== thisSock) return;
      for (const c of contacts || []) {
        if (c?.lid && c?.id) recordLidPnPair(c.lid, c.id);
      }
    });

    thisSock.ev.on("contacts.update", (contacts) => {
      if (sock !== thisSock) return;
      for (const c of contacts || []) {
        if (c?.lid && c?.id) recordLidPnPair(c.lid, c.id);
      }
    });

    thisSock.ev.on("messages.upsert", async ({ messages }) => {
      if (sock !== thisSock) return;
      for (const m of messages) {
        if (m.key?.id && m.message && !m.message.protocolMessage) {
          rememberMessage(m.key.id, m.message);
        }
        logMessageEvent("upsert", {
          id: m.key?.id,
          fromMe: m.key?.fromMe,
          remoteJid: m.key?.remoteJid,
          status: m.status,
        });
      }
    });

    thisSock.ev.on("messages.update", async (updates) => {
      if (sock !== thisSock) return;
      for (const u of updates) {
        logMessageEvent("update", { id: u.key?.id, remoteJid: u.key?.remoteJid, update: u.update });
      }
    });

    thisSock.ev.on("message-receipt.update", async (receipts) => {
      if (sock !== thisSock) return;
      for (const r of receipts) {
        logMessageEvent("receipt", { key: r.key, receipt: r.receipt });
      }
    });

    thisSock.ev.on("creds.update", async () => {
      if (sock !== thisSock) return;
      await saveCreds();
      if (thisSock?.user?.lid && thisSock?.user?.id) {
        recordLidPnPair(thisSock.user.lid, thisSock.user.id);
      }
      if (thisSock?.user?.id && connectionStatus === "open") {
        currentQr = null;
      }
      backupSessionToSupabase(1500);
    });

    thisSock.ev.on("connection.update", async (update) => {
      if (sock !== thisSock) return;
      const { connection, lastDisconnect, qr } = update;

      if (qr && !thisSock?.user?.id) {
        currentQr = qr;
        connectionStatus = "waiting_for_scan";
        console.log("\n=================== SCAN WHATSAPP QR CODE ===================");
        qrcodeTerminal.generate(qr, { small: true });
        console.log(`Or view QR in browser: http://localhost:${PORT}/qr\n`);
      }

      if (connection === "close") {
        currentQr = null;
        const statusCode =
          lastDisconnect?.error instanceof Boom ? lastDisconnect.error.output?.statusCode : null;

        const isLoggedOut = statusCode === DisconnectReason.loggedOut;
        const shouldReconnect = !isLoggedOut;
        console.log(
          `[Baileys] Connection closed:`,
          lastDisconnect?.error?.message,
          `(code: ${statusCode}) | Reconnecting:`,
          shouldReconnect
        );

        if (shouldReconnect) {
          connectionStatus = "reconnecting";
          const delay =
            statusCode === DisconnectReason.connectionReplaced
              ? 5000
              : statusCode === DisconnectReason.restartRequired
              ? 1000
              : 3000;
          clearTimeout(reconnectTimer);
          reconnectTimer = setTimeout(() => {
            connectToWhatsApp().catch(console.error);
          }, delay);
        } else {
          connectionStatus = "logged_out";
          console.log("[Baileys] Explicitly logged out. Clearing credentials...");
          await resetAllStoredAuth();
          clearTimeout(reconnectTimer);
          reconnectTimer = setTimeout(() => {
            connectToWhatsApp().catch(console.error);
          }, 3000);
        }
      } else if (connection === "open") {
        currentQr = null;
        connectionStatus = "open";
        if (thisSock.user?.lid && thisSock.user?.id) {
          recordLidPnPair(thisSock.user.lid, thisSock.user.id);
        }
        ensureRepairMarker();
        console.log(`\n✅ [Baileys] WhatsApp connected successfully as: ${thisSock.user?.id || "Authorized User"}\n`);
        backupSessionToSupabase(500);
      } else if (connection === "connecting") {
        if (connectionStatus !== "open") {
          connectionStatus = "connecting";
        }
      }
    });
  } finally {
    isConnecting = false;
  }
}

async function resetAllStoredAuth() {
  suppressBackup = true;
  clearTimeout(saveDebounceTimer);
  saveDebounceTimer = null;
  clearTimeout(msgCacheTimer);
  msgCacheTimer = null;
  backupQueued = false;

  messageStore.clear();
  lidToPnUser.clear();
  pnToLidUser.clear();
  userDevicesCache.flushAll();
  msgRetryCounterCache.flushAll();

  try {
    await activeSignalKeyStore?.clear?.();
  } catch {}
  activeSignalKeyStore = null;

  try {
    fs.rmSync(AUTH_DIR, { recursive: true, force: true });
  } catch {}

  if (supabase) {
    try {
      await supabase.from("app_settings").delete().in("id", [SESSION_ROW_ID, MSG_CACHE_ROW_ID]);
    } catch {}
  }
}

// 1. Health / Status check endpoint
app.get("/status", (req, res) => {
  const isConnected = connectionStatus === "open" && Boolean(sock?.user?.id);
  res.json({
    connected: isConnected,
    status: isConnected ? "open" : connectionStatus,
    user: isConnected ? sock?.user : null,
    hasQr: Boolean(currentQr) && !isConnected,
    cachedMessages: messageStore.size,
  });
});

app.get("/health", (req, res) => {
  const isConnected = connectionStatus === "open" && Boolean(sock?.user?.id);
  res.json({ ok: true, status: isConnected ? "open" : connectionStatus });
});

// 2. Interactive Web QR Code endpoint
app.get("/qr", async (req, res) => {
  if (connectionStatus === "open") {
    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Turnly WhatsApp Gateway</title>
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #0f172a; color: white; }
            .card { background: #1e293b; padding: 2.5rem; border-radius: 1rem; text-align: center; max-width: 420px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); border: 1px solid #334155; }
            .badge { display: inline-block; padding: 0.35rem 0.85rem; border-radius: 9999px; background: #059669; color: #ecfdf5; font-size: 0.875rem; font-weight: 600; margin-bottom: 1rem; }
          </style>
        </head>
        <body>
          <div class="card">
            <span class="badge">Connected</span>
            <h2>WhatsApp Linked!</h2>
            <p style="color: #94a3b8; font-size: 0.95rem;">Turnly WhatsApp Gateway is live and sending notifications to all flatmates directly.</p>
            <p style="font-size: 0.85rem; color: #64748b; margin-top: 1rem;">Connected Account: ${sock?.user?.id || "Active"}</p>
            <div style="margin-top: 1.5rem;">
              <a href="/relink" onclick="return confirm('Do you want to log out this WhatsApp account and scan a new QR code?');" style="display: inline-block; padding: 0.65rem 1.25rem; background: #e11d48; color: white; border-radius: 0.5rem; text-decoration: none; font-size: 0.9rem; font-weight: 600;">Log Out & Scan New QR Code</a>
            </div>
          </div>
        </body>
      </html>
    `);
  }

  if (!currentQr) {
    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Turnly WhatsApp Gateway</title>
          <meta http-equiv="refresh" content="2">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #0f172a; color: white; }
            .card { background: #1e293b; padding: 2.5rem; border-radius: 1rem; text-align: center; max-width: 420px; border: 1px solid #334155; }
          </style>
        </head>
        <body>
          <div class="card">
            <h2>Initializing WhatsApp Gateway...</h2>
            <p style="color: #94a3b8;">Connecting to WhatsApp servers, please wait a moment...</p>
          </div>
        </body>
      </html>
    `);
  }

  try {
    const qrDataUrl = await QRCode.toDataURL(currentQr, { width: 300, margin: 2 });
    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Scan WhatsApp QR Code</title>
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <meta http-equiv="refresh" content="15">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0f172a; color: white; }
            .card { background: #1e293b; padding: 2.5rem; border-radius: 1.25rem; text-align: center; max-width: 440px; box-shadow: 0 15px 35px rgba(0,0,0,0.5); border: 1px solid #334155; }
            img { background: white; padding: 0.75rem; border-radius: 0.75rem; box-shadow: 0 4px 6px rgba(0,0,0,0.2); }
            h2 { margin-top: 0; margin-bottom: 0.5rem; }
            ol { text-align: left; color: #94a3b8; font-size: 0.9rem; line-height: 1.6; margin: 1.25rem 0; padding-left: 1.25rem; }
          </style>
        </head>
        <body>
          <div class="card">
            <h2>Link WhatsApp to Turnly</h2>
            <ol>
              <li>Open <b>WhatsApp</b> on your phone</li>
              <li>Tap <b>Menu (⋮)</b> or <b>Settings</b> &rarr; <b>Linked Devices</b></li>
              <li>Tap <b>Link a Device</b> and point your camera at this QR code:</li>
            </ol>
            <img src="${qrDataUrl}" alt="WhatsApp QR Code" />
            <p style="color: #64748b; font-size: 0.8rem; margin-top: 1.25rem;">Refreshes automatically every 15s</p>
          </div>
        </body>
      </html>
    `);
  } catch (err) {
    res.status(500).send("Error rendering QR code");
  }
});

// Re-link endpoint: Clears session and forces new QR code generation
app.get("/relink", async (req, res) => {
  try {
    console.log("[Baileys] Manual relink requested. Clearing session and regenerating QR code...");
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
    connectionStatus = "waiting_for_scan";
    currentQr = null;

    const oldSock = sock;
    sock = null;
    if (oldSock) {
      try {
        await Promise.race([
          oldSock.logout(),
          new Promise((resolve) => setTimeout(resolve, 3000)),
        ]);
      } catch {}
      cleanupSocket(oldSock);
    }

    await resetAllStoredAuth();
    reconnectTimer = setTimeout(() => {
      connectToWhatsApp().catch(console.error);
    }, 1000);
    return res.redirect("/qr");
  } catch (err) {
    return res.status(500).send("Error clearing session: " + err.message);
  }
});

// Reset E2EE Signal sessions without logging out or requiring QR re-scan
app.post("/reset-sessions", async (req, res) => {
  try {
    const clearedCount = clearPairwiseSessionFiles();
    ensureRepairMarker();
    if (activeSignalKeyStore?.clear) {
      await activeSignalKeyStore.clear();
    }
    userDevicesCache.flushAll();
    msgRetryCounterCache.flushAll();
    await flushBackupToSupabase();

    console.log(`[Baileys] Manual Signal session reset: cleared ${clearedCount} session/sender-key files.`);
    return res.json({
      success: true,
      clearedCount,
      message: `Cleared ${clearedCount} pairwise Signal session(s). Next outgoing messages will establish fresh E2EE sessions automatically.`,
    });
  } catch (err) {
    console.error("[Baileys] Error resetting Signal sessions:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Check phone registration on WhatsApp
app.get("/check", async (req, res) => {
  const { phone } = req.query;
  if (!phone) return res.status(400).json({ error: "Missing phone parameter" });

  if (connectionStatus !== "open" || !sock) {
    return res.status(503).json({ error: "WhatsApp socket is not connected" });
  }

  try {
    const digits = String(phone).replace(/\D/g, "").slice(-10);
    const jid = `91${digits}@s.whatsapp.net`;
    const result = await sock.onWhatsApp(jid);
    if (Array.isArray(result)) {
      for (const item of result) {
        if (item?.lid && item?.jid) recordLidPnPair(item.lid, item.jid);
      }
    }
    return res.json({ query: phone, jid, result });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/receipts", (req, res) => {
  res.json({ count: recentMessages.length, messages: recentMessages });
});

app.get("/logs", (req, res) => {
  res.json({ count: runtimeLogs.length, logs: runtimeLogs });
});

// 3. Message Dispatch Endpoint (with in-flight deduplication for identical concurrent calls)
const inFlightSends = new Map();

app.post("/send", async (req, res) => {
  const { phone, text } = req.body;

  if (!phone || !text) {
    return res.status(400).json({ success: false, error: "Missing 'phone' or 'text'" });
  }

  // If socket is currently connecting or reconnecting, wait up to 20s for connection to reach 'open'
  if ((connectionStatus === "connecting" || connectionStatus === "reconnecting") && !sock?.user?.id) {
    console.log(`[Baileys] Socket is currently '${connectionStatus}'. Waiting up to 20s for connection to open...`);
    const startWait = Date.now();
    while (Date.now() - startWait < 20000) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      if (connectionStatus === "open" && sock?.user?.id) {
        console.log(`[Baileys] Socket reached 'open' state after ${Date.now() - startWait}ms.`);
        break;
      }
    }
  }

  const isConnected = connectionStatus === "open" && Boolean(sock?.user?.id);
  if (!isConnected || !sock) {
    return res.status(503).json({
      success: false,
      error: `WhatsApp socket is not connected (current status: ${connectionStatus}). Please scan QR code at /qr first.`,
    });
  }

  try {
    let targetJid = "";
    if (phone.includes("@g.us") || phone.includes("@lid") || phone.includes("@s.whatsapp.net")) {
      targetJid = phone.trim();
    } else {
      const digits = phone.replace(/\D/g, "").slice(-10);
      if (digits.length !== 10) {
        return res.status(400).json({ success: false, error: "Invalid 10-digit phone number" });
      }
      const rawJid = `91${digits}@s.whatsapp.net`;

      // Verify with WhatsApp servers that the contact exists
      const [waCheck] = await sock.onWhatsApp(rawJid);
      if (!waCheck || !waCheck.exists) {
        return res.status(404).json({
          success: false,
          error: `Phone number +91 ${digits} is not registered on WhatsApp.`,
        });
      }
      if (waCheck.lid && waCheck.jid) {
        recordLidPnPair(waCheck.lid, waCheck.jid);
      }
      targetJid = waCheck.jid || rawJid;
    }

    const trimmedText = text.trim();
    const inFlightKey = `${targetJid}:${trimmedText}`;
    let sendPromise = inFlightSends.get(inFlightKey);
    if (sendPromise) {
      console.log(`[Baileys] Coalescing concurrent duplicate send to ${targetJid}.`);
    } else {
      sendPromise = (async () => {
        console.log(`[Baileys] Sending to ${targetJid}...`);
        const sent = await sock.sendMessage(targetJid, { text: trimmedText });
        if (sent?.key?.id && sent?.message) {
          rememberMessage(sent.key.id, sent.message);
        }
        console.log(`[Baileys] Sent message details:`, JSON.stringify(sent));
        return sent;
      })().finally(() => {
        inFlightSends.delete(inFlightKey);
      });
      inFlightSends.set(inFlightKey, sendPromise);
    }

    const sent = await sendPromise;

    return res.json({
      success: true,
      targetJid,
      messageId: sent?.key?.id,
      timestamp: sent?.messageTimestamp,
    });
  } catch (err) {
    console.error("[Baileys] Error dispatching message to", phone, ":", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Failed to dispatch message via Baileys",
    });
  }
});

// 4. Background Self-Keep-Alive & Turnly Notification Scheduler
const GATEWAY_PUBLIC_URL = process.env.RENDER_EXTERNAL_URL || "https://turnly-wa-gateway.onrender.com";
const APP_CRON_URL = process.env.APP_CRON_URL || "https://duty-buddy-rotations-main.vercel.app/api/cron/notifications";

let lastExecutedSlot = "";

// Keep Render awake 24/7 by pinging itself every 10 minutes
setInterval(async () => {
  try {
    const res = await fetch(`${GATEWAY_PUBLIC_URL.replace(/\/$/, "")}/health`);
    if (res.ok) {
      console.log(`[Keep-Alive] Pinged ${GATEWAY_PUBLIC_URL}/health successfully.`);
    }
  } catch (err) {
    console.warn(`[Keep-Alive] Warning pinging self:`, err.message);
  }
}, 10 * 60 * 1000);

// Scheduled notification trigger: Runs every minute and checks Asia/Kolkata (IST) time
setInterval(async () => {
  try {
    const now = new Date();
    const istDateString = now.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" }); // YYYY-MM-DD
    const timeParts = now
      .toLocaleTimeString("en-US", { timeZone: "Asia/Kolkata", hour12: false, hour: "2-digit", minute: "2-digit" })
      .split(":");
    const rawHour = parseInt(timeParts[0], 10);
    const istHour = rawHour === 24 ? 0 : rawHour;
    const istMinute = parseInt(timeParts[1], 10);

    const slotKey = `${istDateString}-${istHour}`;

    // Target alert windows:
    // 1. Morning 6:00 AM IST (istHour === 6)
    // 2. Evening 6:00 PM to 11:00 PM IST (istHour >= 18 && istHour <= 23)
    const isTargetHour = istHour === 6 || (istHour >= 18 && istHour <= 23);

    // Fire within the first 5 minutes of the hour (e.g. 06:00, 18:00, etc.) once per slot
    if (isTargetHour && istMinute < 5 && lastExecutedSlot !== slotKey) {
      lastExecutedSlot = slotKey;
      console.log(
        `[Scheduler] ⏰ Triggering automated Turnly notifications for slot: ${slotKey} (${istHour}:${istMinute.toString().padStart(2, "0")} IST)...`
      );

      // If socket is reconnecting, give it up to 25s before triggering
      if (connectionStatus !== "open") {
        console.log(`[Scheduler] Waiting for WhatsApp socket to open before cron trigger...`);
        for (let i = 0; i < 25; i++) {
          if (connectionStatus === "open") break;
          await new Promise((r) => setTimeout(r, 1000));
        }
      }

      const cronUrl = `${APP_CRON_URL}?hour=${istHour}`;
      const res = await fetch(cronUrl, {
        method: "GET",
        headers: { "User-Agent": "Turnly-Scheduler/1.0" },
      });
      const data = await res.json().catch(() => ({}));
      console.log(`[Scheduler] Notification dispatch result for slot ${slotKey}:`, JSON.stringify(data));
    }
  } catch (err) {
    console.error(`[Scheduler] Error in automated cron check:`, err.message);
  }
}, 60 * 1000);

// Scheduler status endpoint
app.get("/scheduler", (req, res) => {
  const now = new Date();
  const istTime = now.toLocaleTimeString("en-US", { timeZone: "Asia/Kolkata" });
  const istDate = now.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
  res.json({
    status: "active",
    istTime,
    istDate,
    lastExecutedSlot,
    targetWindows: "06:00 AM (Morning Roster) & 18:00–23:00 (Hourly Evening Reminders)",
    gatewayUrl: GATEWAY_PUBLIC_URL,
    cronUrl: APP_CRON_URL,
  });
});

// Flush Signal keys and retry message cache on graceful container shutdown
async function gracefulShutdown(signal) {
  console.log(`[Turnly Gateway] Received ${signal}, flushing session & message caches...`);
  try {
    await Promise.all([flushBackupToSupabase(), flushMessageCacheToSupabase()]);
  } catch {}
  process.exit(0);
}

process.on("SIGTERM", () => {
  gracefulShutdown("SIGTERM").catch(() => process.exit(0));
});
process.on("SIGINT", () => {
  gracefulShutdown("SIGINT").catch(() => process.exit(0));
});

// Start express server and Baileys
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[Turnly Gateway] Server listening on port ${PORT}`);
  connectToWhatsApp().catch(console.error);
});
