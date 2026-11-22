import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers,
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

const SUPABASE_URL = process.env.SUPABASE_URL || "https://gspxzpzgdgmnydkjshjs.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("WARNING: SUPABASE_SERVICE_ROLE_KEY environment variable is not set.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY || "", {
  auth: { persistSession: false },
});

const app = express();
app.use(cors());
app.use(express.json());

let sock = null;
let currentQr = null;
let connectionStatus = "initializing"; // 'connecting' | 'open' | 'close' | 'waiting_for_scan'

const recentMessages = [];
function logMessageEvent(event, data) {
  recentMessages.unshift({ time: new Date().toISOString(), event, data });
  if (recentMessages.length > 50) recentMessages.pop();
}

// Restore session from Supabase on boot
async function restoreSessionFromSupabase() {
  try {
    const { data, error } = await supabase
      .from("app_settings")
      .select("whatsapp_gateway_key")
      .eq("id", "baileys_session")
      .maybeSingle();

    if (error || !data?.whatsapp_gateway_key) return;

    const files = JSON.parse(data.whatsapp_gateway_key);
    if (!fs.existsSync(AUTH_DIR)) {
      fs.mkdirSync(AUTH_DIR, { recursive: true });
    }

    for (const [filename, content] of Object.entries(files)) {
      const filePath = path.join(AUTH_DIR, filename);
      fs.writeFileSync(filePath, content, "utf-8");
    }
    console.log(`[Baileys] Restored ${Object.keys(files).length} session files from Supabase.`);
  } catch (err) {
    console.error("[Baileys] Could not restore session from Supabase:", err.message);
  }
}

// Backup session to Supabase
let saveDebounceTimer = null;
function backupSessionToSupabase() {
  clearTimeout(saveDebounceTimer);
  saveDebounceTimer = setTimeout(async () => {
    try {
      if (!fs.existsSync(AUTH_DIR)) return;
      const fileNames = fs.readdirSync(AUTH_DIR);
      if (fileNames.length === 0) return;

      const files = {};
      for (const name of fileNames) {
        const filePath = path.join(AUTH_DIR, name);
        if (fs.statSync(filePath).isFile()) {
          files[name] = fs.readFileSync(filePath, "utf-8");
        }
      }

      await supabase.from("app_settings").upsert({
        id: "baileys_session",
        whatsapp_gateway_key: JSON.stringify(files),
        whatsapp_gateway_enabled: true,
        updated_at: new Date().toISOString(),
      });
      console.log(`[Baileys] Saved ${fileNames.length} session files to Supabase.`);
    } catch (err) {
      console.error("[Baileys] Error backing up session to Supabase:", err.message);
    }
  }, 3000);
}

async function connectToWhatsApp() {
  await restoreSessionFromSupabase();

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version, isLatest } = await fetchLatestBaileysVersion();

  console.log(`[Baileys] Using WhatsApp Web v${version.join(".")} (latest: ${isLatest})`);

  sock = makeWASocket({
    version,
    auth: state,
    logger: pino({ level: "warn" }),
    printQRInTerminal: false,
    browser: Browsers.ubuntu("Chrome"),
    syncFullHistory: false,
  });

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    for (const m of messages) {
      logMessageEvent("upsert", { id: m.key?.id, fromMe: m.key?.fromMe, remoteJid: m.key?.remoteJid, status: m.status });
    }
  });

  sock.ev.on("messages.update", async (updates) => {
    for (const u of updates) {
      logMessageEvent("update", { id: u.key?.id, remoteJid: u.key?.remoteJid, update: u.update });
    }
  });

  sock.ev.on("message-receipt.update", async (receipts) => {
    for (const r of receipts) {
      logMessageEvent("receipt", { key: r.key, receipt: r.receipt });
    }
  });

  sock.ev.on("creds.update", async () => {
    await saveCreds();
    backupSessionToSupabase();
  });

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQr = qr;
      connectionStatus = "waiting_for_scan";
      console.log("\n=================== SCAN WHATSAPP QR CODE ===================");
      qrcodeTerminal.generate(qr, { small: true });
      console.log(`Or view QR in browser: http://localhost:${PORT}/qr\n`);
    }

    if (connection === "close") {
      currentQr = null;
      const statusCode = (lastDisconnect?.error instanceof Boom)
        ? lastDisconnect.error.output?.statusCode
        : null;

      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      console.log(`[Baileys] Connection closed:`, lastDisconnect?.error?.message, `| Reconnecting:`, shouldReconnect);

      if (shouldReconnect) {
        connectionStatus = "reconnecting";
        setTimeout(connectToWhatsApp, 3000);
      } else {
        connectionStatus = "logged_out";
        console.log("[Baileys] Logged out. Clearing credentials...");
        try {
          fs.rmSync(AUTH_DIR, { recursive: true, force: true });
          await supabase.from("app_settings").delete().eq("id", "baileys_session");
        } catch {}
        setTimeout(connectToWhatsApp, 3000);
      }
    } else if (connection === "open") {
      currentQr = null;
      connectionStatus = "open";
      console.log(`\n✅ [Baileys] WhatsApp connected successfully as: ${sock.user?.id || "Authorized User"}\n`);
      backupSessionToSupabase();
    }
  });
}

// 1. Health / Status check endpoint
app.get("/status", (req, res) => {
  res.json({
    connected: connectionStatus === "open",
    status: connectionStatus,
    user: sock?.user || null,
    hasQr: Boolean(currentQr),
  });
});

app.get("/health", (req, res) => {
  res.json({ ok: true, status: connectionStatus });
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
            <p style="font-size: 0.85rem; color: #64748b; margin-top: 1.5rem;">User ID: ${sock?.user?.id || "Active"}</p>
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
    return res.json({ query: phone, jid, result });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/receipts", (req, res) => {
  res.json({ count: recentMessages.length, messages: recentMessages });
});

// 3. Message Dispatch Endpoint
app.post("/send", async (req, res) => {
  const { phone, text } = req.body;

  if (!phone || !text) {
    return res.status(400).json({ success: false, error: "Missing 'phone' or 'text'" });
  }

  if (connectionStatus !== "open" || !sock) {
    return res.status(503).json({
      success: false,
      error: `WhatsApp socket is not connected (current status: ${connectionStatus}). Please scan QR code at /qr first.`,
    });
  }

  try {
    let targetJid = "";
    if (phone.includes("@g.us")) {
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
      targetJid = waCheck.jid;
    }

    console.log(`[Baileys] Sending to ${targetJid}...`);
    const sent = await sock.sendMessage(targetJid, { text: text.trim() });
    console.log(`[Baileys] Sent message ${sent?.key?.id} to ${targetJid}`);
    
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

// Start express server and Baileys
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[Turnly Gateway] Server listening on port ${PORT}`);
  connectToWhatsApp().catch(console.error);
});
