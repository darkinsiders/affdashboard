// affiliate-stats-worker/worker.js
// Hourly CellXpert affiliate stats fetcher
// Logs into each brand's affiliate dashboard, pulls breakdown stats, saves to Firestore

const PROJECT_ID = "affdashboard-3f1a3";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    if (url.pathname === "/run-stats") {
      const token = url.searchParams.get("token");
      if (token !== env.STATS_TOKEN) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }
      // Fire and return immediately — don't block response
      const promise = runStatsFetch(env);
      promise.catch(e => console.error("runStatsFetch error:", e.message));
      return new Response(
        JSON.stringify({ status: "started", message: "Stats fetch initiated" }),
        { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
      );
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runStatsFetch(env));
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// MAIN FETCH LOGIC
// ══════════════════════════════════════════════════════════════════════════════

async function runStatsFetch(env) {
  // ── Auth with Google ──────────────────────────────────────────────────────
  let accessToken;
  try {
    accessToken = await getGoogleAccessToken(env.SA_EMAIL, env.SA_PRIVATE_KEY);
  } catch (e) {
    console.error("SA auth failed:", e.message);
    return;
  }

  // ── Read deals from Firestore ─────────────────────────────────────────────
  let settings;
  try {
    settings = await firestoreGet(PROJECT_ID, "dashboard/data", accessToken);
  } catch (e) {
    console.error("Could not read dashboard/data:", e.message);
    return;
  }

  const deals = settings.deals || [];

  // ── Build date range: 1st of current month → today ───────────────────────
  const now   = new Date();
  const m     = String(now.getMonth() + 1).padStart(2, "0");
  const d     = String(now.getDate()).padStart(2, "0");
  const y     = now.getFullYear();
  const startDate = `${m}%2F01%2F${y}`;   // MM/01/YYYY URL-encoded
  const endDate   = `${m}%2F${d}%2F${y}`; // MM/DD/YYYY URL-encoded

  // ── Deduplicate accounts and fetch ───────────────────────────────────────
  const seen    = new Set();
  const results = [];
  let successCount = 0;
  let failCount    = 0;

  for (const deal of deals) {
    if (!deal || (deal.status || "active") === "archived") continue;

    // Only process CellXpert deals with a dashboard login URL set
    const network = deal.network || getNetworkFromUrl(deal.url || "");
    if (network !== "cellxpert") continue;
    if (!deal.username || !deal.password || !deal.dashboardUrl) continue;

    const domain = extractDomain(deal.dashboardUrl);
    if (!domain) continue;

    // Skip duplicate accounts (same username on same platform)
    const accountKey = `${deal.username}@${domain}`;
    if (seen.has(accountKey)) continue;
    seen.add(accountKey);

    try {
      // ── Step 1: Login ───────────────────────────────────────────────────
      const loginRes = await fetch(`https://${domain}/authenticate`, {
        method: "POST",
        headers: {
          "Content-Type":    "application/x-www-form-urlencoded",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: `user=${encodeURIComponent(deal.username)}&pass=${encodeURIComponent(deal.password)}`,
      });

      const loginData = await loginRes.json();
      if (!loginData.success || !loginData.message) {
        throw new Error(`Login failed: ${loginData.reason || "bad credentials"}`);
      }

      const bearerToken  = loginData.message;
      const affiliateUrl = deriveAffiliateName(domain);

      // ── Step 2: Fetch breakdown stats ───────────────────────────────────
      const statsUrl = [
        "https://affiliateapi.cellxpert.com/?command=processReport",
        `startDate=${startDate}`,
        `endDate=${endDate}`,
        "DateFormat=day",
        "day=true",
        "Country=true",
        "trackingCode=true",
        "Brand=true",
        `uniqueId=${Date.now()}`,
      ].join("&");

      const statsRes = await fetch(statsUrl, {
        headers: {
          Authorization:  `Bearer ${bearerToken}`,
          affiliate_url:  affiliateUrl,
          Accept:         "application/json, text/plain, */*",
        },
      });

      const rows = await statsRes.json();

      results.push({
        domain,
        affiliateUrl,
        dealId:   String(deal.id || ""),
        dealName: deal.name || domain,
        rows:     Array.isArray(rows) ? rows : [],
      });
      successCount++;
    } catch (e) {
      console.error(`Stats fetch failed for ${deal.name || domain}: ${e.message}`);
      results.push({
        domain,
        affiliateUrl:  deriveAffiliateName(domain),
        dealId:        String(deal.id || ""),
        dealName:      deal.name || domain,
        rows:          [],
        error:         e.message,
      });
      failCount++;
    }
  }

  // ── Save to Firestore ─────────────────────────────────────────────────────
  const fetchedAt = new Date().toISOString();
  try {
    await firestoreSet(PROJECT_ID, "affiliate_stats/current", {
      fetchedAt,
      data:         JSON.stringify(results),
      successCount: String(successCount),
      failCount:    String(failCount),
      monthStart:   `${y}-${m}-01`,
      today:        `${y}-${m}-${d}`,
    }, accessToken);
  } catch (e) {
    console.error("Failed to save stats to Firestore:", e.message);
  }

  // ── Telegram summary ──────────────────────────────────────────────────────
  if (env.TG_BOT_TOKEN && env.TG_CHAT_ID) {
    const totalClicks = results.reduce((s, r) =>
      s + (r.rows || []).reduce((rs, row) => rs + (parseInt(row.Unique_Visitors || row["Unique Visitors"]) || 0), 0), 0);
    const totalRegs = results.reduce((s, r) =>
      s + (r.rows || []).reduce((rs, row) => rs + (parseInt(row.Registrations) || 0), 0), 0);
    const totalFTDs = results.reduce((s, r) =>
      s + (r.rows || []).reduce((rs, row) => rs + (parseInt(row.QFTD) || 0), 0), 0);

    let msg = `📊 *Affiliate Stats Synced*\n`;
    msg += `✅ ${successCount} brand${successCount !== 1 ? "s" : ""} fetched`;
    if (failCount > 0) msg += ` | ❌ ${failCount} failed`;
    msg += `\n👆 Clicks MTD: *${totalClicks}*`;
    msg += `\n📋 Regs MTD: *${totalRegs}*`;
    msg += `\n💰 FTDs MTD: *${totalFTDs}*`;
    msg += `\n🕐 ${fetchedAt.slice(0, 16).replace("T", " ")} UTC`;

    fetch(`https://api.telegram.org/bot${env.TG_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: env.TG_CHAT_ID, text: msg, parse_mode: "Markdown" }),
    }).catch(() => {});
  }

  console.log(`Stats fetch done. success=${successCount}, failed=${failCount}`);
}

// ══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════════════════

function extractDomain(url) {
  try { return new URL(url).hostname; } catch { return null; }
}

function getNetworkFromUrl(url) {
  const u = url.toLowerCase();
  if (
    u.includes("cellxpert") || u.includes("padrinopartners") ||
    u.includes("cosmobetpartners") || u.includes("7ladies") ||
    u.includes("needforslots") || u.includes("velobetpartners") ||
    u.includes("rollettoaffiliate") || u.includes("ftdgallery") ||
    u.includes("spinania")
  ) return "cellxpert";
  if (u.includes("affilika") || u.includes("clickholyluck")) return "affilika";
  return "other";
}

function deriveAffiliateName(domain) {
  // track.padrinopartners.com → PadrinoPartners
  // track.cosmobetpartners.com → CosmobetPartners
  let name = domain
    .replace(/^(track|aff|affiliate)\./i, "")
    .replace(/\.(com|net|org|io|co\.uk)$/i, "");

  const suffixes = ["partners", "affiliates", "affiliate"];
  for (const suf of suffixes) {
    if (name.toLowerCase().endsWith(suf)) {
      const base = name.slice(0, -suf.length);
      return cap(base) + cap(suf);
    }
  }
  return cap(name);
}

function cap(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
}

// ══════════════════════════════════════════════════════════════════════════════
// Google Service Account → Access Token (same as main worker)
// ══════════════════════════════════════════════════════════════════════════════

async function getGoogleAccessToken(saEmail, privateKeyPem) {
  const now = Math.floor(Date.now() / 1000);
  const header  = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss:   saEmail,
    scope: "https://www.googleapis.com/auth/datastore",
    aud:   "https://oauth2.googleapis.com/token",
    exp:   now + 3600,
    iat:   now,
  };

  const b64url = str => btoa(str).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  const hB64 = b64url(JSON.stringify(header));
  const pB64 = b64url(JSON.stringify(payload));
  const sigInput = `${hB64}.${pB64}`;

  const pem      = privateKeyPem.replace(/\\n/g, "\n");
  const keyB64   = pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, "");
  const keyBytes = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));

  const cryptoKey = await crypto.subtle.importKey(
    "pkcs8", keyBytes,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false, ["sign"]
  );
  const sigBytes = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5", cryptoKey,
    new TextEncoder().encode(sigInput)
  );

  const sig = b64url(String.fromCharCode(...new Uint8Array(sigBytes)));
  const jwt = `${sigInput}.${sig}`;

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method:  "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body:    `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });

  const { access_token } = await res.json();
  return access_token;
}

// ══════════════════════════════════════════════════════════════════════════════
// Firestore helpers
// ══════════════════════════════════════════════════════════════════════════════

async function firestoreGet(projectId, docPath, token) {
  const res  = await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${docPath}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (json.error) throw new Error(json.error.message);
  return firestoreParseDoc(json);
}

async function firestoreSet(projectId, docPath, data, token) {
  const fields = {};
  for (const [k, v] of Object.entries(data)) {
    if      (typeof v === "string")  fields[k] = { stringValue: v };
    else if (typeof v === "number")  fields[k] = { integerValue: String(Math.round(v)) };
    else if (typeof v === "boolean") fields[k] = { booleanValue: v };
  }
  const res = await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${docPath}`,
    {
      method:  "PATCH",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body:    JSON.stringify({ fields }),
    }
  );
  const json = await res.json();
  if (json.error) throw new Error(json.error.message);
}

function firestoreParseDoc(doc) {
  const out = {};
  for (const [k, v] of Object.entries(doc.fields || {})) {
    out[k] = firestoreParseValue(v);
  }
  return out;
}

function firestoreParseValue(v) {
  if ("stringValue"  in v) return v.stringValue;
  if ("integerValue" in v) return parseInt(v.integerValue);
  if ("doubleValue"  in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue"    in v) return null;
  if ("mapValue"     in v) return firestoreParseDoc(v.mapValue);
  if ("arrayValue"   in v) return (v.arrayValue.values || []).map(firestoreParseValue);
  return null;
}
