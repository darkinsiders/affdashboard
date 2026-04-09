// Postback handler — Cloudflare Worker
// Writes to Firebase Firestore via REST API (no Firebase SDK needed)

const EVENT_MAP = {
  "registration": "reg",
  "signup":       "reg",
  "lead":         "reg",
  "reg":          "reg",
  "ftd":          "ftd",
  "qftd":         "ftd",
  "deposit":      "ftd",
  "sale":         "ftd",
  "conversion":   "ftd",
};

const PROJECT_ID = "affdashboard-3f1a3";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    let p = Object.fromEntries(url.searchParams);

    // Also accept POST body
    if (request.method === "POST") {
      try {
        const ct = request.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          Object.assign(p, await request.json());
        } else {
          Object.assign(p, Object.fromEntries(await request.formData()));
        }
      } catch (_) {}
    }

    // Auth — always return 200 so affiliate platform validation passes,
    // but only process if token is correct
    if (p.token !== env.POSTBACK_TOKEN) {
      return new Response("OK", { status: 200 });
    }

    const funnel    = p.funnel  || "";
    const network   = p.network || p.offer || "";
    const sub1      = p.sub1 || p.clickid || p.aff_sub || p.s1 || "";
    const source    = p.source || p.src || "";    // traffic source label (meta, tiktok, tg, etc.)
    const campaign  = p.campaign || p.c || "";    // campaign name
    const rawEvent  = (p.event || p.type || p.goal || p.status || "").toLowerCase();
    const revenue   = parseFloat(p.revenue || p.commission || p.amount || p.sum || 0) || 0;

    if (!funnel) return new Response("OK", { status: 200 });

    const eventType = EVENT_MAP[rawEvent] || null;
    const funnelKey = funnel.charAt(0).toUpperCase() + funnel.slice(1).toLowerCase();
    const date      = new Date().toISOString().split("T")[0];
    const ts        = new Date().toISOString();
    const docId     = `${funnelKey.toLowerCase()}-${date}`;

    let accessToken;
    try {
      accessToken = await getGoogleAccessToken(env.SA_EMAIL, env.SA_PRIVATE_KEY);
    } catch (e) {
      return new Response(`Auth error: ${e.message}`, { status: 500 });
    }

    // Log raw postback
    try { await addDoc(PROJECT_ID, "postbacks", {
      ts, funnelKey, network, sub1, source, campaign,
      rawEvent, eventType: eventType || "unknown", revenue,
      raw: JSON.stringify(p),
    }, accessToken); } catch(e) { return new Response(`Firestore log error: ${e.message}`, { status: 500 }); }

    // Telegram admin alert (fire-and-forget, never blocks response)
    if (env.TG_BOT_TOKEN && env.TG_CHAT_ID && eventType) {
      const emoji = eventType === "ftd" ? "💰" : "📋";
      const eventLabel = eventType === "ftd" ? "NEW FTD" : "New Reg";
      const revStr = (eventType === "ftd" && revenue > 0) ? `\n💵 Revenue: €${revenue.toFixed(2)}` : "";
      const srcStr = source ? `\n📡 Source: ${source}` : "";
      const camStr = campaign ? `\n🎯 Campaign: ${campaign}` : "";
      const msg = `${emoji} *${eventLabel}* — ${funnelKey}${revStr}${srcStr}${camStr}\n🕐 ${new Date().toUTCString()}`;
      fetch(`https://api.telegram.org/bot${env.TG_BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_id: env.TG_CHAT_ID, text: msg, parse_mode: "Markdown" }),
      }).catch(() => {}); // fire and forget
    }

    // Increment conversion counters
    if (eventType) {
      const fieldTransforms = [];
      if (eventType === "reg") {
        fieldTransforms.push({ fieldPath: "regs", increment: { integerValue: "1" } });
        // Per-source tracking
        if (source) {
          fieldTransforms.push({ fieldPath: `sources.${source}.regs`, increment: { integerValue: "1" } });
          if (campaign) {
            fieldTransforms.push({ fieldPath: `sources.${source}.campaigns.${campaign}.regs`, increment: { integerValue: "1" } });
          }
        }
      } else if (eventType === "ftd") {
        fieldTransforms.push({ fieldPath: "ftds", increment: { integerValue: "1" } });
        if (revenue > 0) {
          fieldTransforms.push({ fieldPath: "revenue", increment: { doubleValue: revenue } });
        }
        // Per-source tracking
        if (source) {
          fieldTransforms.push({ fieldPath: `sources.${source}.ftds`, increment: { integerValue: "1" } });
          if (revenue > 0) {
            fieldTransforms.push({ fieldPath: `sources.${source}.revenue`, increment: { doubleValue: revenue } });
          }
          if (campaign) {
            fieldTransforms.push({ fieldPath: `sources.${source}.campaigns.${campaign}.ftds`, increment: { integerValue: "1" } });
            if (revenue > 0) {
              fieldTransforms.push({ fieldPath: `sources.${source}.campaigns.${campaign}.revenue`, increment: { doubleValue: revenue } });
            }
          }
        }
      }

      await firestoreCommit(PROJECT_ID, [
        {
          update: {
            name: `projects/${PROJECT_ID}/databases/(default)/documents/conversions/${docId}`,
            fields: {
              funnel:    { stringValue: funnelKey },
              date:      { stringValue: date },
              updatedAt: { stringValue: ts },
            },
          },
          updateMask: { fieldPaths: ["funnel", "date", "updatedAt"] },
        },
        {
          transform: {
            document: `projects/${PROJECT_ID}/databases/(default)/documents/conversions/${docId}`,
            fieldTransforms,
          },
        },
      ], accessToken);
    }

    return new Response("OK", { status: 200 });
  }
};

// ── Google Service Account JWT → Access Token ────────────────────────────────

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

  const headerB64  = b64url(JSON.stringify(header));
  const payloadB64 = b64url(JSON.stringify(payload));
  const sigInput   = `${headerB64}.${payloadB64}`;

  // Handle both literal \n and actual newlines in the stored secret
  const pem = privateKeyPem.replace(/\\n/g, "\n");
  const keyB64 = pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, "");
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
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });

  const { access_token } = await res.json();
  return access_token;
}

// ── Firestore helpers ────────────────────────────────────────────────────────

async function addDoc(projectId, collection, data, token) {
  const fields = {};
  for (const [k, v] of Object.entries(data)) {
    if (typeof v === "string") fields[k] = { stringValue: v };
    else if (typeof v === "number") fields[k] = { doubleValue: v };
  }
  await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    }
  );
}

async function firestoreCommit(projectId, writes, token) {
  await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents:commit`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ writes }),
    }
  );
}
