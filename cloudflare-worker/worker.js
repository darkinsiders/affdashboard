// Cloudflare Worker — postback handler + weekly autopilot campaign manager

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

const PROJECT_ID  = "affdashboard-3f1a3";
const META_VER    = "v19.0";
const META_BASE   = `https://graph.facebook.com/${META_VER}`;

// ── Main export ───────────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    // ── Manual autopilot trigger ───────────────────────────────────────────
    if (url.pathname === "/run-autopilot") {
      const token = url.searchParams.get("token");
      if (token !== env.POSTBACK_TOKEN) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }
      ctx.waitUntil(runAutopilot(env));
      return new Response(
        JSON.stringify({ status: "started", message: "Autopilot run initiated" }),
        { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
      );
    }

    // ── Postback handler (existing) ───────────────────────────────────────
    let p = Object.fromEntries(url.searchParams);

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

    if (p.token !== env.POSTBACK_TOKEN) {
      return new Response("OK", { status: 200 });
    }

    const funnel    = p.funnel || p.sub2 || p.aff_sub2 || p.s2 || "";
    const network   = p.network || p.offer || "";
    const sub1      = p.sub1 || p.clickid || p.aff_sub || p.s1 || "";
    const source    = p.source || p.src || "";
    const campaign  = p.campaign || p.c || "";
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

    // Deduplication
    let isDuplicate = false;
    if (sub1 && eventType) {
      const safeId  = sub1.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 64);
      const dedupId = `${safeId}-${eventType}`;
      const dedupDoc = `projects/${PROJECT_ID}/databases/(default)/documents/dedup/${dedupId}`;
      try {
        await firestoreCommit(PROJECT_ID, [{
          update: {
            name: dedupDoc,
            fields: {
              clickid:   { stringValue: sub1 },
              eventType: { stringValue: eventType },
              funnelKey: { stringValue: funnelKey },
              createdAt: { stringValue: ts },
            },
          },
          currentDocument: { exists: false },
        }], accessToken);
      } catch (_) {
        isDuplicate = true;
      }
    }

    // Log raw postback
    try {
      await addDoc(PROJECT_ID, "postbacks", {
        ts, funnelKey, network, sub1, source, campaign,
        rawEvent, eventType: eventType || "unknown", revenue,
        isDuplicate,
        raw: JSON.stringify(p),
      }, accessToken);
    } catch (e) {
      return new Response(`Firestore log error: ${e.message}`, { status: 500 });
    }

    if (isDuplicate) return new Response("OK", { status: 200 });

    // Telegram alert
    if (env.TG_BOT_TOKEN && env.TG_CHAT_ID && eventType) {
      const emoji      = eventType === "ftd" ? "💰" : "📋";
      const eventLabel = eventType === "ftd" ? "NEW FTD" : "New Reg";
      const revStr     = (eventType === "ftd" && revenue > 0) ? `\n💵 Revenue: €${revenue.toFixed(2)}` : "";
      const srcStr     = source   ? `\n📡 Source: ${source}`     : "";
      const camStr     = campaign ? `\n🎯 Campaign: ${campaign}` : "";
      const msg = `${emoji} *${eventLabel}* — ${funnelKey}${revStr}${srcStr}${camStr}\n🕐 ${new Date().toUTCString()}`;
      fetch(`https://api.telegram.org/bot${env.TG_BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_id: env.TG_CHAT_ID, text: msg, parse_mode: "Markdown" }),
      }).catch(() => {});
    }

    // Increment conversion counters
    if (eventType) {
      const fieldTransforms = [];
      if (eventType === "reg") {
        fieldTransforms.push({ fieldPath: "regs", increment: { integerValue: "1" } });
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
  },

  // ── Weekly cron trigger ───────────────────────────────────────────────────
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runAutopilot(env));
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// AUTOPILOT — weekly Meta campaign health check + auto-fix
// ══════════════════════════════════════════════════════════════════════════════

async function runAutopilot(env) {
  const ts        = new Date().toISOString();
  const runDate   = ts.split("T")[0];
  let accessToken;

  try {
    accessToken = await getGoogleAccessToken(env.SA_EMAIL, env.SA_PRIVATE_KEY);
  } catch (e) {
    console.error("Autopilot: SA auth failed", e.message);
    return;
  }

  // ── 1. Read dashboard settings from Firestore ───────────────────────────
  let settings;
  try {
    settings = await firestoreGet(PROJECT_ID, "dashboard/data", accessToken);
  } catch (e) {
    console.error("Autopilot: Could not read dashboard/data", e.message);
    return;
  }

  if (!settings.autopilotEnabled) {
    console.log("Autopilot: disabled in settings, skipping");
    return;
  }

  const metaToken         = settings.metaToken;
  const metaAccountId     = settings.metaAccountId;
  const claudeApiKey      = settings.claudeApiKey;
  const aggressiveness    = settings.autopilotAggressiveness || "balanced";

  if (!metaToken || !metaAccountId || !claudeApiKey) {
    console.error("Autopilot: missing metaToken, metaAccountId, or claudeApiKey");
    return;
  }

  // ── 2. Fetch Meta account data ──────────────────────────────────────────
  let metaData;
  try {
    metaData = await fetchMetaAuditData(metaAccountId, metaToken);
  } catch (e) {
    console.error("Autopilot: Meta fetch failed", e.message);
    return;
  }

  // ── 3. Ask Claude for analysis + action list ────────────────────────────
  let analysis;
  try {
    analysis = await callClaudeAutopilot(claudeApiKey, metaData, aggressiveness);
  } catch (e) {
    console.error("Autopilot: Claude call failed", e.message);
    return;
  }

  const actions = analysis.actions || [];

  // ── 4. Save plan to Firestore for human approval ────────────────────────
  //    The dashboard will show these and let the user approve/reject each
  //    before anything is applied to Meta.
  await firestoreSet(PROJECT_ID, "autopilot_pending/latest", {
    ts,
    runDate,
    status:        "pending",
    aggressiveness,
    score:         analysis.score    || 0,
    summary:       analysis.summary  || "",
    topIssue:      analysis.top_issue || "",
    actions:       JSON.stringify(actions),
    campaignCount: metaData.campaigns.length,
    adCount:       metaData.ads.length,
  }, accessToken);

  // ── 5. Telegram: plan ready for approval ──────────────────────────────
  if (env.TG_BOT_TOKEN && env.TG_CHAT_ID) {
    const count  = actions.length;
    const grade  = scoreToGrade(analysis.score || 0);
    const modeStr = aggressiveness.charAt(0).toUpperCase() + aggressiveness.slice(1);

    let msg = `🤖 *Autopilot Plan Ready \u2014 ${runDate}*\n`;
    msg    += `📊 Score: *${analysis.score || 0}/100* (${grade})\n`;
    msg    += `⚙️ Mode: ${modeStr}\n`;
    msg    += `📋 ${count} action${count !== 1 ? "s" : ""} proposed\n`;
    if (analysis.top_issue) msg += `\n⚠️ *Top issue:* ${analysis.top_issue}\n`;
    msg    += `\n👉 Open the dashboard to review and approve`;

    fetch(`https://api.telegram.org/bot${env.TG_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: env.TG_CHAT_ID, text: msg, parse_mode: "Markdown" }),
    }).catch(() => {});
  }

  console.log(`Autopilot: done. Score=${analysis.score}, applied=${actionsApplied.filter(a=>a.applied).length}, failed=${actionsFailed.length}`);
}

// ── Fetch Meta campaign + ad data for audit ──────────────────────────────────

async function fetchMetaAuditData(accountId, metaToken) {
  const actId = accountId.startsWith("act_") ? accountId : `act_${accountId}`;
  const tok   = `access_token=${encodeURIComponent(metaToken)}`;

  // Campaigns
  const campsUrl = `${META_BASE}/${actId}/campaigns?fields=id,name,status,objective,daily_budget,lifetime_budget&limit=50&${tok}`;
  const campsRes = await fetch(campsUrl);
  const campsData = await campsRes.json();
  if (campsData.error) throw new Error(`Campaigns: ${campsData.error.message}`);

  // Ads with 7-day insights
  const adsUrl = `${META_BASE}/${actId}/ads?fields=id,name,status,campaign_id,adset_id,insights.date_preset(last_7d){spend,clicks,impressions,ctr,cpm,actions,cost_per_action_type,reach,frequency}&limit=100&${tok}`;
  const adsRes = await fetch(adsUrl);
  const adsData = await adsRes.json();
  if (adsData.error) throw new Error(`Ads: ${adsData.error.message}`);

  // Adsets with 7-day insights
  const setsUrl = `${META_BASE}/${actId}/adsets?fields=id,name,status,campaign_id,daily_budget,targeting,insights.date_preset(last_7d){spend,clicks,impressions,ctr,actions}&limit=100&${tok}`;
  const setsRes = await fetch(setsUrl);
  const setsData = await setsRes.json();
  if (setsData.error) throw new Error(`AdSets: ${setsData.error.message}`);

  return {
    campaigns: campsData.data  || [],
    adsets:    setsData.data   || [],
    ads:       adsData.data    || [],
  };
}

// ── Call Claude for autopilot analysis ───────────────────────────────────────

async function callClaudeAutopilot(claudeKey, data, aggressiveness) {
  const campsSummary = (data.campaigns || []).slice(0, 20).map(c => ({
    id:           c.id,
    name:         c.name,
    status:       c.status,
    objective:    c.objective,
    daily_budget: c.daily_budget ? (parseInt(c.daily_budget) / 100).toFixed(2) + " EUR" : "unknown",
  }));

  const adsSummary = (data.ads || []).slice(0, 50).map(a => {
    const ins = (a.insights && a.insights.data && a.insights.data[0]) || {};
    const conv = (ins.actions || []).find(x => x.action_type === "offsite_conversion.fb_pixel_lead" || x.action_type === "link_click");
    return {
      id:           a.id,
      name:         a.name,
      status:       a.status,
      campaign_id:  a.campaign_id,
      adset_id:     a.adset_id,
      spend_7d:     parseFloat(ins.spend || 0).toFixed(2) + " EUR",
      clicks_7d:    ins.clicks || 0,
      impressions:  ins.impressions || 0,
      ctr:          parseFloat(ins.ctr || 0).toFixed(3) + "%",
      cpm:          parseFloat(ins.cpm || 0).toFixed(2) + " EUR",
      conversions:  conv ? parseInt(conv.value || 0) : 0,
    };
  });

  const modeGuide = {
    conservative: "CONSERVATIVE MODE: Do NOT apply any API changes. Only create 'flag' type actions as recommendations for human review. Never pause, scale, or modify anything automatically.",
    balanced:     "BALANCED MODE: Pause ads with spend > €5 and 0 clicks. Pause adsets with CTR < 0.1% and spend > €10. Flag budget issues and scaling opportunities but don't change budgets. All other issues: flag only.",
    aggressive:   "AGGRESSIVE MODE: Pause all ads with spend > €3 and 0 conversions + 0 clicks. Reduce daily_budget by 20% on campaigns with CTR < 0.2%. Increase daily_budget by 25% on campaigns with CTR > 1.5% and conversions > 0. Pause entire campaigns that have spent > €20 with absolutely no clicks.",
  };

  const prompt = `You are an expert Meta Ads campaign manager for casino affiliate offers. Analyze this Meta Ads account and return a list of specific actions.

## ACCOUNT DATA (Last 7 days)

Campaigns (${campsSummary.length}):
${JSON.stringify(campsSummary, null, 2)}

Ads with performance (${adsSummary.length}):
${JSON.stringify(adsSummary, null, 2)}

## INSTRUCTIONS

${modeGuide[aggressiveness] || modeGuide.balanced}

Action types available:
- "pause_ad" — pause a specific ad (use target_id = ad ID)
- "pause_adset" — pause an ad set (use target_id = adset ID)
- "pause_campaign" — pause an entire campaign (use target_id = campaign ID)
- "scale_budget" — increase campaign daily_budget (target_id = campaign ID, value = new budget in cents, e.g. 6000 = €60)
- "reduce_budget" — decrease campaign daily_budget (target_id = campaign ID, value = new budget in cents)
- "flag" — flag for human review, no API change (use for anything you're unsure about)

Severity:
- "critical" — bleeding money with zero results
- "warning" — underperforming, needs attention
- "info" — opportunity or minor issue

Score the account 0-100:
- 90-100: Excellent — campaigns performing well, efficient spend
- 70-89:  Good — minor issues only
- 50-69:  Fair — several problems affecting performance
- 30-49:  Poor — significant waste or structural issues
- 0-29:   Critical — account needs immediate intervention

Return ONLY valid JSON, no markdown:
{
  "actions": [
    {
      "type": "pause_ad",
      "target_id": "123456789",
      "target_name": "Ad name here",
      "reason": "Spent €8.50 with 0 clicks in 7 days",
      "severity": "critical",
      "value": null
    }
  ],
  "score": 72,
  "summary": "2-3 sentences on overall account health and what actions were taken",
  "top_issue": "One sentence on the single biggest problem found"
}`;

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key":         claudeKey,
      "anthropic-version": "2023-06-01",
      "content-type":      "application/json",
    },
    body: JSON.stringify({
      model:      "claude-sonnet-4-6",
      max_tokens: 2000,
      messages:   [{ role: "user", content: prompt }],
    }),
  });

  const body = await res.json();
  if (body.error) throw new Error(body.error.message);

  let text = (body.content && body.content[0] && body.content[0].text) || "";
  text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/i, "").trim();
  return JSON.parse(text);
}

// ── Set (create or overwrite) a Firestore document ──────────────────────────

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

// ── Utility ──────────────────────────────────────────────────────────────────

function scoreToGrade(score) {
  if (score >= 90) return "A+";
  if (score >= 80) return "A";
  if (score >= 70) return "B";
  if (score >= 60) return "C";
  if (score >= 50) return "D";
  return "F";
}

// ══════════════════════════════════════════════════════════════════════════════
// Google Service Account JWT → Access Token
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

  const headerB64  = b64url(JSON.stringify(header));
  const payloadB64 = b64url(JSON.stringify(payload));
  const sigInput   = `${headerB64}.${payloadB64}`;

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

async function addDoc(projectId, collection, data, token) {
  const fields = {};
  for (const [k, v] of Object.entries(data)) {
    if      (typeof v === "string")  fields[k] = { stringValue: v };
    else if (typeof v === "number")  fields[k] = { doubleValue: v };
    else if (typeof v === "boolean") fields[k] = { booleanValue: v };
  }
  await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}`,
    {
      method:  "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body:    JSON.stringify({ fields }),
    }
  );
}

async function firestoreGet(projectId, docPath, token) {
  const res  = await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${docPath}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (json.error) throw new Error(json.error.message);
  return firestoreParseDoc(json);
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

async function firestoreCommit(projectId, writes, token) {
  await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents:commit`,
    {
      method:  "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body:    JSON.stringify({ writes }),
    }
  );
}
