const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");

admin.initializeApp();
const db = admin.firestore();

const POSTBACK_TOKEN = defineSecret("POSTBACK_TOKEN");

// Event type normalisation — filled in per network once managers respond
// Key = what the network sends, value = "reg" or "ftd"
const EVENT_MAP = {
  // Generic fallbacks (most networks use these)
  "registration": "reg",
  "signup":       "reg",
  "lead":         "reg",
  "reg":          "reg",
  "ftd":          "ftd",
  "deposit":      "ftd",
  "sale":         "ftd",
  "conversion":   "ftd",
  // Network-specific values added here tomorrow once docs arrive
};

exports.postback = onRequest(
  { secrets: [POSTBACK_TOKEN] },
  async (req, res) => {
    // ── Auth ───────────────────────────────────────────────
    const token = req.query.token || req.body?.token;
    if (token !== POSTBACK_TOKEN.value()) {
      console.warn("Invalid postback token");
      return res.status(401).send("Unauthorized");
    }

    // ── Parse params ───────────────────────────────────────
    const p       = { ...req.query, ...req.body };
    const funnel  = p.funnel  || "";   // e.g. "dark"  — set in the postback URL per network
    const network = p.network || "";   // e.g. "ftdgallery" — set in the postback URL
    const sub1    = p.sub1    || p.clickid || p.aff_sub || p.s1 || "";
    const rawEvent = (p.event || p.type || p.goal || p.status || "").toLowerCase();
    const revenue = parseFloat(p.revenue || p.commission || p.amount || 0) || 0;

    if (!funnel) {
      console.warn("Postback received with no funnel param");
      return res.status(400).send("Missing funnel");
    }

    const eventType = EVENT_MAP[rawEvent] || null; // "reg", "ftd", or null if unknown
    const funnelKey = funnel.charAt(0).toUpperCase() + funnel.slice(1).toLowerCase();
    const date      = new Date().toISOString().split("T")[0]; // YYYY-MM-DD
    const ts        = new Date().toISOString();

    // ── Log raw postback (always) ──────────────────────────
    await db.collection("postbacks").add({
      ts, funnelKey, network,
      sub1, rawEvent, eventType, revenue,
      raw: JSON.stringify(p),
    });

    // ── Update conversions (only for known event types) ────
    if (eventType) {
      const convRef = db
        .collection("conversions")
        .doc(`${funnelKey.toLowerCase()}-${date}`);

      const update = {
        funnel: funnelKey,
        date,
        updatedAt: ts,
      };

      if (eventType === "reg") {
        update.regs = admin.firestore.FieldValue.increment(1);
      } else if (eventType === "ftd") {
        update.ftds    = admin.firestore.FieldValue.increment(1);
        update.revenue = admin.firestore.FieldValue.increment(revenue);
      }

      await convRef.set(update, { merge: true });
      console.log(`Postback processed: ${funnelKey} ${eventType} sub1=${sub1}`);
    } else {
      console.warn(`Unknown event type "${rawEvent}" from network "${network}" — logged but not counted`);
    }

    return res.status(200).send("OK");
  }
);
