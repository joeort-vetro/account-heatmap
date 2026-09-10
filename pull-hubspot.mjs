// Daily HubSpot pull for the VETRO Account Heatmap.
// Wipes and replaces companies/contacts/deals/engagements/engagement_contacts
// with fresh HubSpot data, then writes meta.last_refresh (status ok|failed).
//
// Ported from the Supabase Edge Function version (tools/vetro-account-heatmap
// in the Chief of Staff repo) after that version hit a hard WORKER_RESOURCE_LIMIT
// ceiling partway through the meetings pull, regardless of batch size (tried
// 200/500/1000 contacts per batch -- all plateaued at the same point). Runs as
// a plain Node script on a GitHub Actions runner instead, which has no
// comparable per-invocation CPU/memory ceiling for this data volume.
//
// Account list is NOT hardcoded here -- it's whatever HubSpot's "Strategic"
// (icp_target) company checkbox currently returns. To add/remove an account
// from the heatmap, flip that checkbox in HubSpot; the next run picks it up.
//
// Env vars expected (set as GitHub Actions repo secrets):
//   HUBSPOT_PRIVATE_APP_TOKEN
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY

import { createClient } from "@supabase/supabase-js";

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!HUBSPOT_TOKEN || !SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing required env vars: HUBSPOT_PRIVATE_APP_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

// hs_persona option value -> grid bucket. Management (persona_2) folds into
// Executives per the original artifact's column design (no separate leadership column).
const PERSONA_BUCKET = {
  persona_1: "engineer",
  persona_2: "executives",
  persona_3: "opsTech",
  persona_4: "salesBdMktg",
  persona_5: "executives",
  persona_6: "goToMarket",
  persona_7: "networkArchitect",
};

// Global throttle for ALL HubSpot calls -- concurrency must be bounded across
// the whole run, not per call-site. Running association types in parallel,
// each with its own pool, previously tripped HubSpot's ten_secondly_rolling
// limit when concurrency wasn't globally bounded.
const HS_MAX_CONCURRENT = 5;
let hsActive = 0;
const hsQueue = [];
function hsAcquire() {
  if (hsActive < HS_MAX_CONCURRENT) {
    hsActive++;
    return Promise.resolve();
  }
  return new Promise((resolve) => hsQueue.push(resolve));
}
function hsRelease() {
  hsActive--;
  const next = hsQueue.shift();
  if (next) {
    hsActive++;
    next();
  }
}

async function hs(path, opts = {}, attempt = 0) {
  await hsAcquire();
  let res;
  try {
    res = await fetch(`https://api.hubapi.com${path}`, {
      ...opts,
      headers: {
        "Authorization": `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json",
        ...(opts.headers || {}),
      },
    });
  } finally {
    hsRelease();
  }
  if (res.status === 429 && attempt < 5) {
    // Back off and retry -- defense in depth on top of the concurrency gate,
    // in case the account's actual rolling window is tighter than expected.
    await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
    return hs(path, opts, attempt + 1);
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`HubSpot ${path} -> ${res.status}: ${body.slice(0, 500)}`);
  }
  return res.json();
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Run `fn` over `items` with at most `concurrency` in flight at once --
// balances wall-clock time against HubSpot's per-10s rate limit.
async function mapWithConcurrency(items, concurrency, fn) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  }
  await Promise.all(new Array(Math.min(concurrency, items.length)).fill(0).map(worker));
  return results;
}

async function batchAssociations(fromType, toType, ids) {
  const map = {};
  const batches = chunk(ids, 100);
  const responses = await mapWithConcurrency(batches, 6, (batch) =>
    hs(`/crm/v4/associations/${fromType}/${toType}/batch/read`, {
      method: "POST",
      body: JSON.stringify({ inputs: batch.map((id) => ({ id })) }),
    })
  );
  for (const resp of responses) {
    for (const r of resp.results ?? []) {
      map[r.from.id] = (r.to ?? []).map((t) => t.toObjectId);
    }
  }
  return map;
}

async function batchRead(objectType, ids, properties) {
  const out = {};
  const uniqueIds = [...new Set(ids)];
  const batches = chunk(uniqueIds, 100);
  const responses = await mapWithConcurrency(batches, 6, (batch) =>
    hs(`/crm/v3/objects/${objectType}/batch/read`, {
      method: "POST",
      body: JSON.stringify({ inputs: batch.map((id) => ({ id })), properties }),
    })
  );
  for (const resp of responses) {
    for (const r of resp.results ?? []) out[r.id] = r.properties;
  }
  return out;
}

async function insertAll(table, rows) {
  for (const batch of chunk(rows, 500)) {
    const { error } = await supabase.from(table).insert(batch);
    if (error) throw new Error(`insert into ${table} failed: ${error.message}`);
  }
}

// Upsert with ON CONFLICT DO NOTHING -- safe to call repeatedly with
// overlapping rows across separate contact batches (an engagement with
// contacts split across two batches gets written once, later attempts
// silently skipped) without needing a global in-memory dedup set first.
async function upsertIgnore(table, rows, onConflict) {
  if (rows.length === 0) return;
  for (const batch of chunk(rows, 500)) {
    const { error } = await supabase.from(table).upsert(batch, { onConflict, ignoreDuplicates: true });
    if (error) throw new Error(`upsert into ${table} failed: ${error.message}`);
  }
}

async function main() {
  const startedAt = new Date().toISOString();
  const t0 = Date.now();

  function log(stage) {
    console.log(`[pull-hubspot] ${stage} @ ${Date.now() - t0}ms`);
  }

  async function markStage(stage, extra = {}) {
    log(stage);
    await supabase.from("meta").upsert({
      key: "last_refresh",
      payload: { timestamp: startedAt, status: "in_progress", stage, elapsed_ms: Date.now() - t0, ...extra },
      updated_at: new Date().toISOString(),
    });
  }

  try {
    await markStage("start");

    // 1. Closed-stage ids across ALL deal pipelines -- prior-vs-open is
    // account-wide (any pipeline), not scoped to New Business/Expansion like
    // the sibling deal-matrix build.
    const pipelines = await hs("/crm/v3/pipelines/deals");
    const closedStageIds = new Set();
    for (const pl of pipelines.results) {
      for (const st of pl.stages) {
        if (st.metadata?.isClosed === "true" || st.metadata?.isClosed === true) closedStageIds.add(st.id);
      }
    }
    await markStage("pipelines_loaded");

    // 2. Companies matching Strategic (icp_target = true) -- the live account list.
    const companies = [];
    let after;
    do {
      const body = {
        filterGroups: [{ filters: [{ propertyName: "icp_target", operator: "EQ", value: "true" }] }],
        properties: ["name", "lifecyclestage", "country"],
        limit: 100,
      };
      if (after) body.after = after;
      const page = await hs("/crm/v3/objects/companies/search", { method: "POST", body: JSON.stringify(body) });
      companies.push(...page.results);
      after = page.paging?.next?.after;
    } while (after);
    const companyIds = companies.map((c) => c.id);
    await markStage("companies_loaded", { company_count: companyIds.length });

    // 3. Owners, for resolving engagement rep names.
    const ownersResp = await hs("/crm/v3/owners?limit=200");
    const ownerNames = {};
    for (const o of ownersResp.results ?? []) {
      ownerNames[o.id] = [o.firstName, o.lastName].filter(Boolean).join(" ") || o.email || "Unassigned";
    }

    // Wipe existing data now that we know this run is actually proceeding
    // (children first for FK order).
    await supabase.from("engagement_contacts").delete().not("engagement_id", "is", null);
    await supabase.from("engagements").delete().not("id", "is", null);
    await supabase.from("deals").delete().not("id", "is", null);
    await supabase.from("contacts").delete().not("id", "is", null);
    await supabase.from("companies").delete().not("id", "is", null);

    // 4. Company -> {contacts, deals} associations. Engagement associations
    // (calls/emails/meetings -- by far the largest volume) are deliberately
    // NOT loaded here; they're processed one type at a time below.
    const [companyContacts, companyDeals] = await Promise.all([
      batchAssociations("companies", "contacts", companyIds),
      batchAssociations("companies", "deals", companyIds),
    ]);
    await markStage("associations_loaded", {
      contacts: Object.values(companyContacts).flat().length,
      deals: Object.values(companyDeals).flat().length,
    });

    // Companies can be written now -- doesn't depend on anything below.
    const companyRows = companies.map((c) => ({
      id: c.id,
      name: c.properties.name || "(no name)",
      lifecycle_stage: c.properties.lifecyclestage || null,
      country: c.properties.country || null,
      hubspot_url: `https://app.hubspot.com/contacts/8186371/record/0-2/${c.id}`,
      contact_count: (companyContacts[c.id] || []).length,
      last_refreshed: startedAt,
    }));
    await insertAll("companies", companyRows);
    await markStage("companies_written");

    // 5. Property batch-reads for contacts/deals, in parallel.
    const [contactProps, dealProps] = await Promise.all([
      batchRead("contacts", Object.values(companyContacts).flat(), [
        "firstname", "lastname", "jobtitle", "email", "phone", "lifecyclestage", "hs_persona", "hs_last_sales_activity_timestamp",
      ]),
      batchRead("deals", Object.values(companyDeals).flat(), ["dealname", "pipeline", "dealstage", "amount", "closedate"]),
    ]);
    await markStage("properties_loaded");

    // Contacts can be written now.
    const contactRows = [];
    for (const [companyId, ids] of Object.entries(companyContacts)) {
      for (const cid of ids) {
        const p = contactProps[cid] || {};
        contactRows.push({
          id: cid,
          company_id: companyId,
          name: [p.firstname, p.lastname].filter(Boolean).join(" ") || null,
          title: p.jobtitle || null,
          email: p.email || null,
          phone: p.phone || null,
          lifecycle_stage: p.lifecyclestage || null,
          persona: p.hs_persona || null,
          persona_bucket: p.hs_persona ? (PERSONA_BUCKET[p.hs_persona] || "unclassified") : "unclassified",
          last_activity: p.hs_last_sales_activity_timestamp || null,
          last_refreshed: startedAt,
        });
      }
    }
    await insertAll("contacts", contactRows);
    await markStage("contacts_written", { contact_count: contactRows.length });

    // Deals can be written now.
    const dealRows = [];
    for (const [companyId, ids] of Object.entries(companyDeals)) {
      for (const did of ids) {
        const p = dealProps[did] || {};
        dealRows.push({
          id: did,
          company_id: companyId,
          name: p.dealname || "(no name)",
          pipeline: p.pipeline || null,
          stage: p.dealstage || null,
          amount: p.amount ? Number(p.amount) : 0,
          is_closed: p.dealstage ? closedStageIds.has(p.dealstage) : false,
          close_date: p.closedate || null,
          last_refreshed: startedAt,
        });
      }
    }
    await insertAll("deals", dealRows);
    await markStage("deals_written", { deal_count: dealRows.length });

    // 6. Engagements -- pulled through CONTACTS, not a separate company-level
    // association, so an engagement only counts for an account if it's tied
    // to one of that account's actual known contacts (excludes company-level
    // engagement associations with no linked contact -- bulk/system logging
    // noise). One type at a time (calls, then emails, then meetings) so at
    // most one type's data is held in memory at once.
    const contactCompanies = {};
    for (const [companyId, ids] of Object.entries(companyContacts)) {
      for (const cid of ids) (contactCompanies[cid] ??= []).push(companyId);
    }
    const allContactIds = Object.keys(contactCompanies);

    const engagementProps = {
      calls: ["hs_call_direction", "hs_timestamp", "hubspot_owner_id", "hs_call_title"],
      emails: ["hs_email_direction", "hs_timestamp", "hubspot_owner_id", "hs_email_subject"],
      meetings: ["hs_meeting_title", "hs_timestamp", "hubspot_owner_id"],
    };

    // 12-month lookback -- full history (some emails went back to 2020) was
    // the actual resource driver on the old Edge Function version, not just
    // holding too much in memory at once. Kept here even though the runner
    // has far more headroom, since it's a deliberate product-definition
    // narrowing (old engagements barely matter for a war-room activity view),
    // not purely a performance workaround -- see context/vetro-build-account-heatmap.md.
    const lookbackCutoff = new Date();
    lookbackCutoff.setMonth(lookbackCutoff.getMonth() - 12);

    // Contact batch size of 1000 -- on the Edge Function version, tuning this
    // (200 -> 500 -> 1000) never cleared the resource ceiling; it plateaued
    // at the same ~19.5k emails regardless of batch size. Kept at 1000 here
    // purely because it's a reasonable chunk size for HubSpot's association
    // batch API, not because it matters for this runner's resource limits.
    let totalEngagements = 0;
    for (const type of ["calls", "emails", "meetings"]) {
      for (const contactBatch of chunk(allContactIds, 1000)) {
        const contactToEng = await batchAssociations("contacts", type, contactBatch);
        const batchEngIds = [...new Set(Object.values(contactToEng).flat())];
        if (batchEngIds.length === 0) continue;

        // Lightweight first pass -- timestamp only -- to filter out-of-window
        // ids before paying for the full property fetch (subject lines etc.)
        // on records we're going to discard anyway.
        const tsProps = await batchRead(type, batchEngIds, ["hs_timestamp"]);
        const recentIds = batchEngIds.filter((id) => {
          const ts = tsProps[id]?.hs_timestamp;
          return ts && new Date(ts) >= lookbackCutoff;
        });
        if (recentIds.length === 0) continue;
        const recentIdSet = new Set(recentIds);
        const props = await batchRead(type, recentIds, engagementProps[type]);

        const engagementRows = [];
        const engagementContactRows = [];
        const writtenPairs = new Set(); // dedup WITHIN this batch only
        for (const [contactId, engIds] of Object.entries(contactToEng)) {
          for (const eid of engIds) {
            if (!recentIdSet.has(eid)) continue;
            const pairKey = `${eid}|${contactId}`;
            if (writtenPairs.has(pairKey)) continue;
            writtenPairs.add(pairKey);
            engagementContactRows.push({ engagement_id: eid, contact_id: contactId, contact_name: null });

            const p = props[eid] || {};
            let direction = null;
            if (type === "calls") {
              direction = p.hs_call_direction === "OUTBOUND" ? "outbound" : p.hs_call_direction === "INBOUND" ? "inbound" : null;
            } else if (type === "emails") {
              // Only 'EMAIL' (Outgoing) counts as sent-by-us; INCOMING_EMAIL
              // is received, FORWARDED_EMAIL/DRAFT_EMAIL are neither per the
              // artifact's "sent only" definition.
              direction = p.hs_email_direction === "EMAIL" ? "outbound" : p.hs_email_direction === "INCOMING_EMAIL" ? "inbound" : null;
            }
            for (const companyId of contactCompanies[contactId] || []) {
              engagementRows.push({
                id: eid,
                company_id: companyId,
                type,
                direction,
                occurred_at: p.hs_timestamp || startedAt,
                rep_id: p.hubspot_owner_id || null,
                rep_name: p.hubspot_owner_id ? (ownerNames[p.hubspot_owner_id] || p.hubspot_owner_id) : null,
                detail: p.hs_call_title || p.hs_email_subject || p.hs_meeting_title || null,
                last_refreshed: startedAt,
              });
            }
          }
        }
        await upsertIgnore("engagements", engagementRows, "id,company_id");
        await upsertIgnore("engagement_contacts", engagementContactRows, "engagement_id,contact_id");
      }
      const { count } = await supabase.from("engagements").select("*", { count: "exact", head: true }).eq("type", type);
      totalEngagements += count ?? 0;
      await markStage(`${type}_written`, { [`${type}_written_count`]: count ?? 0 });
    }

    await supabase.from("meta").upsert({
      key: "last_refresh",
      payload: {
        timestamp: startedAt,
        status: "ok",
        elapsed_ms: Date.now() - t0,
        company_count: companyRows.length,
        contact_count: contactRows.length,
        deal_count: dealRows.length,
        engagement_count: totalEngagements,
      },
      updated_at: new Date().toISOString(),
    });

    console.log(JSON.stringify({ ok: true, companies: companyRows.length, contacts: contactRows.length, deals: dealRows.length, engagements: totalEngagements, elapsed_ms: Date.now() - t0 }));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[pull-hubspot] FAILED @ ${Date.now() - t0}ms: ${message}`);
    await supabase.from("meta").upsert({
      key: "last_refresh",
      payload: { timestamp: startedAt, status: "failed", error: message, elapsed_ms: Date.now() - t0 },
      updated_at: new Date().toISOString(),
    });
    process.exit(1);
  }
}

main();
