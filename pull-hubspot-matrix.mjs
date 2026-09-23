// Daily HubSpot pull for the VETRO Pipeline Matrix (stage x age grid).
// Wipes and replaces matrix_deals with the current set of open deals across
// the New Business and Expansion pipelines, then writes matrix_meta.last_refresh
// (status ok|failed).
//
// Ported from the on-demand /vetro-matrix-refresh command (Chief of Staff repo,
// .claude/commands/vetro-matrix-refresh.md) after that command's own build spec
// (context/vetro-build-deal-stage-age-matrix.md) concluded the artifact-db
// backing it couldn't support unattended refresh -- a cloud routine can't get
// the HubSpot token into its sandbox without exposing it in prompt/run logs,
// and a headless local scheduled task has no Artifact tool to write with.
// Runs in this repo, alongside pull-hubspot.mjs (the account heatmap's pull),
// against the same Supabase project (VETRO Account Heatmap) and the same
// HUBSPOT_PRIVATE_APP_TOKEN, writing to separate matrix_deals/matrix_meta
// tables so it doesn't collide with the heatmap's own deals/meta tables
// (which track a different dataset: all deals tied to the 233 ICP-target
// companies, vs. this job's open deals across two specific sales pipelines).
//
// Env vars expected (set as GitHub Actions repo secrets, shared with pull-hubspot.mjs):
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

// Pipeline id -> internal name, and stage id -> {label, order}. Frozen per
// context/vetro-build-deal-stage-age-matrix.md (confirmed live against
// HubSpot's pipelines API 2026-09-08). Deal records reference stages by
// internal stageId, not label, so this mapping is required to filter/display.
const PIPELINES = {
  default: {
    name: "new_business",
    stages: {
      "10117277": { label: "Discovery", order: 1 },
      "986329655": { label: "Demo", order: 2 },
      presentationscheduled: { label: "Evaluation", order: 3 },
      "5010136": { label: "Scoping", order: 4 },
      "13172524": { label: "Negotiation", order: 5 },
      "1071396951": { label: "RevOps Review", order: 6 },
    },
  },
  "732680197": {
    name: "expansion",
    stages: {
      "1067337730": { label: "Discovery", order: 1 },
      "1067329312": { label: "Demo", order: 2 },
      "1067329313": { label: "Evaluation", order: 3 },
      "1067329314": { label: "Scoping", order: 4 },
      "1067329315": { label: "Negotiation", order: 5 },
      "1067329347": { label: "RevOps Review", order: 6 },
    },
  },
};
const EXCLUDED_STAGE_IDS = ["closedwon", "closedlost", "13172525", "1067329317", "1067329318"];

const AGE_BUCKETS = [
  [45, "0-45"],
  [90, "46-90"],
  [135, "91-135"],
  [180, "136-180"],
  [225, "181-225"],
  [Infinity, "226+"],
];
function ageBucket(days) {
  for (const [max, label] of AGE_BUCKETS) if (days <= max) return label;
  return "226+";
}
function closeQuarter(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  const q = Math.floor(d.getUTCMonth() / 3) + 1;
  return `${d.getUTCFullYear()}-Q${q}`;
}

async function hs(path, opts = {}, attempt = 0) {
  const res = await fetch(`https://api.hubapi.com${path}`, {
    ...opts,
    headers: {
      Authorization: `Bearer ${HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  if (res.status === 429 && attempt < 5) {
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

async function main() {
  const startedAt = new Date().toISOString();
  const t0 = Date.now();
  function log(stage) {
    console.log(`[pull-hubspot-matrix] ${stage} @ ${Date.now() - t0}ms`);
  }

  try {
    log("start");

    // Pull open deals from both pipelines, excluding closed-type stages.
    const deals = [];
    let after;
    do {
      const body = {
        filterGroups: [
          {
            filters: [
              { propertyName: "pipeline", operator: "IN", values: Object.keys(PIPELINES) },
              { propertyName: "dealstage", operator: "NOT_IN", values: EXCLUDED_STAGE_IDS },
            ],
          },
        ],
        properties: ["dealname", "amount", "dealstage", "pipeline", "createdate", "closedate", "hubspot_owner_id"],
        limit: 100,
      };
      if (after) body.after = after;
      const page = await hs("/crm/v3/objects/deals/search", { method: "POST", body: JSON.stringify(body) });
      deals.push(...page.results);
      after = page.paging?.next?.after;
    } while (after);
    log(`deals_loaded (${deals.length})`);

    const ownersResp = await hs("/crm/v3/owners?limit=200");
    const ownerNames = {};
    for (const o of ownersResp.results ?? []) {
      ownerNames[o.id] = [o.firstName, o.lastName].filter(Boolean).join(" ") || o.email || "Unassigned";
    }

    const now = Date.now();
    const rows = deals
      .map((d) => {
        const p = d.properties;
        const pipelineDef = PIPELINES[p.pipeline];
        const stageDef = pipelineDef?.stages[p.dealstage];
        if (!pipelineDef || !stageDef) return null; // shouldn't happen given the search filter, but guard against drift
        const createDate = p.createdate || null;
        const ageDays = createDate ? Math.floor((now - new Date(createDate).getTime()) / 86400000) : null;
        return {
          hubspot_deal_id: d.id,
          deal_name: p.dealname || "(no name)",
          pipeline: pipelineDef.name,
          pipeline_id: p.pipeline,
          stage_id: p.dealstage,
          stage_label: stageDef.label,
          stage_order: stageDef.order,
          amount: p.amount ? Number(p.amount) : 0,
          close_date: p.closedate || null,
          close_quarter: closeQuarter(p.closedate),
          create_date: createDate,
          age_days: ageDays,
          age_bucket: ageDays == null ? null : ageBucket(ageDays),
          owner_name: p.hubspot_owner_id ? ownerNames[p.hubspot_owner_id] || "Unassigned" : "Unassigned",
          owner_id: p.hubspot_owner_id || null,
          last_refreshed: startedAt,
        };
      })
      .filter(Boolean);

    // Wipe-and-replace: delete everything, then insert the fresh set.
    const { error: delErr } = await supabase.from("matrix_deals").delete().not("hubspot_deal_id", "is", null);
    if (delErr) throw new Error(`delete matrix_deals failed: ${delErr.message}`);
    for (const batch of chunk(rows, 500)) {
      const { error } = await supabase.from("matrix_deals").insert(batch);
      if (error) throw new Error(`insert into matrix_deals failed: ${error.message}`);
    }
    log(`matrix_deals_written (${rows.length})`);

    const pipelinesPulled = Object.keys(PIPELINES);
    const { error: metaErr } = await supabase.from("matrix_meta").upsert({
      key: "last_refresh",
      payload: {
        timestamp: startedAt,
        pipelines_pulled: pipelinesPulled,
        deal_count: rows.length,
        status: "ok",
      },
      updated_at: new Date().toISOString(),
    });
    if (metaErr) throw new Error(`upsert matrix_meta failed: ${metaErr.message}`);

    console.log(JSON.stringify({ ok: true, deals: rows.length, elapsed_ms: Date.now() - t0 }));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[pull-hubspot-matrix] FAILED @ ${Date.now() - t0}ms: ${message}`);
    await supabase.from("matrix_meta").upsert({
      key: "last_refresh",
      payload: { timestamp: startedAt, pipelines_pulled: [], deal_count: 0, status: "failed", error: message },
      updated_at: new Date().toISOString(),
    });
    process.exit(1);
  }
}

main();
