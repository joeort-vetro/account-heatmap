// Daily HubSpot pull for the VETRO Bookings by State page (bookings.html).
// Wipes and replaces bookings_deals with every closed-won bookings deal, tagged
// with the associated company's state, country and market, then writes
// bookings_meta.last_refresh (status ok|failed).
//
// Built for the CEO's 2026-11-13 ask: historical bookings by state, then by market,
// by market, as the "reality" half of 2027 regional quota setting.
//
// Bookings definitions (one row per deal):
//   new_logo  = New Business Pipe (default), stage Closed won, `amount`.
//   expansion = Expansion Pipe (732680197) Closed Won with dealtype Upsell,
//               `amount`; plus Renewal Pipe (5452187) Closed won with dealtype
//               Mid-Contract or Upsell, `upsell_downsell_recurring_revenue_amount`
//               (net recurring change). Before 2025 VETRO booked mid-contract
//               upsells in the Renewal Pipe; the Expansion Pipe starts in 2025.
//   Excluded: deals with amount <= 0 (mid-contract downgrades), Price Increase deals (contractual escalators, not sales output),
//             renewals, Non-Recurring Only and Partner pipes, deals with no
//             close date, and closed-won deals dated in the future (pre-booked
//             multi-year escalator years).
//
// Geography = the deal's primary associated company's State/Region (`state`),
// not service footprint: a multi-state operator's whole deal lands in the state
// on its company record. No fallback fields: blank or non-US State/Region stays unmapped.
//
// Env vars (GitHub Actions repo secrets, shared with the other pulls):
//   HUBSPOT_PRIVATE_APP_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from "@supabase/supabase-js";

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
// DRY_RUN=<path>: pull from HubSpot and write rows to that JSON file; no Supabase writes.
const DRY_RUN = process.env.DRY_RUN;
if (!HUBSPOT_TOKEN || (!DRY_RUN && (!SUPABASE_URL || !SERVICE_ROLE_KEY))) {
  console.error("Missing required env vars: HUBSPOT_PRIVATE_APP_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const supabase = DRY_RUN ? null : createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

const NB_PIPE = "default", NB_WON = "closedwon";
const EXP_PIPE = "732680197", EXP_WON = "1067329317";
const REN_PIPE = "5452187", REN_WON = "5452193";

// Market = the 2027 "Updated Sub-Vertical" segment model (Michaela's TAM build).
// Fallback: legacy HubSpot sub-vertical crosswalked per the TAM file's
// "Sub-Vertical Crosswalk" tab. segment_overrides (Supabase, loaded from the
// TAM file) holds the companies where the file's segment differs from that
// crosswalk (mostly ISP re-tiering by annual revenue) and wins when present.
// When the new segments are imported into HubSpot, read that property instead.
function fallbackSegment(sv) {
  sv = (sv || "").trim();
  if (!sv) return "Not Mapped";
  if (["Tier 1", "ISP Tier 1"].includes(sv)) return "ISP Tier 1";
  if (["Tier 2", "Tier 3", "ISP Tier 2", "ISP Tier 3"].includes(sv)) return "ISP Tier 2/3";
  if (["Tier 4", "ISP Tier 4"].includes(sv)) return "ISP Tier 4";
  if (["Coop ISP", "Small Coop", "Medium Coop", "Large Coop"].includes(sv)) return "Co-op";
  if (["Middle Mile/Transport", "Dark Fiber/Middle Mile/Transport", "Dark Fiber", "OAN"].includes(sv)) return "Core infrastructure";
  if (["Local Government - Small", "Local Government - Large", "Other State Government", "DOT"].includes(sv)) return "Government";
  if (sv.startsWith("Eng - ") || ["PM/Consultant", "EPC", "Engineering Firm"].includes(sv)) return "Engineering Firm";
  return "Other";
}

const US = {
  AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California", CO: "Colorado", CT: "Connecticut",
  DE: "Delaware", DC: "District of Columbia", FL: "Florida", GA: "Georgia", HI: "Hawaii", ID: "Idaho", IL: "Illinois",
  IN: "Indiana", IA: "Iowa", KS: "Kansas", KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland",
  MA: "Massachusetts", MI: "Michigan", MN: "Minnesota", MS: "Mississippi", MO: "Missouri", MT: "Montana",
  NE: "Nebraska", NV: "Nevada", NH: "New Hampshire", NJ: "New Jersey", NM: "New Mexico", NY: "New York",
  NC: "North Carolina", ND: "North Dakota", OH: "Ohio", OK: "Oklahoma", OR: "Oregon", PA: "Pennsylvania",
  RI: "Rhode Island", SC: "South Carolina", SD: "South Dakota", TN: "Tennessee", TX: "Texas", UT: "Utah",
  VT: "Vermont", VA: "Virginia", WA: "Washington", WV: "West Virginia", WI: "Wisconsin", WY: "Wyoming",
  PR: "Puerto Rico", GU: "Guam", VI: "U.S. Virgin Islands",
};
const NAME_TO_CODE = Object.fromEntries(Object.entries(US).map(([k, v]) => [v.toLowerCase(), k]));
function stateCode(c) {
  // State/Region (`state`) only. No fallback: a blank stays unmapped.
  for (const raw of [c.state]) {
    const s = (raw || "").trim();
    if (!s) continue;
    if (US[s.toUpperCase()]) return s.toUpperCase();
    if (NAME_TO_CODE[s.toLowerCase()]) return NAME_TO_CODE[s.toLowerCase()];
  }
  return null;
}
function isUS(c, code) {
  const country = (c.country || "").trim().toLowerCase();
  if (["united states", "usa", "us", "u.s.", "united states of america", "puerto rico"].includes(country)) return true;
  return !country && !!code;
}

async function hs(path, opts = {}, attempt = 0) {
  const res = await fetch(`https://api.hubapi.com${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}`, "Content-Type": "application/json", ...(opts.headers || {}) },
  });
  if (res.status === 429 && attempt < 6) {
    await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
    return hs(path, opts, attempt + 1);
  }
  if (!res.ok) throw new Error(`HubSpot ${path} -> ${res.status}: ${(await res.text()).slice(0, 500)}`);
  return res.json();
}
const chunk = (a, n) => Array.from({ length: Math.ceil(a.length / n) }, (_, i) => a.slice(i * n, i * n + n));

async function searchDeals(pipeline, stage) {
  const out = [];
  let after;
  do {
    const body = {
      filterGroups: [{ filters: [
        { propertyName: "pipeline", operator: "EQ", value: pipeline },
        { propertyName: "dealstage", operator: "EQ", value: stage },
      ] }],
      properties: ["dealname", "pipeline", "dealstage", "dealtype", "amount", "upsell_downsell_recurring_revenue_amount", "closedate", "hubspot_owner_id"],
      sorts: [{ propertyName: "hs_object_id", direction: "ASCENDING" }],
      limit: 200,
    };
    if (after) body.after = after;
    const page = await hs("/crm/v3/objects/deals/search", { method: "POST", body: JSON.stringify(body) });
    out.push(...page.results);
    after = page.paging?.next?.after;
    if (out.length >= 9800 && after) throw new Error(`search cap reached for ${pipeline}/${stage}; switch to date-windowed paging`);
  } while (after);
  return out;
}

async function main() {
  const startedAt = new Date().toISOString();
  const t0 = Date.now();
  const log = (s) => console.log(`[pull-hubspot-bookings] ${s} @ ${Date.now() - t0}ms`);
  try {
    log("start");
    const [nb, exp, ren] = await Promise.all([
      searchDeals(NB_PIPE, NB_WON), searchDeals(EXP_PIPE, EXP_WON), searchDeals(REN_PIPE, REN_WON),
    ]);
    log(`deals nb=${nb.length} exp=${exp.length} ren=${ren.length}`);

    const num = (v) => (v === null || v === undefined || v === "" ? 0 : Number(v) || 0);
    const candidates = [];
    for (const d of nb) candidates.push({ d, motion: "new_logo", amount: num(d.properties.amount) });
    for (const d of exp) if (d.properties.dealtype === "Upsell") candidates.push({ d, motion: "expansion", amount: num(d.properties.amount) });
    for (const d of ren) if (["Mid-Contract", "Upsell"].includes(d.properties.dealtype))
      candidates.push({ d, motion: "expansion", amount: num(d.properties.upsell_downsell_recurring_revenue_amount) });

    const now = Date.now();
    const kept = candidates.filter(({ d, amount }) => {
      const cd = d.properties.closedate;
      return cd && new Date(cd).getTime() <= now && amount > 0;
    });

    // Primary company per deal (association typeId 5 = deal_to_company primary).
    const companyOf = {};
    for (const ids of chunk(kept.map((k) => k.d.id), 1000)) {
      const r = await hs("/crm/v4/associations/deals/companies/batch/read", {
        method: "POST", body: JSON.stringify({ inputs: ids.map((id) => ({ id })) }),
      });
      for (const res of r.results ?? []) {
        const tos = res.to ?? [];
        const primary = tos.find((t) => (t.associationTypes ?? []).some((a) => a.typeId === 5)) || tos[0];
        if (primary) companyOf[String(res.from.id)] = String(primary.toObjectId);
      }
    }
    const cids = [...new Set(Object.values(companyOf))];
    const companies = {};
    for (const ids of chunk(cids, 100)) {
      const r = await hs("/crm/v3/objects/companies/batch/read", {
        method: "POST",
        body: JSON.stringify({
          inputs: ids.map((id) => ({ id })),
          properties: ["name", "state", "state__u_s__", "hs_state_code", "country", "sub_vertical__proposed_"],
        }),
      });
      for (const c of r.results ?? []) companies[c.id] = c.properties;
    }
    log(`companies_loaded (${cids.length})`);

    const owners = {}, ownerActive = {};
    for (const archived of [false, true]) {
      const resp = await hs(`/crm/v3/owners?limit=500&archived=${archived}`);
      for (const o of resp.results ?? []) {
        owners[o.id] = [o.firstName, o.lastName].filter(Boolean).join(" ") || "Unassigned";
        ownerActive[o.id] = !archived;
      }
    }

    const overrides = {};
    if (!DRY_RUN) {
      const { data, error } = await supabase.from("segment_overrides").select("company_id,segment").limit(20000);
      if (error) throw new Error(`read segment_overrides failed: ${error.message}`);
      for (const r of data ?? []) overrides[r.company_id] = r.segment;
    } else if (process.env.OVERRIDES_JSON) {
      const fs = await import("node:fs");
      Object.assign(overrides, JSON.parse(fs.readFileSync(process.env.OVERRIDES_JSON, "utf8")));
    }

    const rows = kept.map(({ d, motion, amount }) => {
      const p = d.properties;
      const cid = companyOf[d.id] || null;
      const c = (cid && companies[cid]) || {};
      const code = stateCode(c);
      const us = isUS(c, code);
      return {
        hubspot_deal_id: d.id,
        deal_name: p.dealname || "(no name)",
        company_id: cid,
        company_name: c.name || null,
        motion,
        pipeline_id: p.pipeline,
        deal_type: p.dealtype || null,
        amount,
        close_date: p.closedate,
        close_year: new Date(p.closedate).getUTCFullYear(),
        is_us: us,
        state_code: us ? code : null,
        country: (c.country || "").trim() || (us ? "United States" : null),
        market: (cid && overrides[cid]) || fallbackSegment(c.sub_vertical__proposed_),
        sub_vertical: c.sub_vertical__proposed_ || null,
        owner_name: p.hubspot_owner_id ? owners[p.hubspot_owner_id] || "Unassigned" : "Unassigned",
        owner_active: p.hubspot_owner_id ? ownerActive[p.hubspot_owner_id] ?? null : null,
        last_refreshed: startedAt,
      };
    });

    if (DRY_RUN) {
      const fs = await import("node:fs");
      fs.writeFileSync(DRY_RUN, JSON.stringify(rows));
      console.log(JSON.stringify({ dry_run: DRY_RUN, deals: rows.length }));
      return;
    }
    const { error: delErr } = await supabase.from("bookings_deals").delete().not("hubspot_deal_id", "is", null);
    if (delErr) throw new Error(`delete bookings_deals failed: ${delErr.message}`);
    for (const batch of chunk(rows, 500)) {
      const { error } = await supabase.from("bookings_deals").insert(batch);
      if (error) throw new Error(`insert bookings_deals failed: ${error.message}`);
    }
    log(`bookings_deals_written (${rows.length})`);

    const { error: metaErr } = await supabase.from("bookings_meta").upsert({
      key: "last_refresh",
      payload: {
        timestamp: startedAt, status: "ok", deal_count: rows.length,
        no_company: rows.filter((r) => !r.company_id).length,
        us_no_state: rows.filter((r) => r.is_us && !r.state_code).length,
      },
      updated_at: new Date().toISOString(),
    });
    if (metaErr) throw new Error(`upsert bookings_meta failed: ${metaErr.message}`);
    console.log(JSON.stringify({ ok: true, deals: rows.length, elapsed_ms: Date.now() - t0 }));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[pull-hubspot-bookings] FAILED: ${message}`);
    if (DRY_RUN) process.exit(1);
    await supabase.from("bookings_meta").upsert({
      key: "last_refresh",
      payload: { timestamp: startedAt, status: "failed", deal_count: 0, error: message },
      updated_at: new Date().toISOString(),
    });
    process.exit(1);
  }
}

main();
