import {
  addDecimalStrings,
  bucketForTimestamp,
  formatDay,
  GRAINS,
  httpError,
  json,
  normalizeNetwork,
  normalizeProductEvent,
  normalizeRollupDelta,
  normalizeSuccessfulTransactionEvent,
  periodBounds,
} from './lib.js';
import { getIndexerStatus, runConfiguredRadiusIndexers, runRadiusIndexer } from './indexer.js';

const ROLLUP_TABLES = {
  minute: { table: 'metric_rollups_minute', bucketColumn: 'bucket_minute' },
  hour: { table: 'metric_rollups_hour', bucketColumn: 'bucket_hour' },
  day: { table: 'metric_rollups_day', bucketColumn: 'bucket_day' },
};

const VALUE_TABLES = {
  minute: { table: 'metric_value_rollups_minute', bucketColumn: 'bucket_minute' },
  hour: { table: 'metric_value_rollups_hour', bucketColumn: 'bucket_hour' },
  day: { table: 'metric_value_rollups_day', bucketColumn: 'bucket_day' },
};

export default {
  async scheduled(controller, env, ctx) {
    ctx.waitUntil((async () => {
      const result = await runConfiguredRadiusIndexers(env, {
        scheduledTime: controller.scheduledTime,
        continueOnError: true,
        strategy: env.INDEXER_SCHEDULED_STRATEGY || env.INDEXER_STRATEGY,
        ingestEvents: (events) => ingestSuccessfulTransactionEvents(events, env),
      });
      console.log('scheduled indexer result', JSON.stringify(result));
    })());
  },

  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      if (request.method === 'OPTIONS') return cors(new Response(null, { status: 204 }));
      if (request.method === 'GET' && url.pathname === '/') return dashboard();
      if (request.method === 'GET' && url.pathname === '/health') return json({ ok: true });

      if (url.pathname === '/api/v1/transactions/successful' && request.method === 'POST') {
        await requireIngestAuth(request, env);
        return cors(await ingestSuccessfulTransactions(request, env));
      }

      if (url.pathname === '/api/v1/rollups' && request.method === 'POST') {
        await requireIngestAuth(request, env);
        return cors(await ingestRollups(request, env));
      }

      if (url.pathname === '/api/v1/product-events' && request.method === 'POST') {
        await requireIngestAuth(request, env);
        return cors(await ingestProductEvents(request, env));
      }

      if (url.pathname === '/api/v1/indexer/run' && request.method === 'POST') {
        await requireIngestAuth(request, env);
        const options = {
          network: url.searchParams.get('network') || undefined,
          strategy: url.searchParams.get('strategy') || undefined,
          maxScanMs: url.searchParams.get('max_scan_ms'),
          initialBackfillMs: url.searchParams.get('initial_backfill_ms'),
          blockBatchSize: url.searchParams.get('block_batch_size'),
          blockBatchConcurrency: url.searchParams.get('block_batch_concurrency'),
          receiptBatchSize: url.searchParams.get('receipt_batch_size'),
          ingestEvents: (events) => ingestSuccessfulTransactionEvents(events, env),
        };
        const result = options.network && options.network !== 'all'
          ? await runRadiusIndexer(env, options)
          : await runConfiguredRadiusIndexers(env, options);
        return cors(json(result));
      }

      if (url.pathname === '/api/v1/indexer/status' && request.method === 'GET') {
        await requireIngestAuth(request, env);
        return cors(json(await getIndexerStatus(env, url.searchParams.get('network') || 'all')));
      }

      if (url.pathname === '/api/v1/metrics/summary' && request.method === 'GET') {
        return cors(await summary(url, env));
      }

      if (url.pathname === '/api/v1/metrics/trend' && request.method === 'GET') {
        return cors(await trend(url, env));
      }

      if (url.pathname === '/api/v1/metrics/operations' && request.method === 'GET') {
        return cors(await operations(url, env));
      }

      if (url.pathname === '/api/v1/metrics/value' && request.method === 'GET') {
        return cors(await valueMoved(url, env));
      }

      if (url.pathname === '/api/v1/metrics/funnel' && request.method === 'GET') {
        return cors(await funnel(url, env));
      }

      return json({ error: 'Not found' }, { status: 404 });
    } catch (error) {
      const status = error.status || 500;
      const message = status >= 500 ? 'Internal server error' : error.message;
      console.error(error);
      return cors(json({ error: message }, { status }));
    }
  },
};

async function ingestSuccessfulTransactions(request, env) {
  const body = await request.json();
  const inputEvents = Array.isArray(body) ? body : body.events || [body];
  return json(await ingestSuccessfulTransactionEvents(inputEvents, env));
}

async function ingestSuccessfulTransactionEvents(inputEvents, env) {
  if (!Array.isArray(inputEvents) || inputEvents.length === 0) throw httpError(400, 'Expected one event or a non-empty events array');
  if (inputEvents.length > 500) throw httpError(413, 'At most 500 events are accepted per request');

  const now = new Date().toISOString();
  let accepted = 0;
  let duplicate = 0;

  for (const input of inputEvents) {
    const event = normalizeSuccessfulTransactionEvent(input);
    const inserted = await insertProcessedTransaction(env, event, now);
    if (!inserted) {
      duplicate += 1;
      continue;
    }

    accepted += 1;
    await env.DB.prepare(
      `insert or ignore into first_successful_transactions
        (network, from_address, tx_hash, first_seen_at)
       values (?, ?, ?, ?)`
    ).bind(event.network, event.from, event.txHash, now).run();

    for (const grain of GRAINS) {
      const bucket = bucketForTimestamp(event.timestampMs, grain);
      const activeDelta = await recordActiveAddress(env, event.network, grain, bucket, event.from, now);
      await incrementMetricRollup(env, grain, event.network, bucket, event.opType, 1, event.valueWei, activeDelta, now);
      if (event.asset && event.valueWei !== '0') {
        await incrementValueRollup(env, grain, event.network, bucket, event.asset, 1, event.valueWei, now);
      }
    }
  }

  return { accepted, duplicate };
}

async function ingestRollups(request, env) {
  const body = await request.json();
  const batchId = body.id || body.batch_id;
  if (!batchId) throw httpError(400, 'batch id is required');
  const source = body.source ? String(body.source) : 'aggregation-pipeline';
  const deltas = Array.isArray(body.deltas) ? body.deltas.map(normalizeRollupDelta) : [];
  if (deltas.length === 0) throw httpError(400, 'deltas must be a non-empty array');
  if (deltas.length > 1000) throw httpError(413, 'At most 1000 deltas are accepted per request');
  if (deltas.some((delta) => !delta.opType && !delta.asset)) {
    throw httpError(400, 'Each delta must include op_type, asset, or both');
  }

  const networkSet = new Set(deltas.map((delta) => delta.network));
  if (networkSet.size !== 1) throw httpError(400, 'A rollup batch may target only one network');
  const network = [...networkSet][0];
  const bucketStart = body.bucket_start ? String(body.bucket_start) : deltas[0].bucket;
  const bucketEnd = body.bucket_end ? String(body.bucket_end) : deltas[deltas.length - 1].bucket;
  const now = new Date().toISOString();

  const batchInsert = await env.DB.prepare(
    `insert or ignore into metric_ingestion_batches
      (id, source, network, bucket_start, bucket_end, ingested_at, notes)
     values (?, ?, ?, ?, ?, ?, ?)`
  ).bind(String(batchId), source, network, bucketStart, bucketEnd, now, body.notes ? String(body.notes) : null).run();

  if (!batchInsert.meta?.changes) return json({ accepted: 0, duplicate_batch: true });

  let accepted = 0;
  for (const delta of deltas) {
    if (delta.opType) {
      await incrementMetricRollup(
        env,
        delta.grain,
        delta.network,
        delta.bucket,
        delta.opType,
        delta.successfulTxCount,
        delta.valueWeiSum,
        delta.activeAddressEstimate || 0,
        now
      );
      accepted += 1;
    }

    if (delta.asset) {
      await incrementValueRollup(
        env,
        delta.grain,
        delta.network,
        delta.bucket,
        delta.asset,
        delta.successfulTxCount,
        delta.valueWeiSum,
        now
      );
      accepted += 1;
    }
  }

  return json({ accepted, duplicate_batch: false });
}

async function ingestProductEvents(request, env) {
  const body = await request.json();
  const inputs = Array.isArray(body) ? body : body.events || [body];
  if (!Array.isArray(inputs) || inputs.length === 0) throw httpError(400, 'Expected one event or a non-empty events array');

  const now = new Date().toISOString();
  let accepted = 0;
  for (const input of inputs) {
    const event = normalizeProductEvent(input);
    const result = await env.DB.prepare(
      `insert or ignore into product_wallet_events
        (id, network, user_id, wallet_address, event_type, occurred_at, metadata_json, inserted_at)
       values (?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      event.id,
      event.network,
      event.userId,
      event.walletAddress,
      event.eventType,
      event.occurredAt,
      event.metadataJson,
      now
    ).run();
    accepted += result.meta?.changes || 0;
  }

  return json({ accepted });
}

async function summary(url, env) {
  const network = normalizeNetwork(url.searchParams.get('network') || 'testnet');
  const period = url.searchParams.get('period') || 'day';
  const bounds = periodBounds(period, url.searchParams.get('end'));

  const row = await env.DB.prepare(
    `select
       coalesce(sum(case when bucket_day between ? and ? then successful_tx_count else 0 end), 0) as current_count,
       coalesce(sum(case when bucket_day between ? and ? then successful_tx_count else 0 end), 0) as previous_count,
       coalesce(sum(case when bucket_day between ? and ? then active_address_estimate else 0 end), 0) as current_active_addresses
     from metric_rollups_day
     where network = ?
       and bucket_day between ? and ?`
  ).bind(
    bounds.currentStart,
    bounds.currentEnd,
    bounds.previousStart,
    bounds.previousEnd,
    bounds.currentStart,
    bounds.currentEnd,
    network,
    bounds.previousStart,
    bounds.currentEnd
  ).first();

  const current = Number(row?.current_count || 0);
  const previous = Number(row?.previous_count || 0);
  const changePct = previous === 0 ? null : ((current - previous) / previous) * 100;

  return json({
    network,
    period,
    ...bounds,
    successful_transactions: current,
    previous_successful_transactions: previous,
    change_pct: changePct,
    active_address_estimate: Number(row?.current_active_addresses || 0),
  });
}

async function trend(url, env) {
  const network = normalizeNetwork(url.searchParams.get('network') || 'testnet');
  const grain = normalizeGrain(url.searchParams.get('grain') || 'day');
  const { table, bucketColumn } = ROLLUP_TABLES[grain];
  const from = normalizeRangeStart(grain, url.searchParams.get('from'));
  const to = normalizeRangeEnd(grain, url.searchParams.get('to'));

  const rows = await env.DB.prepare(
    `select ${bucketColumn} as bucket,
            sum(successful_tx_count) as successful_transactions,
            sum(active_address_estimate) as active_address_estimate
       from ${table}
      where network = ?
        and ${bucketColumn} between ? and ?
      group by ${bucketColumn}
      order by ${bucketColumn}`
  ).bind(network, from, to).all();

  return json({ network, grain, from, to, buckets: rows.results || [] });
}

async function operations(url, env) {
  const network = normalizeNetwork(url.searchParams.get('network') || 'testnet');
  const from = normalizeRangeStart('day', url.searchParams.get('from'));
  const to = normalizeRangeEnd('day', url.searchParams.get('to'));

  const rows = await env.DB.prepare(
    `select op_type,
            sum(successful_tx_count) as successful_transactions,
            sum(active_address_estimate) as active_address_estimate
       from metric_rollups_day
      where network = ?
        and bucket_day between ? and ?
      group by op_type
      order by successful_transactions desc`
  ).bind(network, from, to).all();

  return json({ network, from, to, operations: rows.results || [] });
}

async function valueMoved(url, env) {
  const network = normalizeNetwork(url.searchParams.get('network') || 'testnet');
  const from = normalizeRangeStart('day', url.searchParams.get('from'));
  const to = normalizeRangeEnd('day', url.searchParams.get('to'));

  const rows = await env.DB.prepare(
    `select asset,
            successful_tx_count,
            value_wei_sum
       from metric_value_rollups_day
      where network = ?
        and bucket_day between ? and ?`
  ).bind(network, from, to).all();

  const totalsByAsset = new Map();
  for (const row of rows.results || []) {
    const current = totalsByAsset.get(row.asset) || { asset: row.asset, successful_transactions: 0, value_wei_sum: '0' };
    current.successful_transactions += Number(row.successful_tx_count || 0);
    current.value_wei_sum = addDecimalStrings(current.value_wei_sum, row.value_wei_sum || '0');
    totalsByAsset.set(row.asset, current);
  }

  return json({ network, from, to, assets: [...totalsByAsset.values()] });
}

async function funnel(url, env) {
  const networkParam = url.searchParams.get('network');
  const network = networkParam ? normalizeNetwork(networkParam) : null;
  const from = normalizeRangeStart('day', url.searchParams.get('from'));
  const to = normalizeRangeEnd('day', url.searchParams.get('to'));

  const row = await env.DB.prepare(
    `select
       count(distinct p.wallet_address) as signups,
       count(distinct case when f.from_address is not null then p.wallet_address end) as signups_with_first_successful_tx
     from product_wallet_events p
     left join first_successful_transactions f
       on f.from_address = p.wallet_address
      and (p.network is null or f.network = p.network)
      and (? is null or f.network = ?)
     where p.event_type = 'SIGNUP'
       and substr(p.occurred_at, 1, 10) between ? and ?
       and (? is null or p.network = ?)`
  ).bind(network, network, from, to, network, network).first();

  return json({
    network,
    from,
    to,
    signups: Number(row?.signups || 0),
    signups_with_first_successful_tx: Number(row?.signups_with_first_successful_tx || 0),
  });
}

async function insertProcessedTransaction(env, event, now) {
  const result = await env.DB.prepare(
    `insert or ignore into processed_successful_transactions
      (network, tx_hash, chain_id, event_timestamp_ms, first_seen_at, from_address, to_address, op_type)
     values (?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    event.network,
    event.txHash,
    event.chainId,
    event.timestampMs,
    now,
    event.from,
    event.to,
    event.opType
  ).run();
  return Boolean(result.meta?.changes);
}

async function recordActiveAddress(env, network, grain, bucket, address, now) {
  const result = await env.DB.prepare(
    `insert or ignore into metric_active_addresses
      (network, bucket_grain, bucket_start, address, first_seen_at)
     values (?, ?, ?, ?, ?)`
  ).bind(network, grain, bucket, address, now).run();
  return result.meta?.changes ? 1 : 0;
}

async function incrementMetricRollup(env, grain, network, bucket, opType, count, valueWei, activeDelta, now) {
  const { table, bucketColumn } = ROLLUP_TABLES[grain];
  const existing = await env.DB.prepare(
    `select value_wei_sum from ${table}
      where network = ? and ${bucketColumn} = ? and op_type = ?`
  ).bind(network, bucket, opType).first();

  if (!existing) {
    await env.DB.prepare(
      `insert into ${table}
        (network, ${bucketColumn}, op_type, successful_tx_count, value_wei_sum, active_address_estimate, updated_at)
       values (?, ?, ?, ?, ?, ?, ?)`
    ).bind(network, bucket, opType, count, valueWei, activeDelta, now).run();
    return;
  }

  const nextValue = addDecimalStrings(existing.value_wei_sum || '0', valueWei);
  await env.DB.prepare(
    `update ${table}
        set successful_tx_count = successful_tx_count + ?,
            value_wei_sum = ?,
            active_address_estimate = coalesce(active_address_estimate, 0) + ?,
            updated_at = ?
      where network = ? and ${bucketColumn} = ? and op_type = ?`
  ).bind(count, nextValue, activeDelta, now, network, bucket, opType).run();
}

async function incrementValueRollup(env, grain, network, bucket, asset, count, valueWei, now) {
  const { table, bucketColumn } = VALUE_TABLES[grain];
  const existing = await env.DB.prepare(
    `select value_wei_sum from ${table}
      where network = ? and ${bucketColumn} = ? and asset = ?`
  ).bind(network, bucket, asset).first();

  if (!existing) {
    await env.DB.prepare(
      `insert into ${table}
        (network, ${bucketColumn}, asset, successful_tx_count, value_wei_sum, updated_at)
       values (?, ?, ?, ?, ?, ?)`
    ).bind(network, bucket, asset, count, valueWei, now).run();
    return;
  }

  const nextValue = addDecimalStrings(existing.value_wei_sum || '0', valueWei);
  await env.DB.prepare(
    `update ${table}
        set successful_tx_count = successful_tx_count + ?,
            value_wei_sum = ?,
            updated_at = ?
      where network = ? and ${bucketColumn} = ? and asset = ?`
  ).bind(count, nextValue, now, network, bucket, asset).run();
}

async function requireIngestAuth(request, env) {
  if (!env.INGEST_TOKEN) return;
  const expected = `Bearer ${env.INGEST_TOKEN}`;
  if (request.headers.get('authorization') !== expected) throw httpError(401, 'Unauthorized');
}

function normalizeGrain(grain) {
  const value = String(grain || '').toLowerCase();
  if (!GRAINS.has(value)) throw httpError(400, 'grain must be minute, hour, or day');
  return value;
}

function normalizeRangeStart(grain, value) {
  if (value) {
    if (grain === 'day') return String(value).slice(0, 10);
    return bucketForTimestamp(new Date(value).getTime(), grain);
  }
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - 30);
  return grain === 'day' ? formatDay(date) : bucketForTimestamp(date.getTime(), grain);
}

function normalizeRangeEnd(grain, value) {
  if (value) {
    if (grain === 'day') return String(value).slice(0, 10);
    return bucketForTimestamp(new Date(value).getTime(), grain);
  }
  const date = new Date();
  return grain === 'day' ? formatDay(date) : bucketForTimestamp(date.getTime(), grain);
}

function cors(response) {
  const headers = new Headers(response.headers);
  headers.set('access-control-allow-origin', '*');
  headers.set('access-control-allow-methods', 'GET,POST,OPTIONS');
  headers.set('access-control-allow-headers', 'content-type, authorization');
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function dashboard() {
  return new Response(DASHBOARD_HTML, {
    headers: { 'content-type': 'text/html; charset=utf-8' },
  });
}

const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Radius Transaction Analytics</title>
  <style>
    :root {
      color-scheme: dark;
      --ink: #f5f7fb;
      --muted: #8f9aad;
      --line: #202a3a;
      --surface: #080c12;
      --panel: #0f1622;
      --panel-strong: #141d2b;
      --up: #58d68d;
      --down: #ff6b6b;
      --flat: #aab4c3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at top left, #121a27 0, var(--surface) 420px);
      min-height: 100vh;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 22px 28px;
      border-bottom: 1px solid var(--line);
      background: rgba(8, 12, 18, .82);
      backdrop-filter: blur(14px);
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: 0;
    }
    .subhead {
      margin-top: 5px;
      color: var(--muted);
      font-size: 13px;
    }
    main {
      max-width: 1120px;
      margin: 0 auto;
      padding: 34px 24px;
    }
    .controls {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    select, input, button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--ink);
      padding: 0 10px;
      font: inherit;
    }
    button {
      background: var(--panel-strong);
      color: var(--ink);
      cursor: pointer;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(170px, 1fr));
      gap: 14px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
      min-height: 150px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
    }
    .value {
      margin-top: 16px;
      font-size: 42px;
      line-height: 1;
      font-weight: 700;
      letter-spacing: 0;
    }
    .comparison {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 18px;
      color: var(--muted);
      font-size: 14px;
    }
    .arrow {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 24px;
      height: 24px;
      border-radius: 999px;
      background: var(--panel-strong);
      font-size: 15px;
      line-height: 1;
    }
    .up { color: var(--up); }
    .down { color: var(--down); }
    .flat { color: var(--flat); }
    .status {
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
      min-height: 18px;
    }
    .range {
      margin-bottom: 18px;
      display: grid;
      gap: 4px;
      justify-items: start;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 820px) {
      header { display: block; }
      .controls { margin-top: 14px; }
      .metrics { grid-template-columns: repeat(2, minmax(130px, 1fr)); }
      main { padding: 20px 14px; }
      .value { font-size: 34px; }
    }
    @media (max-width: 520px) {
      .metrics { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Radius Transaction Analytics</h1>
      <div class="subhead">Rolling 7-day activity compared with the prior 7 days</div>
    </div>
    <div class="controls">
      <select id="network" aria-label="Network">
        <option value="testnet">Testnet</option>
        <option value="mainnet">Mainnet</option>
      </select>
      <input id="end" type="date" aria-label="End date">
      <button id="refresh">Refresh</button>
    </div>
  </header>
  <main>
    <div class="range">
      <span id="currentRange">Current range</span>
      <span id="previousRange">Previous range</span>
    </div>
    <section class="metrics">
      <div class="metric">
        <div>
          <div class="label">Successful tx</div>
          <div class="value" id="txValue">0</div>
        </div>
        <div class="comparison"><span class="arrow flat" id="txArrow">→</span><span id="txChange">0.0%</span></div>
      </div>
      <div class="metric">
        <div>
          <div class="label">Active addresses</div>
          <div class="value" id="activeValue">0</div>
        </div>
        <div class="comparison"><span class="arrow flat" id="activeArrow">→</span><span id="activeChange">0.0%</span></div>
      </div>
      <div class="metric">
        <div>
          <div class="label">RUSD moved</div>
          <div class="value" id="valueValue">0</div>
        </div>
        <div class="comparison"><span class="arrow flat" id="valueArrow">→</span><span id="valueChange">0.0%</span></div>
      </div>
      <div class="metric">
        <div>
          <div class="label">Avg tx / day</div>
          <div class="value" id="avgValue">0</div>
        </div>
        <div class="comparison"><span class="arrow flat" id="avgArrow">→</span><span id="avgChange">0.0%</span></div>
      </div>
    </section>
    <div class="status" id="status"></div>
  </main>
  <script>
    const ids = [
      'network', 'end', 'refresh', 'currentRange', 'previousRange', 'status',
      'txValue', 'txArrow', 'txChange',
      'activeValue', 'activeArrow', 'activeChange',
      'valueValue', 'valueArrow', 'valueChange',
      'avgValue', 'avgArrow', 'avgChange'
    ];
    const el = Object.fromEntries(ids.map((id) => [id, document.getElementById(id)]));
    el.end.valueAsDate = new Date();
    el.refresh.addEventListener('click', load);
    el.network.addEventListener('change', load);
    el.end.addEventListener('change', load);

    async function get(path) {
      const response = await fetch(path);
      if (!response.ok) throw new Error(await response.text());
      return response.json();
    }

    async function load() {
      el.status.textContent = 'Loading';
      const network = el.network.value;
      const ranges = buildRanges(el.end.value);
      el.currentRange.textContent = 'Current: ' + ranges.currentStart + ' to ' + ranges.currentEnd;
      el.previousRange.textContent = 'Previous: ' + ranges.previousStart + ' to ' + ranges.previousEnd;

      const currentTrend = await get('/api/v1/metrics/trend?network=' + network + '&grain=day&from=' + ranges.currentStart + '&to=' + ranges.currentEnd);
      const previousTrend = await get('/api/v1/metrics/trend?network=' + network + '&grain=day&from=' + ranges.previousStart + '&to=' + ranges.previousEnd);
      const currentValue = await get('/api/v1/metrics/value?network=' + network + '&from=' + ranges.currentStart + '&to=' + ranges.currentEnd);
      const previousValue = await get('/api/v1/metrics/value?network=' + network + '&from=' + ranges.previousStart + '&to=' + ranges.previousEnd);

      const current = summarize(currentTrend, currentValue);
      const previous = summarize(previousTrend, previousValue);

      renderMetric('tx', current.tx, previous.tx, formatInteger);
      renderMetric('active', current.active, previous.active, formatInteger);
      renderMetric('value', current.valueWei, previous.valueWei, formatWei);
      renderMetric('avg', current.tx / 7, previous.tx / 7, formatDecimal);
      el.status.textContent = 'Updated ' + new Date().toLocaleTimeString();
    }

    function buildRanges(value) {
      const end = value ? parseDay(value) : startOfUtcDay(new Date());
      const currentStart = addDays(end, -6);
      const previousEnd = addDays(currentStart, -1);
      const previousStart = addDays(previousEnd, -6);
      return {
        currentStart: dayString(currentStart),
        currentEnd: dayString(end),
        previousStart: dayString(previousStart),
        previousEnd: dayString(previousEnd)
      };
    }

    function summarize(trend, value) {
      const buckets = trend.buckets || [];
      const assets = value.assets || [];
      return {
        tx: buckets.reduce((sum, row) => sum + Number(row.successful_transactions || 0), 0),
        active: buckets.reduce((sum, row) => sum + Number(row.active_address_estimate || 0), 0),
        valueWei: assets.reduce((sum, row) => sum + BigInt(row.value_wei_sum || '0'), 0n)
      };
    }

    function renderMetric(prefix, current, previous, formatter) {
      const change = percentChange(current, previous);
      el[prefix + 'Value'].textContent = formatter(current);
      el[prefix + 'Change'].textContent = change == null ? 'n/a' : formatSigned(change) + '%';
      setArrow(el[prefix + 'Arrow'], change);
    }

    function percentChange(current, previous) {
      const currentNumber = typeof current === 'bigint' ? Number(current) : Number(current);
      const previousNumber = typeof previous === 'bigint' ? Number(previous) : Number(previous);
      if (previousNumber === 0) return currentNumber === 0 ? 0 : null;
      return ((currentNumber - previousNumber) / previousNumber) * 100;
    }

    function setArrow(node, change) {
      node.className = 'arrow flat';
      node.textContent = '→';
      if (change == null || change === 0) return;
      if (change > 0) {
        node.className = 'arrow up';
        node.textContent = '↑';
      } else {
        node.className = 'arrow down';
        node.textContent = '↓';
      }
    }

    function formatSigned(value) {
      return (value > 0 ? '+' : '') + value.toFixed(1);
    }

    function formatInteger(value) {
      return Math.round(Number(value)).toLocaleString();
    }

    function formatDecimal(value) {
      return Number(value).toLocaleString(undefined, { maximumFractionDigits: 1 });
    }

    function formatWei(value) {
      const wei = BigInt(value || 0);
      const divisor = 1000000000000000000n;
      const whole = wei / divisor;
      const decimal = (wei % divisor) / 1000000000000000n;
      const amount = Number(whole) + Number(decimal) / 1000;
      if (amount >= 1000000) return (amount / 1000000).toFixed(1) + 'M';
      if (amount >= 1000) return (amount / 1000).toFixed(1) + 'K';
      return amount.toLocaleString(undefined, { maximumFractionDigits: 3 });
    }

    function parseDay(value) {
      return new Date(value + 'T00:00:00.000Z');
    }

    function startOfUtcDay(date) {
      return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
    }

    function addDays(date, days) {
      const next = new Date(date);
      next.setUTCDate(next.getUTCDate() + days);
      return next;
    }

    function dayString(date) {
      return date.toISOString().slice(0, 10);
    }

    load().catch((error) => {
      el.status.textContent = error.message;
    });
  </script>
</body>
</html>`;
