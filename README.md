# Radius Analytics DB

Cloudflare Workers + D1 service for Radius transaction growth analytics.

The north-star metric is successful Radius transactions per day, week, and month. A transaction is counted only when a Radius receipt reports `status = 1`; RPC request volume, retries, failed receipts, and mempool attempts are excluded.

## Architecture

This repo keeps D1 as a product-facing rollup database:

- successful transaction event ingestion for the Phase 1 testnet MVP;
- idempotent counting by `network + tx_hash`;
- minute, hour, and day rollups by network and operation type;
- value rollups by network, bucket, and asset;
- active-address estimates using exact D1 helper rows for MVP scale;
- a pre-aggregated rollup ingestion endpoint for a high-throughput pipeline;
- a scheduled Radius JSON-RPC indexer for testnet-scale activity;
- dashboard APIs and a small dashboard at `/`.

Raw successful transaction facts should still live outside D1 at production scale, for example R2 Parquet/Iceberg, ClickHouse, BigQuery, Snowflake, Kafka, or Redpanda.

## Setup

Install dependencies:

```bash
npm install
```

Create a D1 database and replace the placeholder IDs in `wrangler.toml`:

```bash
npx wrangler d1 create radius-analytics
```

Apply migrations locally:

```bash
npm run db:migrate:local
```

Run the Worker:

```bash
npm run dev
```

Run the Worker with Wrangler's local scheduled-event test route enabled:

```bash
npm run dev:scheduled
```

For remote deployments, set an ingest token and apply migrations:

```bash
npx wrangler secret put INGEST_TOKEN
npm run db:migrate:remote
npm run deploy
```

## Ingest Successful Transactions

`POST /api/v1/transactions/successful`

Authorization is required only when `INGEST_TOKEN` is configured:

```http
Authorization: Bearer <INGEST_TOKEN>
```

Example:

```json
{
  "network": "testnet",
  "chain_id": 72344,
  "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
  "status": 1,
  "timestamp_ms": 1779148800000,
  "from": "0x2222222222222222222222222222222222222222",
  "to": "0x3333333333333333333333333333333333333333",
  "input": "0x",
  "value_wei": "1000000000000000000",
  "asset": "RUSD"
}
```

The endpoint accepts either one event or `{ "events": [...] }`. Duplicate `network + tx_hash` pairs are ignored.

Operation type can be supplied as `op_type`; otherwise the service classifies coarse types:

- `TRANSFER`: native value transfer;
- `TOKEN_TRANSFER`: ERC-20 `transfer(address,uint256)`;
- `APPROVE`: ERC-20 `approve(address,uint256)`;
- `CALL`: contract call not otherwise classified;
- `UNKNOWN`: unclassified.

## Scheduled Radius Indexer

The Worker has a cron trigger that runs every minute and scans Radius reconstructed blocks from the testnet RPC. Radius block numbers are millisecond timestamps, so the indexer stores a D1 cursor and advances through a bounded time window each run.

Defaults in `wrangler.toml` are tuned for testnet activity around 2-3 TPS:

- `RADIUS_TESTNET_RPC_URL=https://rpc.testnet.radiustech.xyz`
- `RADIUS_MAINNET_RPC_URL=https://rpc.radiustech.xyz`
- `INDEXER_NETWORKS=testnet,mainnet`
- `INDEXER_STRATEGY=logs`
- `INDEXER_MAX_SCAN_MS=60000`
- `INDEXER_BLOCK_BATCH_SIZE=3000`
- `INDEXER_BLOCK_BATCH_CONCURRENCY=8`
- `INDEXER_FINALITY_LAG_MS=2000`
- `INDEXER_WORKER` service binding to `radius-analytics-db`

On the Cloudflare Free plan, scheduled events have a small CPU budget. The
deployed cron handler uses the low-cost `logs` strategy against the SBC token
contract. Use `strategy=blocks` on the manual endpoint for slower reconciliation
scans that probe reconstructed timestamp blocks directly.

Manual controls:

- `POST /api/v1/indexer/run?network=testnet|mainnet|all` runs one scan window immediately.
- `GET /api/v1/indexer/status?network=testnet|mainnet|all` returns stored cursors and last run stats.

Authorization follows the ingest endpoints: when `INGEST_TOKEN` is configured, send `Authorization: Bearer <INGEST_TOKEN>`.

### Local testing

Wrangler does not automatically fire cron schedules during local development. Use one of these paths instead:

```bash
npm run dev:scheduled
```

Then, in another terminal, trigger the scheduled handler:

```bash
npm run indexer:scheduled:local
```

To bypass the scheduled-event wrapper and run the indexer directly over HTTP:

```bash
npm run indexer:run:local
```

The default local script scans a 5-second window so it returns quickly. To test the full configured scan window:

```bash
npm run indexer:run:local:full
```

## Ingest Aggregated Rollups

`POST /api/v1/rollups`

This is the production-oriented path for a high-throughput aggregation pipeline. The batch `id` is idempotent.

```json
{
  "id": "testnet-2026-05-19T08:00Z",
  "source": "radius-event-stream-aggregator",
  "bucket_start": "2026-05-19",
  "bucket_end": "2026-05-19",
  "deltas": [
    {
      "network": "testnet",
      "grain": "day",
      "bucket": "2026-05-19",
      "op_type": "TRANSFER",
      "successful_tx_count": 1250,
      "value_wei_sum": "890000000000000000000",
      "active_address_estimate": 410
    },
    {
      "network": "testnet",
      "grain": "day",
      "bucket": "2026-05-19",
      "asset": "RUSD",
      "successful_tx_count": 1000,
      "value_wei_sum": "890000000000000000000"
    }
  ]
}
```

## Product Events

`POST /api/v1/product-events`

Use this to support signup-to-first-transaction reporting:

```json
{
  "event_type": "SIGNUP",
  "network": "testnet",
  "user_id": "user_123",
  "wallet_address": "0x2222222222222222222222222222222222222222",
  "occurred_at": "2026-05-19T08:00:00.000Z"
}
```

## API

- `GET /api/v1/metrics/summary?network=testnet&period=day|week|month&end=2026-05-19`
- `GET /api/v1/metrics/trend?network=testnet&grain=day&from=2026-05-01&to=2026-05-19`
- `GET /api/v1/metrics/operations?network=testnet&from=2026-05-01&to=2026-05-19`
- `GET /api/v1/metrics/value?network=testnet&from=2026-05-01&to=2026-05-19`
- `GET /api/v1/metrics/funnel?network=testnet&from=2026-05-01&to=2026-05-19`

## Notes

Radius has sub-second finality and no Ethereum-style reorg window, so the indexer does not wait for multiple confirmations. Receipt `status = 1` remains the source of truth.

D1 stores decimal wei sums as text to avoid 64-bit integer overflow. The service updates those sums in application code. At production throughput, feed D1 pre-aggregated deltas and keep exact raw facts in the analytical store.

---

> ⚠️ **Demo code — not production-ready.** Provided as-is, without warranty, and may contain known, unpatched vulnerabilities (including in dependencies). If you reuse it, run your own security and supply-chain scans and patch before deploying.
