export const NETWORKS = {
  testnet: { chainId: 72344, name: 'Radius Testnet' },
  mainnet: { chainId: 723487, name: 'Radius Network' },
};

export const OP_TYPES = new Set(['TRANSFER', 'TOKEN_TRANSFER', 'APPROVE', 'CALL', 'OP', 'UNKNOWN']);
export const GRAINS = new Set(['minute', 'hour', 'day']);

const TRANSFER_SELECTOR = '0xa9059cbb';
const APPROVE_SELECTOR = '0x095ea7b3';
const HEX_32 = /^0x[0-9a-f]{64}$/;
const ADDRESS = /^0x[0-9a-f]{40}$/;
const DECIMAL = /^[0-9]+$/;

export function json(data, init = {}) {
  const headers = new Headers(init.headers);
  headers.set('content-type', 'application/json; charset=utf-8');
  return new Response(JSON.stringify(data), { ...init, headers });
}

export function normalizeNetwork(network) {
  const value = String(network || '').toLowerCase();
  if (!NETWORKS[value]) throw httpError(400, `Unsupported network: ${network}`);
  return value;
}

export function normalizeAddress(address, field = 'address') {
  if (address == null || address === '') return null;
  const value = String(address).toLowerCase();
  if (!ADDRESS.test(value)) throw httpError(400, `Invalid ${field}`);
  return value;
}

export function normalizeTxHash(txHash) {
  const value = String(txHash || '').toLowerCase();
  if (!HEX_32.test(value)) throw httpError(400, 'Invalid tx_hash');
  return value;
}

export function normalizeDecimalString(value, field) {
  const normalized = value == null || value === '' ? '0' : String(value);
  if (!DECIMAL.test(normalized)) throw httpError(400, `Invalid ${field}; expected an unsigned decimal string`);
  return stripLeadingZeroes(normalized);
}

export function classifyOperation(event) {
  if (event.op_type) {
    const opType = String(event.op_type).toUpperCase();
    return OP_TYPES.has(opType) ? opType : 'UNKNOWN';
  }

  const input = String(event.input || event.data || '').toLowerCase();
  const valueWei = normalizeDecimalString(event.value_wei ?? event.value ?? '0', 'value_wei');

  if (input.startsWith(TRANSFER_SELECTOR)) return 'TOKEN_TRANSFER';
  if (input.startsWith(APPROVE_SELECTOR)) return 'APPROVE';
  if (valueWei !== '0' && (input === '' || input === '0x')) return 'TRANSFER';
  if (event.to && input.length > 2) return 'CALL';
  return 'UNKNOWN';
}

export function normalizeSuccessfulTransactionEvent(input) {
  if (!input || typeof input !== 'object') throw httpError(400, 'Event must be an object');

  const network = normalizeNetwork(input.network);
  const chainId = Number(input.chain_id ?? NETWORKS[network].chainId);
  if (chainId !== NETWORKS[network].chainId) {
    throw httpError(400, `chain_id ${chainId} does not match ${network}`);
  }

  const status = Number(input.status);
  if (status !== 1) throw httpError(422, 'Only successful receipt events with status = 1 are accepted');

  const timestampMs = Number(input.timestamp_ms);
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) {
    throw httpError(400, 'timestamp_ms must be a positive millisecond timestamp');
  }

  const from = normalizeAddress(input.from ?? input.from_address, 'from');
  if (!from) throw httpError(400, 'from is required');

  const valueWei = normalizeDecimalString(input.value_wei ?? input.value ?? '0', 'value_wei');
  const opType = classifyOperation({ ...input, value_wei: valueWei });
  const asset = normalizeAsset(input.asset, valueWei);

  return {
    network,
    chainId,
    txHash: normalizeTxHash(input.tx_hash ?? input.hash),
    status,
    timestampMs,
    from,
    to: normalizeAddress(input.to ?? input.to_address, 'to'),
    opType,
    valueWei,
    asset,
  };
}

export function normalizeRollupDelta(input) {
  if (!input || typeof input !== 'object') throw httpError(400, 'Rollup delta must be an object');
  const network = normalizeNetwork(input.network);
  const grain = String(input.grain || '').toLowerCase();
  if (!GRAINS.has(grain)) throw httpError(400, 'grain must be minute, hour, or day');
  const bucket = normalizeBucket(grain, input.bucket);
  const opType = input.op_type ? String(input.op_type).toUpperCase() : null;
  if (opType && !OP_TYPES.has(opType)) throw httpError(400, `Unsupported op_type: ${input.op_type}`);

  const successfulTxCount = Number(input.successful_tx_count ?? 0);
  if (!Number.isSafeInteger(successfulTxCount) || successfulTxCount < 0) {
    throw httpError(400, 'successful_tx_count must be a non-negative safe integer');
  }

  const activeAddressEstimate = input.active_address_estimate == null
    ? null
    : Number(input.active_address_estimate);
  if (activeAddressEstimate != null && (!Number.isSafeInteger(activeAddressEstimate) || activeAddressEstimate < 0)) {
    throw httpError(400, 'active_address_estimate must be a non-negative safe integer');
  }

  return {
    network,
    grain,
    bucket,
    opType,
    asset: input.asset ? String(input.asset).toUpperCase() : null,
    successfulTxCount,
    valueWeiSum: normalizeDecimalString(input.value_wei_sum ?? '0', 'value_wei_sum'),
    activeAddressEstimate,
  };
}

export function normalizeProductEvent(input) {
  if (!input || typeof input !== 'object') throw httpError(400, 'Product event must be an object');
  const eventType = String(input.event_type || '').toUpperCase();
  if (!['SIGNUP', 'WALLET_CONNECTED'].includes(eventType)) {
    throw httpError(400, 'event_type must be SIGNUP or WALLET_CONNECTED');
  }

  const occurredAt = input.occurred_at
    ? new Date(input.occurred_at)
    : new Date();
  if (Number.isNaN(occurredAt.getTime())) throw httpError(400, 'occurred_at must be an ISO timestamp');

  return {
    id: input.id ? String(input.id) : crypto.randomUUID(),
    network: input.network ? normalizeNetwork(input.network) : null,
    userId: input.user_id ? String(input.user_id) : null,
    walletAddress: normalizeAddress(input.wallet_address, 'wallet_address'),
    eventType,
    occurredAt: occurredAt.toISOString(),
    metadataJson: input.metadata == null ? null : JSON.stringify(input.metadata),
  };
}

export function bucketForTimestamp(timestampMs, grain) {
  const date = new Date(timestampMs);
  if (Number.isNaN(date.getTime())) throw httpError(400, 'Invalid timestamp');
  date.setUTCSeconds(0, 0);
  if (grain === 'hour') date.setUTCMinutes(0, 0, 0);
  if (grain === 'day') return date.toISOString().slice(0, 10);
  return date.toISOString();
}

export function normalizeBucket(grain, value) {
  if (!value) throw httpError(400, 'bucket is required');
  if (grain === 'day') {
    const day = String(value).slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw httpError(400, 'day bucket must be YYYY-MM-DD');
    return day;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw httpError(400, 'bucket must be an ISO timestamp');
  return bucketForTimestamp(date.getTime(), grain);
}

export function addDecimalStrings(left, right) {
  return (BigInt(normalizeDecimalString(left, 'left')) + BigInt(normalizeDecimalString(right, 'right'))).toString();
}

export function periodBounds(period, endDay = null) {
  const end = endDay ? parseUtcDay(endDay) : startOfUtcDay(new Date());
  let start;
  if (period === 'day') {
    start = new Date(end);
  } else if (period === 'week') {
    start = new Date(end);
    const day = start.getUTCDay() || 7;
    start.setUTCDate(start.getUTCDate() - day + 1);
  } else if (period === 'month') {
    start = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), 1));
  } else {
    throw httpError(400, 'period must be day, week, or month');
  }

  const currentStart = formatDay(start);
  const currentEnd = formatDay(end);
  const days = Math.floor((end.getTime() - start.getTime()) / 86400000) + 1;
  const previousEnd = new Date(start);
  previousEnd.setUTCDate(previousEnd.getUTCDate() - 1);
  const previousStart = new Date(previousEnd);
  previousStart.setUTCDate(previousStart.getUTCDate() - days + 1);

  return {
    currentStart,
    currentEnd,
    previousStart: formatDay(previousStart),
    previousEnd: formatDay(previousEnd),
  };
}

export function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

export function formatDay(date) {
  return date.toISOString().slice(0, 10);
}

function parseUtcDay(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value))) throw httpError(400, 'end must be YYYY-MM-DD');
  return new Date(`${value}T00:00:00.000Z`);
}

function startOfUtcDay(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function stripLeadingZeroes(value) {
  return value.replace(/^0+(?=\d)/, '');
}

function normalizeAsset(asset, valueWei) {
  if (asset) return String(asset).toUpperCase();
  return valueWei === '0' ? null : 'RUSD';
}
