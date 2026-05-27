import { NETWORKS, normalizeNetwork } from './lib.js';

const DEFAULT_RPC_URLS = {
  testnet: 'https://rpc.testnet.radiustech.xyz',
  mainnet: 'https://rpc.radiustech.xyz',
};

const DEFAULT_FINALITY_LAG_MS = 2000;
const DEFAULT_INITIAL_BACKFILL_MS = 60000;
const DEFAULT_MAX_SCAN_MS = 60000;
const DEFAULT_BLOCK_BATCH_SIZE = 3000;
const DEFAULT_BLOCK_BATCH_CONCURRENCY = 6;
const DEFAULT_RECEIPT_BATCH_SIZE = 200;
const SBC_ADDRESSES = {
  testnet: '0x33ad9e4bd16b69b5bfded37d8b5d9ff9aba014fb',
  mainnet: '0x33ad9e4bd16b69b5bfded37d8b5d9ff9aba014fb',
};

export async function runConfiguredRadiusIndexers(env, options = {}) {
  const networks = options.network
    ? normalizeNetworkList(options.network)
    : configuredNetworks(env);
  const results = [];

  for (const network of networks) {
    try {
      results.push(await runRadiusIndexer(env, { ...options, network }));
    } catch (error) {
      if (!options.continueOnError) throw error;
      results.push({
        network,
        error: error.message || String(error),
      });
    }
  }

  return { networks: results };
}

export async function runRadiusIndexer(env, options = {}) {
  const network = normalizeNetwork(options.network || env.INDEXER_NETWORK || 'testnet');
  const enabled = String(env.INDEXER_ENABLED ?? 'true').toLowerCase() !== 'false';
  if (!enabled) return { skipped: true, reason: 'indexer disabled', network };
  if (typeof options.ingestEvents !== 'function') throw new Error('ingestEvents callback is required');

  const chainId = NETWORKS[network].chainId;
  const rpcUrl = options.rpcUrl || rpcUrlForNetwork(env, network);
  const now = new Date().toISOString();

  try {
    const latest = await rpcCall(rpcUrl, 'eth_blockNumber', []);
    const latestBlock = hexQuantityToBigInt(latest, 'eth_blockNumber');
    const finalityLagMs = envInt(env.INDEXER_FINALITY_LAG_MS, DEFAULT_FINALITY_LAG_MS);
    const stableTo = latestBlock - BigInt(finalityLagMs);
    if (stableTo < 0n) throw new Error('RPC returned an invalid latest block');

    const cursor = await getIndexerCursor(env, network);
    const initialBackfillMs = optionInt(options.initialBackfillMs, env.INDEXER_INITIAL_BACKFILL_MS, DEFAULT_INITIAL_BACKFILL_MS);
    const maxScanMs = optionInt(options.maxScanMs, env.INDEXER_MAX_SCAN_MS, DEFAULT_MAX_SCAN_MS);
    const from = cursor
      ? BigInt(cursor.last_scanned_block) + 1n
      : maxBigInt(0n, stableTo - BigInt(initialBackfillMs) + 1n);

    if (from > stableTo) {
      await saveIndexerCursor(env, {
        network,
        chainId,
        lastScannedBlock: cursor?.last_scanned_block || stableTo.toString(),
        now,
        blocksScanned: 0,
        transactionsSeen: 0,
        successfulTransactions: 0,
        error: null,
      });
      return {
        network,
        chain_id: chainId,
        up_to_date: true,
        latest_block: latestBlock.toString(),
        stable_to: stableTo.toString(),
        last_scanned_block: cursor?.last_scanned_block || stableTo.toString(),
        blocks_scanned: 0,
        transactions_seen: 0,
        successful_transactions: 0,
        accepted: 0,
        duplicate: 0,
      };
    }

    const to = minBigInt(stableTo, from + BigInt(maxScanMs) - 1n);
    const scanOptions = {
      network,
      chainId,
      from,
      to,
      blockBatchSize: optionInt(options.blockBatchSize, env.INDEXER_BLOCK_BATCH_SIZE, DEFAULT_BLOCK_BATCH_SIZE),
      blockBatchConcurrency: optionInt(options.blockBatchConcurrency, env.INDEXER_BLOCK_BATCH_CONCURRENCY, DEFAULT_BLOCK_BATCH_CONCURRENCY),
      receiptBatchSize: optionInt(options.receiptBatchSize, env.INDEXER_RECEIPT_BATCH_SIZE, DEFAULT_RECEIPT_BATCH_SIZE),
    };
    const strategy = String(options.strategy || env.INDEXER_STRATEGY || 'blocks').toLowerCase();
    const scan = strategy === 'logs'
      ? await scanRadiusTokenLogs(rpcUrl, scanOptions)
      : await scanRadiusBlocks(rpcUrl, scanOptions);

    const ingestion = await ingestEventChunks(scan.events, options.ingestEvents);

    await saveIndexerCursor(env, {
      network,
      chainId,
      lastScannedBlock: to.toString(),
      now,
      blocksScanned: scan.blocksScanned,
      transactionsSeen: scan.transactionsSeen,
      successfulTransactions: scan.events.length,
      error: null,
    });

    return {
      network,
      chain_id: chainId,
      from_block: from.toString(),
      to_block: to.toString(),
      latest_block: latestBlock.toString(),
      stable_to: stableTo.toString(),
      strategy,
      blocks_scanned: scan.blocksScanned,
      transactions_seen: scan.transactionsSeen,
      successful_transactions: scan.events.length,
      accepted: ingestion.accepted || 0,
      duplicate: ingestion.duplicate || 0,
      caught_up: to >= stableTo,
    };
  } catch (error) {
    await markIndexerError(env, network, chainId, now, error);
    throw error;
  }
}

export async function getIndexerStatus(env, networkInput = 'testnet') {
  if (String(networkInput || '').toLowerCase() === 'all') {
    const statuses = [];
    for (const network of configuredNetworks(env)) {
      statuses.push(await getIndexerStatus(env, network));
    }
    return { networks: statuses };
  }

  const network = normalizeNetwork(networkInput);
  const row = await getIndexerCursor(env, network);
  return { network, cursor: row || null };
}

export async function scanRadiusBlocks(rpcUrl, options) {
  const events = [];
  let blocksScanned = 0;
  let transactionsSeen = 0;
  const blockBatchConcurrency = options.blockBatchConcurrency || DEFAULT_BLOCK_BATCH_CONCURRENCY;

  const ranges = [];
  for (let start = options.from; start <= options.to; start += BigInt(options.blockBatchSize)) {
    const end = minBigInt(options.to, start + BigInt(options.blockBatchSize) - 1n);
    ranges.push({ start, end });
  }

  for (let rangeOffset = 0; rangeOffset < ranges.length; rangeOffset += blockBatchConcurrency) {
    const group = ranges.slice(rangeOffset, rangeOffset + blockBatchConcurrency);
    const blockGroups = await Promise.all(group.map((range) => fetchBlockBatch(rpcUrl, range.start, range.end)));
    const transactions = [];

    for (const blocks of blockGroups) {
      for (const block of blocks) {
        blocksScanned += 1;
        const blockNumber = block?.number || null;
        const blockTransactions = Array.isArray(block?.transactions) ? block.transactions : [];
        transactionsSeen += blockTransactions.length;

        for (const tx of blockTransactions) {
          if (tx && typeof tx === 'object') {
            transactions.push({ tx, blockNumber });
          }
        }
      }
    }

    for (let offset = 0; offset < transactions.length; offset += options.receiptBatchSize) {
      const batch = transactions.slice(offset, offset + options.receiptBatchSize);
      const receipts = await fetchReceiptBatch(rpcUrl, batch.map(({ tx }) => tx.hash).filter(Boolean));
      for (const { tx, blockNumber } of batch) {
        const receipt = receipts.get(String(tx.hash || '').toLowerCase());
        const event = buildSuccessfulTransactionEvent(options.network, options.chainId, tx, receipt, blockNumber);
        if (event) events.push(event);
      }
    }
  }

  return { blocksScanned, transactionsSeen, events };
}

export async function scanRadiusTokenLogs(rpcUrl, options) {
  const address = SBC_ADDRESSES[options.network];
  if (!address) throw new Error(`No token log address configured for ${options.network}`);

  const logs = await rpcCall(rpcUrl, 'eth_getLogs', [{
    address,
    fromBlock: toHexQuantity(options.from),
    toBlock: toHexQuantity(options.to),
  }]);
  const hashes = [...new Set((Array.isArray(logs) ? logs : [])
    .map((log) => String(log.transactionHash || '').toLowerCase())
    .filter(Boolean))];

  const transactions = await fetchTransactionBatch(rpcUrl, hashes);
  const receipts = await fetchReceiptBatch(rpcUrl, hashes);
  const events = [];

  for (const hash of hashes) {
    const tx = transactions.get(hash);
    const receipt = receipts.get(hash);
    const event = buildSuccessfulTransactionEvent(options.network, options.chainId, tx, receipt, receipt?.blockNumber || tx?.blockNumber);
    if (event) events.push({ ...event, asset: 'SBC' });
  }

  return {
    blocksScanned: Number(options.to - options.from + 1n),
    transactionsSeen: hashes.length,
    events,
  };
}

export function buildSuccessfulTransactionEvent(network, chainId, tx, receipt, fallbackBlockNumber) {
  if (!tx || !receipt || !isSuccessfulStatus(receipt.status)) return null;
  const blockNumber = receipt.blockNumber || tx.blockNumber || fallbackBlockNumber;
  const timestampMs = Number(hexQuantityToBigInt(blockNumber, 'blockNumber'));
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) return null;

  return {
    network,
    chain_id: chainId,
    tx_hash: receipt.transactionHash || tx.hash,
    status: 1,
    timestamp_ms: timestampMs,
    from: tx.from,
    to: tx.to || null,
    input: tx.input || tx.data || '0x',
    value_wei: hexToDecimalString(tx.value || '0x0'),
    asset: tx.value && hexQuantityToBigInt(tx.value, 'value') !== 0n ? 'RUSD' : null,
  };
}

export function hexToDecimalString(value) {
  return hexQuantityToBigInt(value, 'quantity').toString();
}

export function hexQuantityToBigInt(value, field) {
  if (typeof value !== 'string' || !/^0x[0-9a-fA-F]+$/.test(value)) {
    throw new Error(`Invalid ${field || 'hex quantity'}`);
  }
  return BigInt(value);
}

async function fetchBlockBatch(rpcUrl, from, to) {
  const calls = [];
  for (let block = from; block <= to; block += 1n) {
    calls.push({
      id: block.toString(),
      method: 'eth_getBlockByNumber',
      params: [toHexQuantity(block), true],
    });
  }

  const responses = await rpcBatch(rpcUrl, calls);
  return calls.map((call) => responses.get(call.id)?.result).filter(Boolean);
}

async function fetchReceiptBatch(rpcUrl, hashes) {
  const calls = hashes.map((hash, index) => ({
    id: String(index),
    method: 'eth_getTransactionReceipt',
    params: [hash],
  }));
  if (calls.length === 0) return new Map();

  const responses = await rpcBatch(rpcUrl, calls);
  const receipts = new Map();
  for (const call of calls) {
    const receipt = responses.get(call.id)?.result;
    if (receipt?.transactionHash) receipts.set(receipt.transactionHash.toLowerCase(), receipt);
  }
  return receipts;
}

async function fetchTransactionBatch(rpcUrl, hashes) {
  const calls = hashes.map((hash, index) => ({
    id: String(index),
    method: 'eth_getTransactionByHash',
    params: [hash],
  }));
  if (calls.length === 0) return new Map();

  const responses = await rpcBatch(rpcUrl, calls);
  const transactions = new Map();
  for (const call of calls) {
    const tx = responses.get(call.id)?.result;
    if (tx?.hash) transactions.set(tx.hash.toLowerCase(), tx);
  }
  return transactions;
}

async function rpcCall(rpcUrl, method, params) {
  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (!response.ok) throw new Error(`RPC HTTP ${response.status}`);

  const payload = await response.json();
  if (payload.error) throw new Error(`RPC ${method} error ${payload.error.code}: ${payload.error.message}`);
  return payload.result;
}

async function rpcBatch(rpcUrl, calls) {
  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(calls.map((call) => ({
      jsonrpc: '2.0',
      id: call.id,
      method: call.method,
      params: call.params,
    }))),
  });
  if (!response.ok) throw new Error(`RPC batch HTTP ${response.status}`);

  const payload = await response.json();
  if (!Array.isArray(payload)) throw new Error('RPC batch returned a non-array response');

  const responses = new Map();
  for (const item of payload) {
    if (item.error) throw new Error(`RPC batch item ${item.id} error ${item.error.code}: ${item.error.message}`);
    responses.set(String(item.id), item);
  }
  return responses;
}

async function getIndexerCursor(env, network) {
  return env.DB.prepare(
    `select network, chain_id, last_scanned_block, last_run_at, last_success_at, last_error,
            last_blocks_scanned, last_transactions_seen, last_successful_transactions, updated_at
       from radius_indexer_cursors
      where network = ?`
  ).bind(network).first();
}

async function saveIndexerCursor(env, input) {
  await env.DB.prepare(
    `insert into radius_indexer_cursors
      (network, chain_id, last_scanned_block, last_run_at, last_success_at, last_error,
       last_blocks_scanned, last_transactions_seen, last_successful_transactions, updated_at)
     values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     on conflict(network) do update set
       chain_id = excluded.chain_id,
       last_scanned_block = excluded.last_scanned_block,
       last_run_at = excluded.last_run_at,
       last_success_at = excluded.last_success_at,
       last_error = excluded.last_error,
       last_blocks_scanned = excluded.last_blocks_scanned,
       last_transactions_seen = excluded.last_transactions_seen,
       last_successful_transactions = excluded.last_successful_transactions,
       updated_at = excluded.updated_at`
  ).bind(
    input.network,
    input.chainId,
    String(input.lastScannedBlock),
    input.now,
    input.error ? null : input.now,
    input.error,
    input.blocksScanned,
    input.transactionsSeen,
    input.successfulTransactions,
    input.now
  ).run();
}

async function markIndexerError(env, network, chainId, now, error) {
  const existing = await getIndexerCursor(env, network);
  await env.DB.prepare(
    `insert into radius_indexer_cursors
      (network, chain_id, last_scanned_block, last_run_at, last_success_at, last_error,
       last_blocks_scanned, last_transactions_seen, last_successful_transactions, updated_at)
     values (?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
     on conflict(network) do update set
       chain_id = excluded.chain_id,
       last_run_at = excluded.last_run_at,
       last_error = excluded.last_error,
       last_blocks_scanned = 0,
       last_transactions_seen = 0,
       last_successful_transactions = 0,
       updated_at = excluded.updated_at`
  ).bind(
    network,
    chainId,
    existing?.last_scanned_block || '0',
    now,
    existing?.last_success_at || null,
    error.message || String(error),
    now
  ).run();
}

function isSuccessfulStatus(status) {
  if (typeof status === 'number') return status === 1;
  if (typeof status === 'string') return status === '0x1' || status === '1';
  return false;
}

async function ingestEventChunks(events, ingestEvents) {
  const totals = { accepted: 0, duplicate: 0 };
  for (let offset = 0; offset < events.length; offset += 500) {
    const result = await ingestEvents(events.slice(offset, offset + 500));
    totals.accepted += result.accepted || 0;
    totals.duplicate += result.duplicate || 0;
  }
  return totals;
}

function envInt(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function optionInt(optionValue, envValue, fallback) {
  return envInt(optionValue ?? envValue, fallback);
}

function configuredNetworks(env) {
  return normalizeNetworkList(env.INDEXER_NETWORKS || env.INDEXER_NETWORK || 'testnet');
}

function normalizeNetworkList(value) {
  if (String(value || '').toLowerCase() === 'all') return Object.keys(NETWORKS);
  const networks = String(value || '')
    .split(',')
    .map((network) => network.trim())
    .filter(Boolean)
    .map(normalizeNetwork);
  return [...new Set(networks)];
}

function rpcUrlForNetwork(env, network) {
  const key = `RADIUS_${network.toUpperCase()}_RPC_URL`;
  if (env[key]) return env[key];
  if (env.RADIUS_RPC_URL && !env.INDEXER_NETWORKS) return env.RADIUS_RPC_URL;
  return DEFAULT_RPC_URLS[network];
}

function toHexQuantity(value) {
  return `0x${value.toString(16)}`;
}

function minBigInt(left, right) {
  return left < right ? left : right;
}

function maxBigInt(left, right) {
  return left > right ? left : right;
}
