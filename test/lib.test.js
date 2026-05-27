import assert from 'node:assert/strict';
import test from 'node:test';

import {
  addDecimalStrings,
  bucketForTimestamp,
  classifyOperation,
  normalizeSuccessfulTransactionEvent,
  periodBounds,
} from '../src/lib.js';

test('classifies common EVM operations', () => {
  assert.equal(classifyOperation({ input: '0xa9059cbb0000', value_wei: '0' }), 'TOKEN_TRANSFER');
  assert.equal(classifyOperation({ input: '0x095ea7b30000', value_wei: '0' }), 'APPROVE');
  assert.equal(classifyOperation({ input: '0x', value_wei: '1' }), 'TRANSFER');
  assert.equal(classifyOperation({ to: '0x3333333333333333333333333333333333333333', input: '0x12345678', value_wei: '0' }), 'CALL');
});

test('normalizes successful transaction events', () => {
  const event = normalizeSuccessfulTransactionEvent({
    network: 'TESTNET',
    chain_id: 72344,
    tx_hash: '0x1111111111111111111111111111111111111111111111111111111111111111',
    status: 1,
    timestamp_ms: 1779148800000,
    from: '0x2222222222222222222222222222222222222222',
    to: '0x3333333333333333333333333333333333333333',
    input: '0x',
    value_wei: '000100',
  });

  assert.equal(event.network, 'testnet');
  assert.equal(event.chainId, 72344);
  assert.equal(event.opType, 'TRANSFER');
  assert.equal(event.valueWei, '100');
  assert.equal(event.asset, 'RUSD');
});

test('builds UTC buckets', () => {
  const timestamp = Date.parse('2026-05-19T08:34:56.789Z');
  assert.equal(bucketForTimestamp(timestamp, 'minute'), '2026-05-19T08:34:00.000Z');
  assert.equal(bucketForTimestamp(timestamp, 'hour'), '2026-05-19T08:00:00.000Z');
  assert.equal(bucketForTimestamp(timestamp, 'day'), '2026-05-19');
});

test('adds decimal strings beyond sqlite integer range', () => {
  assert.equal(
    addDecimalStrings('100000000000000000000000000000000000000', '900000000000000000000000000000000000000'),
    '1000000000000000000000000000000000000000'
  );
});

test('computes current and previous equivalent periods', () => {
  assert.deepEqual(periodBounds('week', '2026-05-19'), {
    currentStart: '2026-05-18',
    currentEnd: '2026-05-19',
    previousStart: '2026-05-16',
    previousEnd: '2026-05-17',
  });
});
