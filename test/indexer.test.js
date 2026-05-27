import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildSuccessfulTransactionEvent,
  hexQuantityToBigInt,
  hexToDecimalString,
} from '../src/indexer.js';

test('converts hex quantities to decimal strings', () => {
  assert.equal(hexToDecimalString('0xde0b6b3a7640000'), '1000000000000000000');
  assert.equal(hexQuantityToBigInt('0x19e405ec044', 'block').toString(), '1779196411972');
});

test('builds successful transaction ingestion events from rpc payloads', () => {
  const event = buildSuccessfulTransactionEvent(
    'testnet',
    72344,
    {
      hash: '0x1111111111111111111111111111111111111111111111111111111111111111',
      blockNumber: '0x19e405ec044',
      from: '0x2222222222222222222222222222222222222222',
      to: '0x3333333333333333333333333333333333333333',
      input: '0x',
      value: '0xde0b6b3a7640000',
    },
    {
      transactionHash: '0x1111111111111111111111111111111111111111111111111111111111111111',
      blockNumber: '0x19e405ec044',
      status: '0x1',
    },
    null
  );

  assert.deepEqual(event, {
    network: 'testnet',
    chain_id: 72344,
    tx_hash: '0x1111111111111111111111111111111111111111111111111111111111111111',
    status: 1,
    timestamp_ms: 1779196411972,
    from: '0x2222222222222222222222222222222222222222',
    to: '0x3333333333333333333333333333333333333333',
    input: '0x',
    value_wei: '1000000000000000000',
    asset: 'RUSD',
  });
});

test('ignores failed receipts', () => {
  assert.equal(
    buildSuccessfulTransactionEvent(
      'testnet',
      72344,
      {
        hash: '0x1111111111111111111111111111111111111111111111111111111111111111',
        blockNumber: '0x19e405ec044',
      },
      { status: '0x0' },
      null
    ),
    null
  );
});
