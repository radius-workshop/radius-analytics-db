create table if not exists radius_indexer_cursors (
  network text primary key,
  chain_id integer not null,
  last_scanned_block text not null,
  last_run_at text,
  last_success_at text,
  last_error text,
  last_blocks_scanned integer not null default 0,
  last_transactions_seen integer not null default 0,
  last_successful_transactions integer not null default 0,
  updated_at text not null
);
