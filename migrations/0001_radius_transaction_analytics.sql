create table if not exists metric_rollups_minute (
  network text not null,
  bucket_minute text not null,
  op_type text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  active_address_estimate integer,
  updated_at text not null,
  primary key (network, bucket_minute, op_type)
);

create table if not exists metric_rollups_hour (
  network text not null,
  bucket_hour text not null,
  op_type text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  active_address_estimate integer,
  updated_at text not null,
  primary key (network, bucket_hour, op_type)
);

create table if not exists metric_rollups_day (
  network text not null,
  bucket_day text not null,
  op_type text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  active_address_estimate integer,
  updated_at text not null,
  primary key (network, bucket_day, op_type)
);

create table if not exists metric_value_rollups_minute (
  network text not null,
  bucket_minute text not null,
  asset text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  updated_at text not null,
  primary key (network, bucket_minute, asset)
);

create table if not exists metric_value_rollups_hour (
  network text not null,
  bucket_hour text not null,
  asset text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  updated_at text not null,
  primary key (network, bucket_hour, asset)
);

create table if not exists metric_value_rollups_day (
  network text not null,
  bucket_day text not null,
  asset text not null,
  successful_tx_count integer not null,
  value_wei_sum text not null default '0',
  updated_at text not null,
  primary key (network, bucket_day, asset)
);

create table if not exists processed_successful_transactions (
  network text not null,
  tx_hash text not null,
  chain_id integer not null,
  event_timestamp_ms integer not null,
  first_seen_at text not null,
  from_address text not null,
  to_address text,
  op_type text not null,
  primary key (network, tx_hash)
);

create index if not exists idx_processed_successful_transactions_seen
  on processed_successful_transactions (network, first_seen_at);

create table if not exists metric_active_addresses (
  network text not null,
  bucket_grain text not null,
  bucket_start text not null,
  address text not null,
  first_seen_at text not null,
  primary key (network, bucket_grain, bucket_start, address)
);

create table if not exists first_successful_transactions (
  network text not null,
  from_address text not null,
  tx_hash text not null,
  first_seen_at text not null,
  primary key (network, from_address)
);

create table if not exists metric_ingestion_batches (
  id text primary key,
  source text not null,
  network text not null,
  bucket_start text not null,
  bucket_end text not null,
  ingested_at text not null,
  notes text
);

create table if not exists metric_reconciliation_runs (
  id text primary key,
  network text not null,
  bucket_start text not null,
  bucket_end text not null,
  source text not null,
  status text not null,
  started_at text not null,
  completed_at text,
  notes text
);

create table if not exists product_wallet_events (
  id text primary key,
  network text,
  user_id text,
  wallet_address text not null,
  event_type text not null,
  occurred_at text not null,
  metadata_json text,
  inserted_at text not null
);

create index if not exists idx_product_wallet_events_lookup
  on product_wallet_events (network, event_type, occurred_at);

create index if not exists idx_product_wallet_events_wallet
  on product_wallet_events (network, wallet_address);

create table if not exists operation_type_registry (
  op_type text primary key,
  description text not null,
  is_system integer not null default 0,
  updated_at text not null
);

insert or ignore into operation_type_registry (op_type, description, is_system, updated_at) values
  ('TRANSFER', 'Native RUSD value transfer', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('TOKEN_TRANSFER', 'ERC-20 transfer(address,uint256)', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('APPROVE', 'ERC-20 approve(address,uint256)', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('CALL', 'Contract call not otherwise classified', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('OP', 'Known Radius protocol or system operation', 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('UNKNOWN', 'Unclassified transaction', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
