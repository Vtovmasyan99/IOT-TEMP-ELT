-- Staging
create table if not exists staging_temperature_raw (
  run_id        uuid not null,
  source_file   text not null,
  id            text,
  room_id_raw   text,
  noted_date_raw text,
  temp_raw      text,
  location_raw  text,
  row_number    bigint generated always as identity,
  loaded_at     timestamptz default now()
);

-- Final
create table if not exists temperature_readings (
  id           text primary key,
  room_id      text not null,
  reading_ts   timestamptz not null,
  temp_c       numeric not null,
  location     text not null check (location in ('In','Out')),
  source_file  text not null,
  ingested_at  timestamptz default now()
);

create index if not exists ix_temperature_readings_ts on temperature_readings (reading_ts);

-- Rejects
create table if not exists temperature_rejects (
  run_id       uuid not null,
  source_file  text not null,
  raw_row      jsonb not null,
  error_reason text not null,
  rejected_at  timestamptz default now()
);

create table if not exists elt_runs (
  run_id                uuid primary key,
  source_file           text not null,
  file_checksum_sha256  text not null,
  started_at            timestamptz not null,
  ended_at              timestamptz,
  status                text check (status in ('running','success','failed')) default 'running',
  rows_in_file          integer,
  rows_loaded_staging   integer default 0,
  rows_valid            integer default 0,
  rows_rejected         integer default 0,
  message               text
);


