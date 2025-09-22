with norm as (
  select
    s.run_id,
    s.source_file,
    s.id,
    nullif(btrim(s.room_id_raw), '') as room_id,
    to_timestamp(s.noted_date_raw, 'DD-MM-YYYY HH24:MI') at time zone 'UTC' as reading_ts,
    case
      when s.temp_raw ~ '^-?[0-9]+(\\.[0-9]+)?$' then (s.temp_raw)::numeric
      else null
    end as temp_c,
    case
      when lower(btrim(s.location_raw)) = 'in' then 'In'
      when lower(btrim(s.location_raw)) = 'out' then 'Out'
      else null
    end as location,
    s.row_number,
    jsonb_build_object(
      'id', s.id,
      'room_id_raw', s.room_id_raw,
      'noted_date_raw', s.noted_date_raw,
      'temp_raw', s.temp_raw,
      'location_raw', s.location_raw,
      'row_number', s.row_number
    ) as raw_row
  from staging_temperature_raw s
  where s.run_id = :run_id
),
dedup as (
  select
    n.*,
    count(*) over (partition by n.source_file, n.id) as id_count_in_file,
    min(n.row_number) over (partition by n.source_file, n.id) as first_row_in_file
  from norm n
),
validated as (
  select
    d.*,
    case
      when d.id is null or btrim(d.id) = '' then 'null_id'
      when d.room_id is null or btrim(d.room_id) = '' then 'room_id_null'
      when d.reading_ts is null then 'date_parse_error'
      when d.temp_c is null then 'temp_not_numeric'
      when d.temp_c < (-50)::numeric or d.temp_c > 80::numeric then 'temp_out_of_range'
      when d.location is null then 'location_invalid'
      when d.id_count_in_file > 1 and d.row_number <> d.first_row_in_file then 'dup_in_file'
      else null
    end as error_reason
  from dedup d
),
ins_rejects as (
  insert into temperature_rejects (run_id, source_file, raw_row, error_reason)
  select v.run_id, v.source_file, v.raw_row, v.error_reason
  from validated v
  where v.error_reason is not null
  returning 1
),
upsert_valid as (
  insert into temperature_readings (id, room_id, reading_ts, temp_c, location, source_file)
  select v.id, v.room_id, v.reading_ts, v.temp_c, v.location, v.source_file
  from validated v
  where v.error_reason is null
  on conflict (id) do update set
    room_id = excluded.room_id,
    reading_ts = excluded.reading_ts,
    temp_c = excluded.temp_c,
    location = excluded.location,
    source_file = excluded.source_file
  returning 1
)
select
  (select count(*) from upsert_valid) as rows_valid,
  (select count(*) from ins_rejects) as rows_rejected;


