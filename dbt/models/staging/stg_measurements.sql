-- Staging model: one row per sensor measurement value, typed and deduplicated.
with source as (
    select * from read_parquet('{{ env_var("PARQUET_DIR", "../data/parquet") }}/*.parquet')
)

select
    cast(sensor_id as bigint)        as sensor_id,
    cast(timestamp as timestamp)     as measured_at,
    cast(latitude as double)         as latitude,
    cast(longitude as double)        as longitude,
    value_type,
    cast(value as double)            as value
from source
where value is not null
