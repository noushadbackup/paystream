{# The models run on Trino in the stack and on DuckDB in CI; these are the only two places the dialects differ. #}

{% macro p95(expr) -%}
    {%- if target.type == 'duckdb' -%} approx_quantile({{ expr }}, 0.95)
    {%- else -%} approx_percentile({{ expr }}, 0.95)
    {%- endif -%}
{%- endmacro %}

{% macro millis_between(start_ts, end_ts) -%}
    {%- if target.type == 'duckdb' -%} cast(epoch_ms({{ end_ts }}) - epoch_ms({{ start_ts }}) as double)
    {%- else -%} cast(to_milliseconds({{ end_ts }} - {{ start_ts }}) as double)
    {%- endif -%}
{%- endmacro %}

{# Bangladesh reports by local business day (Asia/Dhaka, UTC+6), not by UTC date. #}
{% macro dhaka_date(ts) -%}
    {%- if target.type == 'duckdb' -%} cast(timezone('Asia/Dhaka', {{ ts }}) as date)
    {%- else -%} cast({{ ts }} AT TIME ZONE 'Asia/Dhaka' as date)
    {%- endif -%}
{%- endmacro %}

{% macro list_agg(expr) -%}
    {%- if target.type == 'duckdb' -%} string_agg(distinct {{ expr }}, ',' order by {{ expr }})
    {%- else -%} array_join(array_sort(array_distinct(array_agg({{ expr }}))), ',')
    {%- endif -%}
{%- endmacro %}

{# Stable pseudonym for sharing data outside the institution: same input, same token; not reversible without the salt. #}
{% macro pseudonymize(expr) -%}
    {%- if target.type == 'duckdb' -%} substr(sha256('{{ var("pii_salt") }}' || {{ expr }}), 1, 16)
    {%- else -%} substr(lower(to_hex(sha256(to_utf8('{{ var("pii_salt") }}' || {{ expr }})))), 1, 16)
    {%- endif -%}
{%- endmacro %}
