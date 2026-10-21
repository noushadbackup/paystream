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
