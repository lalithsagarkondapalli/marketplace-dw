{# Cast raw text to a type, turning empty strings into nulls instead of failing. #}
{% macro to_ts(col) -%} cast(nullif(trim({{ col }}), '') as timestamp) {%- endmacro %}
{% macro to_num(col) -%} cast(nullif(trim({{ col }}), '') as numeric(12,2)) {%- endmacro %}
{% macro to_int(col) -%} cast(cast(nullif(trim({{ col }}), '') as numeric) as integer) {%- endmacro %}
