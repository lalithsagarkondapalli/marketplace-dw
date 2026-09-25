{# Deterministic md5 surrogate key over one or more columns (nulls made explicit). #}
{% macro surrogate_key(columns) -%}
    md5({% for c in columns %}coalesce(cast({{ c }} as text), '~null~'){% if not loop.last %} || '|' || {% endif %}{% endfor %})
{%- endmacro %}
