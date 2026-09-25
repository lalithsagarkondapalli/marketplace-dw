{#
  SCD Type 2 guard: for each business key, validity ranges [valid_from, valid_to)
  must not overlap. Returns offending rows.
#}
{% test no_overlapping_ranges(model, key, valid_from, valid_to) %}
select a.{{ key }}, a.{{ valid_from }}, a.{{ valid_to }}
from {{ model }} a
join {{ model }} b
  on a.{{ key }} = b.{{ key }}
 and a.{{ valid_from }} < b.{{ valid_from }}
 and coalesce(a.{{ valid_to }}, 'infinity'::timestamp) > b.{{ valid_from }}
{% endtest %}
