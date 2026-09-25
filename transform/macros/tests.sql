{% test non_negative(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}


{% test canceled_has_timestamp(model) %}

{{ config(severity='warn') }}

select *
from {{ model }}
where status = 'canceled'
    and canceled_at is null

{% endtest %}


{% test paid_has_timestamp(model) %}

{{ config(severity='warn') }}

select *
from {{ model }}
where status = 'paid'
    and paid_at is null

{% endtest %}
