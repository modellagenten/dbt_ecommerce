{% macro generate_date_suffix_table_name(base_table_name) %}
    {% set current_date = modules.datetime.date.today().strftime('%Y_%m_%d') %}
    {% set table_name = base_table_name ~ '_' ~ current_date %}
    {{ return(table_name) }}
{% endmacro %}