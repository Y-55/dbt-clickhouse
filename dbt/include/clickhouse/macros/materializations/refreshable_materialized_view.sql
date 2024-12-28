{#
  Materialization for ClickHouse to create and manage refreshable materialized views.
  This materialization includes logic to handle existing views, create new ones, and perform
  updates using either atomic exchanges or intermediate steps as needed.
#}
{%- materialization refreshable_materialized_view, adapter='clickhouse' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}
  -- Specify the interval at which the materialized view should be refreshed, e.g., "EVERY 1 MINUTE"
  {%- set refresh_interval = config.require("refresh_interval") -%}

  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if backup_relation is none %}
    -- There is not existing relation, so we can just create
    {{ log('Creating new relation' + target_relation.name )}}
    {{ clickhouse__get_create_refreshable_materialized_view_as_sql(target_relation, sql, refresh_interval) }}

  {% elif existing_relation.can_exchange %}
    {{ log('Replacing existing relation ' + target_relation.name + ' using atomic exchange')}}
    -- We can do an atomic exchange, so no need for an intermediate
    {%- set mv_relation = target_relation.derivative('_mv', 'materialized_view') -%}
    -- Drop the existing mv
    {% call statement('main') -%}
      {{ clickhouse__drop_mv(mv_relation, cluster_clause) }}
    {%- endcall %}
    -- Create the new table
    {% call statement('main') -%}
      {% set has_contract = config.get('contract').enforced %}
      {{ create_table_or_empty(False, backup_relation, sql, has_contract) }}
    {%- endcall %}
    -- Exchange the new table with the existing relation
    {% do exchange_tables_atomic(backup_relation, existing_relation) %}
    -- Create the new mv
    {% call statement('main') -%}
      {{ clickhouse__create_refreshable_materialized_view(mv_relation, target_relation, sql, refresh_interval) }}
    {%- endcall %}

  {% else %}
    {{ log('Replacing existing materialized view ' + target_relation.name + ' using intermediate')}}
    -- We have to use an intermediate and rename accordingly
    {{ clickhouse__replace_refreshable_materialized_view(target_relation, existing_relation, intermediate_relation, backup_relation, sql, refresh_interval) }}
  {% endif %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

{% macro clickhouse__get_create_refreshable_materialized_view_as_sql(relation, sql, refresh_interval) -%}
  {% set has_contract = config.get('contract').enforced %}
  {%- set cluster_clause = on_cluster_clause(relation) -%}
  {%- set mv_relation = relation.derivative('_mv', 'materialized_view') -%}
  -- Create empty table with the model's sql statement
  {% call statement('main') %}    
    {{ create_table_or_empty(False, relation, sql, has_contract) }}
  {% endcall %}
  -- Create the materialized view to the table
  {% call statement('create existing mv: ' + mv_relation.name) -%}
    {{ clickhouse__create_refreshable_materialized_view(mv_relation, relation, sql, refresh_interval) }};
  {% endcall %}
{%- endmacro %}

{%macro clickhouse__create_refreshable_materialized_view(relation, target_table, sql, refresh_interval) -%}
  create materialized view if not exists {{ relation }} {{ on_cluster_clause(relation)}}
  refresh {{ refresh_interval }}
  to {{ target_table }}
  as {{ sql }}
{%- endmacro %}

{% macro clickhouse__replace_refreshable_materialized_view(target_relation, existing_relation, intermediate_relation, backup_relation, sql, refresh_interval) %}
  -- drop existing materialized view while we recreate the target table
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}
  {%- set mv_relation = target_relation.derivative('_mv', 'materialized_view') -%}
  {%- set has_contract = config.get('contract').enforced -%}
  -- Drop exisiting mv
  {% call statement('main') -%}
    {{ clickhouse__drop_mv(mv_relation, cluster_clause) }}
  {%- endcall %}
  -- Create the new table
  {% call statement('main') -%}
    {{ create_table_or_empty(False, intermediate_relation, sql, has_contract) }}
  {%- endcall %}
  -- Rename the existing table to backup
  {{ adapter.rename_relation(existing_relation, backup_relation) }}
  -- Rename the new table to the target
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  -- Create the new mv
  {% call statement('main') -%}
    {{ clickhouse__create_refreshable_materialized_view(mv_relation, target_relation, sql, refresh_interval) }}
  {%- endcall %}
{% endmacro %}
