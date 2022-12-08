{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('products_scd') }}
select
    _airbyte_products_hashid,
    {{ json_extract_array('_links', ['self'], ['self']) }} as {{ adapter.quote('self') }},
    {{ json_extract_array('_links', ['collection'], ['collection']) }} as collection,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('products_scd') }} as table_alias
-- _links at products/_links
where 1 = 1
and _links is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

