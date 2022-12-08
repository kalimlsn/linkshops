{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('products_scd') }}
{{ unnest_cte(ref('products_scd'), 'products', 'variants') }}
select
    _airbyte_products_hashid,
    {{ json_extract_scalar(unnested_column_value('variants'), ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar(unnested_column_value('variants'), ['sku'], ['sku']) }} as sku,
    {{ json_extract_scalar(unnested_column_value('variants'), ['on_sale'], ['on_sale']) }} as on_sale,
    {{ json_extract_scalar(unnested_column_value('variants'), ['quantity'], ['quantity']) }} as quantity,
    {{ json_extract_scalar(unnested_column_value('variants'), ['parent_id'], ['parent_id']) }} as parent_id,
    {{ json_extract('', unnested_column_value('variants'), ['attributes'], ['attributes']) }} as {{ adapter.quote('attributes') }},
    {{ json_extract_scalar(unnested_column_value('variants'), ['sale_price'], ['sale_price']) }} as sale_price,
    {{ json_extract_scalar(unnested_column_value('variants'), ['regular_price'], ['regular_price']) }} as regular_price,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('products_scd') }} as table_alias
-- variants at products/variants
{{ cross_join_unnest('products', 'variants') }}
where 1 = 1
and variants is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

