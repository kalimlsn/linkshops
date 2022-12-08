{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "public",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('products_variants_ab3') }}
select
    _airbyte_products_hashid,
    {{ adapter.quote('id') }},
    sku,
    on_sale,
    quantity,
    parent_id,
    {{ adapter.quote('attributes') }},
    sale_price,
    regular_price,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_variants_hashid
from {{ ref('products_variants_ab3') }}
-- variants at products/variants from {{ ref('products_scd') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

