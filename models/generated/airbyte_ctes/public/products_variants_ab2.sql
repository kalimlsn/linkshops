{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('products_variants_ab1') }}
select
    _airbyte_products_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('id') }},
    cast(sku as {{ dbt_utils.type_string() }}) as sku,
    {{ cast_to_boolean('on_sale') }} as on_sale,
    cast(quantity as {{ dbt_utils.type_string() }}) as quantity,
    cast(parent_id as {{ dbt_utils.type_string() }}) as parent_id,
    cast({{ adapter.quote('attributes') }} as {{ type_json() }}) as {{ adapter.quote('attributes') }},
    cast(sale_price as {{ dbt_utils.type_string() }}) as sale_price,
    cast(regular_price as {{ dbt_utils.type_string() }}) as regular_price,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('products_variants_ab1') }}
-- variants at products/variants
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

