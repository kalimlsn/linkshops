{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('products__links_collection_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte__links_hashid',
        'href',
    ]) }} as _airbyte_collection_hashid,
    tmp.*
from {{ ref('products__links_collection_ab2') }} tmp
-- collection at products/_links/collection
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

