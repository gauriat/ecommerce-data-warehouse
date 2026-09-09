with products as (
    select * from {{ source('raw', 'products') }}
),

translations as (
    select * from {{ source('raw', 'product_category_name_translation') }}
),

cleaned as (
    select
        p.product_id,
        coalesce(p.product_category_name, 'unknown')          as product_category_name,
        coalesce(t.product_category_name_english, 'unknown')  as product_category_name_english,
        nullif(p.product_weight_g, 0)                         as product_weight_g,
        nullif(p.product_length_cm, 0)                        as product_length_cm,
        nullif(p.product_height_cm, 0)                        as product_height_cm,
        nullif(p.product_width_cm, 0)                         as product_width_cm
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
    where p.product_id is not null
)

select * from cleaned