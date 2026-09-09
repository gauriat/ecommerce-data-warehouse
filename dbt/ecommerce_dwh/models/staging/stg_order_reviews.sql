with source as (
    select * from {{ source('raw', 'order_reviews') }}
),

cleaned as (
    select
        review_id,
        order_id,
        (review_score)::int                    as review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::timestamp        as review_creation_ts,
        review_answer_timestamp::timestamp     as review_answer_ts
    from source
    where order_id is not null
      and review_score is not null
      and (review_score)::int between 1 and 5
)

select * from cleaned