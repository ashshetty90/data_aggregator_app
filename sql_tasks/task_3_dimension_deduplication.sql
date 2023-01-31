create table dim_dep_agreement_compacted as (
select min(sk) as sk,agrmnt_id,client_id,product_id,interest_rate,min(actual_from_dt) as actual_from_dt,max(actual_to_dt) as actual_to_dt
from
(
  select *,
  /* grouping duplicate rows together using change_flag */
  case when change_flag > ( lead(change_flag) over ( partition by agrmnt_id order by actual_from_dt ) ) then change_flag
  else ( lead (change_flag) over ( partition by agrmnt_id order by actual_from_dt)) end as dup_rows
  from
  (
    select *,
    /* marking rows with no change as per the client_id, product_id, interest_rate  */
    case when lag (client_id) over (partition by agrmnt_id order by actual_from_dt) = client_id
          and lag (product_id) over (partition by agrmnt_id order by actual_from_dt) = product_id
          and lag (interest_rate) over (partition by agrmnt_id order by actual_from_dt) = interest_rate
          then 1
          else 0 end as change_flag
    from dim_dep_agreement
  ) as grouped_duplicates
) as non_duplicates
group by agrmnt_id,client_id,product_id,interest_rate,dup_rows
order by sk
)