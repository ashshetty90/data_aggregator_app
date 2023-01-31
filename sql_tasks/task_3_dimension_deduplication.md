# Column de-duplication

The entire query can be broken down into three parts
## Step-1
```sqlite-psql
select *,
    /* marking rows with no change as per the client_id, product_id, interest_rate  */
    case when lag (client_id) over (partition by agrmnt_id order by actual_from_dt) = client_id
          and lag (product_id) over (partition by agrmnt_id order by actual_from_dt) = product_id
          and lag (interest_rate) over (partition by agrmnt_id order by actual_from_dt) = interest_rate
          then 1
          else 0 end as change_flag
    from dim_dep_agreement
```
The above sql creates a _change_flag_ column to identify the duplicates by comparing the current row with the previous row using the LAG() function. In the attached screenshot you can see this created a boolean column highlighting duplicates and non-suplicates
[step-1](images/step_1.png) 

## Step-2
```sqlite-psql
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
  ) as row_duplicates
```
In step-2 using the LEAD() function, I am creating an "OR" condition, comparin row-1 with row2. So if you observe the column _dup_rows_ in the [screenshot](images/step-2.png), it is basically ORing the current and the next row to duplicate rows

## Step-3
The last and the final step is two group these records and find the **min** of **actual_from_dt** and the **max** of **actual_to_dt** column grouped by  agrmnt_id,client_id,product_id,interest_rate,dup_rows columns
```sqlite-psql
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
```
The screenshot [here](images/step-3.png) displays the final output
