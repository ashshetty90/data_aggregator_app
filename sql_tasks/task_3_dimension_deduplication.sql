MERGE
    INTO dim_dep_agreement_compacted AS target
        USING (SELECT *
               FROM dim_dep_agreement) AS source
        ON source.agrmnt_id = target.agrmnt_id
        WHEN MATCHED AND (
                target.client_id IS DISTINCT FROM source.client_id
                OR target.product_id IS DISTINCT FROM source.product_id
                OR target.interest_rate IS DISTINCT FROM source.interest_rate
            )
            THEN UPDATE
            SET
                ,actual_from_dt = target.actual_from_dt
                ,actual_to_dt = source.actual_to_dt
                ,client_id =source.client_id
                ,product_id = source.product_id
                ,interest_rate = source.interest_rate
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (actual_from_dt, actual_to_dt, client_id,
                         product_id, interest_rate)
            VALUES (actual_from_dt, actual_to_dt, client_id,
                         product_id, interest_rate);


create table dim_dep_agreement_compacted as
(
  select * except(row_num) from (
      select *,
        row_number() over (
          partition by Firstname, Lastname
          order by creation_date desc
        ) row_num
      FROM
      dataset.table_name
   ) t
  where row_num=1
);



create  table dim_dep_agreement_compacted as
(
  select * except(row_num) from (
      select *,
        row_number() over (
          partition by client_id, product_id, interest_rate
          order by creation_date desc
        ) row_num
      FROM
      dim_dep_agreement
   ) t
  where row_num=1
)

-- range between '15 day' preceding and current row


----------- v2 ------------------(unexpected behaviour for row 5)
SELECT
A.agrmnt_id,
MIN(DATE(A.actual_from_dt)),
MAX(DATE(A.actual_to_dt)),
a.client_id,
A.product_id ,
A.interest_rate
  FROM dim_dep_agreement a inner join dim_dep_agreement b
    on a.agrmnt_id=b.agrmnt_id
    and a.client_id = b.client_id
    AND A.product_id = b.product_id
    AND A.interest_rate = B.interest_rate
GROUP BY A.agrmnt_id,a.client_id,
A.product_id ,
A.interest_rate
----------- v2 ------------------

--- final ----

SELECT min(sk) as sk,agrmnt_id,client_id,product_id,interest_rate,min(actual_from_dt) as actual_from_dt,MAX(actual_to_dt) as actual_to_dt
FROM
(
  select *,
  /* grouping duplicate rows together using change_flag */
  case when change_flag > ( LEAD(change_flag) OVER ( PARTITION BY agrmnt_id ORDER BY actual_from_dt ) ) then change_flag
  else ( LEAD (change_flag) OVER ( PARTITION BY agrmnt_id ORDER BY actual_from_dt)) end as dup_rows
  FROM
  (
    SELECT *,
    /* marking rows with no change as per the client_id, product_id, interest_rate  */
    CASE WHEN LAG (client_id) OVER (PARTITION BY agrmnt_id ORDER BY actual_from_dt) = client_id
          and LAG (product_id) OVER (PARTITION BY agrmnt_id ORDER By actual_from_dt) = product_id
          and LAG (interest_rate) OVER (PARTITION BY agrmnt_id ORDER BY actual_from_dt) = interest_rate
          then 1
          else 0 end as change_flag
    FROM task_3_dimension_de_duplication
  ) as a
) AS B
GROUP by agrmnt_id,client_id,product_id,interest_rate,dup_rows
order by sk
---- final ----