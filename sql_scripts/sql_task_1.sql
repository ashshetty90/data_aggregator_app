
-------- v1--------------------------------
select
transaction_id,
user_id,
date,
count(transaction_id) OVER (PARTITION BY user_id ORDER BY date)
--ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY date)
--FIRST_VALUE(transaction_id) OVER(PARTITION BY user_id ORDER BY date)
from transactions_compact
--WHERE date >= current_date - interval '7 days'
WHERE DATE(date) < current_date
AND DATE(date) >= current_date - interval '7 days'
GROUP BY transaction_id,
user_id,
date
----------- v1 -------------------------------
------------ v2 ------------------------------


select
transaction_id,
user_id,
date,
count(transaction_id) OVER (PARTITION BY user_id ORDER BY transaction_id,user_id,date)
--ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY date)
--FIRST_VALUE(transaction_id) OVER(PARTITION BY user_id ORDER BY date)
from transactions_compact
--WHERE date >= current_date - interval '7 days'
WHERE DATE(date) < current_date
AND DATE(date) >= current_date - interval '15 days'
GROUP BY transaction_id,
user_id,
date
------------ v2 ------------------------------




----------- final query ------------------

select
transaction_id,
user_id,
date,
count(transaction_id) OVER (PARTITION BY user_id ORDER BY DATE(date) RANGE BETWEEN INTERVAL '8' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING)
from task_2_feature_table_computation
GROUP BY transaction_id,
user_id,
date
----------- final query ------------------