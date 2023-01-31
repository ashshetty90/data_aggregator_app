select
transaction_id,
user_id,
date,
/*
getting the count of transaction ids by partitioning the data
based on the user_id for the window between yesterday and 7 days from today
*/
count(transaction_id) over
(partition by user_id
order by date(date)
range between interval '7' day preceding and interval '1' day preceding) as txn_count_last_7_days
from transactions
group by transaction_id,
user_id,
date;