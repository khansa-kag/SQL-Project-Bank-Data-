Select * from customer_nodes;
select * from customer_transactions;
select * from regions;

-- A. Customer Nodes Exploration
-- 1. How many unique nodes are there on the Data Bank system?
select  count(distinct node_id) as Unique_Nodes
from customer_nodes;
-- select customer_id, count(distinct node_id) as Unique_Nodes, region_id from customer_nodes group by customer_id, region_id;


-- 2. What is the number of nodes per region?
select count(c.node_id) as Count_Unique_Nodes, c.region_id, r.region_name
from customer_nodes c
join regions r on c.region_id = r. region_id
group by region_id, region_name;

select count(c.node_id) as Count_Unique_Nodes, r.region_id, r.region_name,
dense_rank () over (order by count(node_id) desc) as Ranking_Based_on_Node_Id
from customer_nodes c
join regions r on c.region_id = r. region_id
group by region_id, region_name;

-- 3. How many customers are allocated to each region?
select count(distinct c.customer_id) as Number_of_Customers, c.region_id, r.region_name
from customer_nodes c
join regions r on c.region_id = r. region_id
group by region_id, region_name;

-- 4. How many days on average are customers reallocated to a different node?
select Avg(datediff(end_date, start_date)) as Days_on_Average
from customer_nodes
WHERE end_date != '9999-12-31';



-- 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
WITH RankedNodes AS (
	SELECT
		r.region_id as r_id,
		r.region_name as r_name,
		DATEDIFF(end_date, start_date) AS days_spent,
		((ROW_NUMBER() 
		    OVER (PARTITION BY r.region_name 
	        ORDER BY DATEDIFF(c.end_date, c.start_date))-1)/COUNT(*)
			OVER (PARTITION BY r.region_name))*100 as percentile
	FROM customer_nodes c join regions r
	ON r.region_id = c.region_id
	WHERE end_date != '99991231'
	ORDER BY r.region_name, percentile)
SELECT
	r_name as region,
    MIN(CASE WHEN percentile >= 50 THEN days_spent END) AS median,
    MIN(CASE WHEN percentile >= 80 THEN days_spent END) AS percentile_80,
    MIN(CASE WHEN percentile >= 95 THEN days_spent END) AS percentile_95
FROM RankedNodes
GROUP BY r_name;

-- B. Customer Transactions
--  1. What is the unique count and total amount for each transaction type?
select txn_type, count(txn_type) as Unique_Count, sum(txn_amount) as total_amount
from customer_transactions
group by txn_type;

-- 2. What is the average total historical deposit counts and amounts for all customers? 
with deposit_summary as (
select customer_id, txn_type, count(txn_type) as total_deposit_counts, sum(txn_amount) as total_amount
from customer_transactions 
group by customer_id, txn_type
)
Select  txn_type, avg(total_deposit_counts), avg(total_amount)
from deposit_summary
where txn_type = 'deposit'
group by txn_type;

select customer_id, txn_type, count(txn_type) as total_deposit_counts, sum(txn_amount) as total_amount
from customer_transactions 
group by customer_id, txn_type;

-- 3. For each month - how many Data Bank customers make more than 1 
-- deposit and either 1 purchase or 1 withdrawal in a single month?
select customer_id, txn_type,
	Sum(case when txn_type = 'deposit' then 1 Else 0 End) as Deposit_count,
    sum(case when txn_type = 'purchase' then 1 Else 0 end) as purchase_count,
    sum(case  when txn_type = 'withdrawal' then 1 Else 0 end) as withdrawal_count
from customer_transactions
group by customer_id, txn_type;

select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name,
	Sum(case when txn_type = 'deposit' then 1 Else 0 End) as Deposit_count,
    sum(case when txn_type = 'purchase' then 1 Else 0 end) as purchase_count,
    sum(case  when txn_type = 'withdrawal' then 1 Else 0 end) as withdrawal_count
from customer_transactions
group by customer_id, MONTH(txn_date), MONTHNAME(txn_date);



With txn_summary as ( 
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name,
	Sum(case when txn_type = 'deposit' then 1 Else 0 End) as Deposit_count,
    sum(case when txn_type = 'purchase' then 1 Else 0 end) as purchase_count,
    sum(case  when txn_type = 'withdrawal' then 1 Else 0 end) as withdrawal_count
from customer_transactions
group by customer_id, MONTH(txn_date), MONTHNAME(txn_date)
)
select count(distinct t.customer_id) as number_of_customers, t.month_id, t.month_name
from txn_summary t
where t.Deposit_count > 1 and (t.purchase_count > 0 or t.withdrawal_count > 0) 
group by month_id, month_name;


-- 4. What is the closing balance for each customer at the end of the month?

select customer_id, last_day(txn_date) as end_of_the_month,
sum(case when txn_type = 'deposit' then txn_amount Else -txn_amount end) as closing_balance 
from customer_transactions
where txn_date <= last_day(txn_date) 
group by customer_id, last_day(txn_date);



with closing_balance_summary as (
select customer_id, last_day(txn_date) as end_of_the_month,
sum(case when txn_type = 'deposit' then txn_amount Else -txn_amount End ) as Closing_balance
from customer_transactions
where txn_date <= last_day(txn_date)
group by customer_id , last_day(txn_date))

Select c.customer_id, c.end_of_the_month, Month(c.end_of_the_month), MonthName(c.end_of_the_month), c.closing_balance
from closing_balance_summary c
group by customer_id, end_of_the_month;


-- 5. What is the percentage of customers who increase their closing balance
-- by more than 5%?
-- lead(closing_balance) over (partition by customer_id order by end_of_the_month) as next_month_closing_balance
-- By me : 
with closing_balance_summary as (
select customer_id, LAST_DAY(txn_date) as end_of_the_month,
sum(case when txn_type = 'deposit' then txn_amount Else -txn_amount end) as closing_balance
from customer_transactions
where txn_type IN ('deposit', 'withdrawal')
group by customer_id, LAST_DAY(txn_date)
), 
lead_balance as ( 
select customer_id, end_of_the_month, closing_balance,
lag(closing_balance) over (partition by customer_id order by end_of_the_month) as previous_month_closing_balance
from closing_balance_summary 
),
percentage_increase as (
select customer_id, end_of_the_month, closing_balance,previous_month_closing_balance, 
((previous_month_closing_balance- closing_balance) / closing_balance) * 100 AS percent_increase
from lead_balance
WHERE closing_balance > 0 
),
Customer_increases as (
select customer_id
from percentage_increase  
where percent_increase > 5
)
Select (COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM closing_balance_summary) * 100) AS percentage_of_customers_with_increase
from customer_increases;



-- Correct: 
    SELECT 
        customer_id, LAST_DAY(txn_date) AS end_date,
        SUM(CASE WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount ELSE txn_amount END) AS transactions
    FROM customer_transactions
    GROUP BY customer_id, LAST_DAY(txn_date);

WITH monthly_transactions AS (
    SELECT 
        customer_id,
        LAST_DAY(txn_date) AS end_date,
        SUM(CASE 
            WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount 
            ELSE txn_amount 
        END) AS transactions
    FROM customer_transactions
    GROUP BY customer_id, LAST_DAY(txn_date)
),

closing_balances AS (
    SELECT 
        m.customer_id,
        m.end_date,
        COALESCE(
            (SELECT SUM(mt.transactions) 
             FROM monthly_transactions mt
             WHERE mt.customer_id = m.customer_id 
             AND mt.end_date <= m.end_date), 0
        ) AS closing_balance
    FROM monthly_transactions m
),

pct_increase AS (
    SELECT 
        customer_id,
        end_date,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date) AS prev_closing_balance,
        100 * (closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date)) / NULLIF(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date), 0) AS pct_increase
    FROM closing_balances
)

SELECT 
    FORMAT(
        100.0 * COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions), 
        2
    ) AS pct_customers
FROM pct_increase
WHERE pct_increase > 5;

-- C. Data Allocation Challenge

-- Option 1: data is allocated based off the amount of money at the end of the previous month
-- Grouping the month, month name, and year
select  MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year
from customer_transactions
group by   MONTH(txn_date), MONTHNAME(txn_date), Year(txn_date);
-- check the days
select  Day(txn_date) as Day
from customer_transactions
group by   Day(txn_date)
order by Day(txn_date) desc;

-- Calculating the mount of money at the end of the previous month which is March (previous month) 

select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day,txn_amount,   
		sum(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over (partition by customer_id order by txn_date) as Previous_Month_Closing_Amount
from customer_transactions
where MONTH(txn_date) = 3 and MONTHNAME(txn_date) = 'March' and Year(txn_date) = 2020 and  Day(txn_date) Between 1 AND 31 ;


-- Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
select customer_id,txn_amount, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day,      
		Avg(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over (partition by customer_id order by txn_date) as Previous_30_Days_Amount
from customer_transactions
where MONTH(txn_date) = 4 and MONTHNAME(txn_date) = 'April' and Year(txn_date) = 2020 and  Day(txn_date) Between 1 AND 31 ;


select  MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year
from customer_transactions
group by   MONTH(txn_date), MONTHNAME(txn_date), Year(txn_date);

select  Day(txn_date) as Day
from customer_transactions
group by   Day(txn_date)
order by Day(txn_date) desc;

-- Option 3: data is updated real-time

-- Sub_Multichallenge question 
-- running customer balance column that includes the impact each transaction
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id order by txn_date) as running_balance
from customer_transactions; 
-- customer balance at the end of each month

select customer_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year,     
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) as ending_balance
from customer_transactions
group by customer_id, MONTHNAME(txn_date), Year(txn_date);

-- minimum, average and maximum values of the running balance for each customer
With balance as (
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id order by txn_date) as running_balance
from customer_transactions
)
select b.customer_id, min(b.running_balance) as Minimum, max(b.running_balance) as Maximum,  avg(b.running_balance) as Average
From balance b
group by customer_id;

With balance as (
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id order by txn_date) as running_balance
from customer_transactions
)
select b.customer_id, min(b.running_balance) as Minimum, max(b.running_balance) as Maximum,  avg(b.running_balance) as Average, b.month_name 
From balance b
group by customer_id, month_name;

-- Option 1: data is allocated based off the amount of money at the end of the previous month
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions;
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id, MONTHNAME(txn_date) order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions;
with all_running_balance as (
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id, MONTHNAME(txn_date) order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions
),

ending_balance as (
select e.customer_id, e.month_id, Max(e.running_balance) as month_end_balance
from all_running_balance e
group by e.customer_id, e.month_id
)
select  eb.month_id, Sum(eb.month_end_balance) as data_required_for_allocated
from ending_balance eb
group by eb.month_id
ORDER BY data_required_for_allocated DESC;
-- upper wale mein per month running balance show ho raha he jo neechae akae sub months ko add kar daeta. 


-- Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		AVG(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions;
select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		Sum(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id, MONTHNAME(txn_date) order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions;


With transaction_amount as (

select customer_id, MONTH(txn_date) AS month_id, Year(txn_date) as Year,     
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) as ending_balance
from customer_transactions
group by customer_id, MONTH(txn_date), Year(txn_date)
),
running_balance as (
select t.customer_id, t.month_id, 
 Sum(t.ending_balance) over (partition by t.customer_id order by t.month_id) As running_customer_balance
 from transaction_amount t
 ),
 average_balance as (
 select w.customer_id, Avg(w.running_customer_balance) as Average_Amount_of_Money 
 from running_balance w
 group by w.customer_id
 )
 select month_id, Round(Sum(y.Average_Amount_of_Money),0) as data_required_per_month
 from running_balance w
 Join average_balance y
 on w.customer_id = y.customer_id
 group by month_id;
 
 -- For Understanding: 
 select customer_id, MONTH(txn_date) AS month_id, Year(txn_date) as Year,txn_date, txn_type, txn_amount,    
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over (partition by customer_id order by MONTH(txn_date)) As running_customer_balance
from customer_transactions;

select customer_id, MONTH(txn_date) AS month_id, Year(txn_date) as Year, txn_date, txn_type,txn_amount,     
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over (partition by customer_id order by txn_date) As running_customer_balance
from customer_transactions;

select customer_id, MONTH(txn_date) AS month_id, Year(txn_date) as Year, txn_date, txn_type,txn_amount,     
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over (partition by customer_id) As running_customer_balance
from customer_transactions;

select customer_id, MONTH(txn_date) AS month_id, MONTHNAME(txn_date) AS month_name, Year(txn_date) as Year, Day(txn_date) as Day, txn_type, txn_amount,      
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			 WHEN txn_type = 'withdrawal' THEN -txn_amount
			 WHEN txn_type = 'purchase' THEN -txn_amount
             ELSE 0 END) over(partition by customer_id, MONTHNAME(txn_date) order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
from customer_transactions;

-- Option 3: data is updated real-time
