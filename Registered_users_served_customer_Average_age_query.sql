-- account : 1. Registered users till date

with cte as (
select mobile_number,min(user_creation_date) as user_creation_date 
from (
select user_reference_number,mobile_number,date(created_at) as user_creation_date from ring_source.mysql.users where mobile_number is not null and date(created_at)<='2025-09-30'
union all 
select user_reference_number,mobile_number,date(created_at) as user_creation_date from kissht_source.mysql.users where mobile_number is not  null and date(created_at)<='2025-09-30'
)a
group by all 
)

SELECT 
  --mobile_number,
  --user_creation_date AS actual_date,
  -- CASE 
  --   WHEN MONTH(user_creation_date) >= 4 THEN TO_CHAR(user_creation_date, 'YYYY') || '-' || TO_CHAR(user_creation_date + INTERVAL '1 year', 'YY')
  --   ELSE TO_CHAR(user_creation_date - INTERVAL '1 year', 'YYYY') || '-' || TO_CHAR(user_creation_date, 'YY')
  -- END AS financial_year,
  -- -- count(1),

  -- date_trunc(month,date(user_creation_date))  as month_date,
  -- count(distinct iff(date(user_creation_date)<='2024-03-31', mobile_number,null)) as ason_mar24,
  -- count(distinct iff(date(user_creation_date)<='2024-06-30', mobile_number,null)) as ason_jun24,
  count(distinct iff(date(user_creation_date)<='2024-09-30', mobile_number,null)) as ason_sep24,
  -- count(distinct iff(date(user_creation_date)<='2024-12-31', mobile_number,null)) as ason_dec24,
  -- count(distinct iff(date(user_creation_date)<='2025-03-31', mobile_number,null)) as ason_mar25,
  -- count(distinct iff(date(user_creation_date)<='2025-06-30', mobile_number,null)) as ason_jun25,
  count(distinct iff(date(user_creation_date)<='2025-09-30', mobile_number,null)) as ason_sep25,
  -- count(distinct iff(date(user_creation_date)<='2025-12-31', mobile_number,null)) as ason_dec25
  from cte 
  -- where date(user_creation_date) between '2024-03-01' and '2025-09-30'
  group by all 
  order by all
;



-- account :2.  Unique customer served till date 

with cte_1 as (
select pan_hash,min(system_banking_date) as system_banking_date from 
(
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_kissht_loans a
    join KISSHT_source.BI.settlements b on a.fb_transaction_id=b.fb_transaction_id and is_settled='DISBURSED'
    LEFT JOIN KISSHT_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1 
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
)rs
group by all 
)
SELECT
  -- CASE 
  --   WHEN MONTH(date) >= 4 THEN TO_CHAR(date, 'YYYY') || '-' || TO_CHAR(date + INTERVAL '1 year', 'YY')
  --   ELSE TO_CHAR(date - INTERVAL '1 year', 'YYYY') || '-' || TO_CHAR(date, 'YY')
  -- END AS financial_year,
  -- -- count(1),
  -- count(distinct pan_hash)


  -- count(distinct iff(date(system_banking_date)<='2024-03-31', pan_hash,null)) as ason_mar24,
  -- count(distinct iff(date(system_banking_date)<='2024-06-30', pan_hash,null)) as ason_jun24,
  count(distinct iff(date(system_banking_date)<='2024-09-30', pan_hash,null)) as ason_sep24,
  -- count(distinct iff(date(system_banking_date)<='2024-12-31', pan_hash,null)) as ason_dec24,
  -- count(distinct iff(date(system_banking_date)<='2025-03-31', pan_hash,null)) as ason_mar25,
  -- count(distinct iff(date(system_banking_date)<='2025-06-30', pan_hash,null)) as ason_jun25,
  count(distinct iff(date(system_banking_date)<='2025-09-30', pan_hash,null)) as ason_sep25,

  
  from cte_1 
  
  group by all 
  ;


-- accounts  : 3. Total customer served in FY
with cte_2 as (
  select pan_hash,
  CASE 
    WHEN MONTH(system_banking_date) >= 4 THEN TO_CHAR(system_banking_date, 'YYYY') || '-' || TO_CHAR(system_banking_date + INTERVAL '1 year', 'YY')
    ELSE TO_CHAR(system_banking_date - INTERVAL '1 year', 'YYYY') || '-' || TO_CHAR(system_banking_date, 'YY')
  END AS financial_year
  
  ,min(system_banking_date) as date from 
(
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_kissht_loans a
    join KISSHT_source.BI.settlements b on a.fb_transaction_id=b.fb_transaction_id and is_settled='DISBURSED'
    LEFT JOIN KISSHT_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1 
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
)rs
group by all 
)
select financial_year,count(1),count(distinct pan_hash) from cte_2 group by all order by 1
;


-- accounts  : 3. Total customer served in 
-- APR24_SEP24
-- APR25_SEP25

with cte_2 as (
  select pan_hash,
  CASE 
    WHEN MONTH(system_banking_date) >= 4 THEN TO_CHAR(system_banking_date, 'YYYY') || '-' || TO_CHAR(system_banking_date + INTERVAL '1 year', 'YY')
    ELSE TO_CHAR(system_banking_date - INTERVAL '1 year', 'YYYY') || '-' || TO_CHAR(system_banking_date, 'YY')
  END AS financial_year
  ,min(system_banking_date) as system_banking_date from 
(
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_kissht_loans a
    join KISSHT_source.BI.settlements b on a.fb_transaction_id=b.fb_transaction_id and is_settled='DISBURSED'
    LEFT JOIN KISSHT_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1 
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-09-30'
)rs
group by all 
)
select 

-- financial_year,count(1),

count(distinct iff(date(system_banking_date) between '2024-04-01' and '2024-09-30' , pan_hash,null)) as  for_APR24_SEP24,
count(distinct iff(date(system_banking_date) between '2025-04-01' and '2025-09-30' , pan_hash,null)) as  for_APR25_SEP25

from cte_2 

-- group by all order by 1
;



-- accounts  : 3.-- average age of the customers as of Sep24 and Sep25.

select 
    date_trunc(month,a.disbursement_date) as month_date,
    sum(a.age) as sum_of_age,
    count(distinct a.loan_reference_number) as loan_count,
    sum(a.age)/count(distinct a.loan_reference_number) as average_age
from mis.core.loan_tape_FY23_25 a
where (disbursement_date between '2024-09-01' and '2024-09-30') or (disbursement_date between '2025-09-01' and '2025-09-30')
group by all order by all;

-- MONTH_DATE	SUM_OF_AGE	LOAN_COUNT	AVERAGE_AGE
-- 2024-09-01	8006996	    252223	    31.745701
-- 2025-09-01	13048681	416407	    31.336363

-- Average age FY22-23, FY23-24, FY24-25, Apr24-Sep24, Apr25-Sep25


select 
    sum(iff(a.disbursement_date between '2022-04-01' and '2023-03-31',a.age,null))/count(distinct iff(a.disbursement_date between '2022-04-01' and '2023-03-31',a.loan_reference_number,null)) as "FY22-23",
    sum(iff(a.disbursement_date between '2023-04-01' and '2024-03-31',a.age,null))/count(distinct iff(a.disbursement_date between '2023-04-01' and '2024-03-31',a.loan_reference_number,null)) as "FY23-24",
    sum(iff(a.disbursement_date between '2024-04-01' and '2025-03-31',a.age,null))/count(distinct iff(a.disbursement_date between '2024-04-01' and '2025-03-31',a.loan_reference_number,null)) as "FY24-25",
    sum(iff(a.disbursement_date between '2024-04-01' and '2024-09-30',a.age,null))/count(distinct iff(a.disbursement_date between '2024-04-01' and '2024-09-30',a.loan_reference_number,null)) as "Apr24-Sep24",
    sum(iff(a.disbursement_date between '2025-04-01' and '2025-09-30',a.age,null))/count(distinct iff(a.disbursement_date between '2025-04-01' and '2025-09-30',a.loan_reference_number,null)) as "Apr25-Sep25",
    sum(iff(a.disbursement_date between '2024-09-01' and '2024-09-30',a.age,null))/count(distinct iff(a.disbursement_date between '2024-09-01' and '2024-09-30',a.loan_reference_number,null)) as "Sep24",
    sum(iff(a.disbursement_date between '2025-09-01' and '2025-09-30',a.age,null))/count(distinct iff(a.disbursement_date between '2025-09-01' and '2025-09-30',a.loan_reference_number,null)) as "Sep25"
from mis.core.loan_tape_FY23_25 a
;

