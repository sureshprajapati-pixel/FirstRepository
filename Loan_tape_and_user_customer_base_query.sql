-- account : 1. Registered users till date


with cte as (
select mobile_number,min(user_creation_date) as user_creation_date 
from (
select user_reference_number,mobile_number,date(created_at) as user_creation_date from ring_source.mysql.users where mobile_number is not null and date(created_at)<='2025-12-31'
union all 
select user_reference_number,mobile_number,date(created_at) as user_creation_date from kissht_source.mysql.users where mobile_number is not  null and date(created_at)<='2025-12-31'
)a
group by all 
)

SELECT 
  count(distinct iff(date(user_creation_date)<='2025-12-31', mobile_number,null)) as ason_dec25
from cte 
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
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
)rs
group by all 
)
SELECT
  count(distinct iff(date(system_banking_date)<='2025-12-31', pan_hash,null)) as ason_Dec25,

from cte_1 
group by all 
;



-- accounts  : 3. Total customer served in FY -- Till Dec25


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
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
)rs
group by all 
)
select financial_year,count(1),count(distinct pan_hash) from cte_2 group by all order by 1
;




-- accounts  : 3. Total customer served in 

-- APR25_DEC25

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
    where status<>'CANCELLED' AND DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
    
 UNION ALL 
    select a.loan_reference_number,a.user_reference_number,pd.pan_hash,system_banking_date 
    from mis.core.vw_ring_loans a
    join ring_source.BI.settlements b on a.transaction_reference_number=b.transaction_reference_number and is_settled='DISBURSED'
    LEFT JOIN ring_SOURCE.mysql.pan_data pd  on a.user_reference_number=pd.user_reference_number and is_primary=1
    where status<>'CANCELLED'AND  DATE(coalesce(b.banking_datetime,SETTLEMENT_DATE))<='2025-12-31'
)rs
group by all 
)
select 

count(distinct iff(date(system_banking_date) between '2025-04-01' and '2025-12-31' , pan_hash,null)) as  for_APR25_DEC25

from cte_2 

;



-- accounts  : 4.-- average age of the customers as of Sep24 and Sep25.

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


-- Average age Apr25-Dec25

select 
   
    sum(iff(a.disbursement_date between '2025-04-01' and '2025-12-31',a.age,null))/
    count(distinct iff(a.disbursement_date between '2025-04-01' and '2025-12-31',a.loan_reference_number,null)) as "Apr25-Dec25"
   
from mis.core.loan_tape_FY23_25 a
where date(disbursement_date) between '2025-04-01' and '2025-12-31' 
group by all order by all;




-- accounts  : 5.-- Loan Tape Summary.

-- Top 50 city
create or replace table ring_trf.transient.Apr25_dec25_top_50_city as 
select city_name,count(loan_reference_number) as loan_count,sum(loan_amount) as loan_amount
from mis.core.loan_tape_FY23_25
where date(disbursement_date) between '2025-04-01' and '2025-12-31'
group by city_name order by loan_count desc limit 50;


-- Loan Tape Summary
select        
        date_trunc(month,date(DISBURSEMENT_DATE)) as month_date,
        'APR25-Dec25' as PERIOD,
        CASE
            WHEN IFNULL(dis.age, 0) < 25 THEN '<25 years'
            WHEN IFNULL(dis.age, 0) >= 25 AND IFNULL(dis.age, 0) <= 35 THEN '25 to 35 Years'
            WHEN IFNULL(dis.age, 0) > 35 AND IFNULL(dis.age, 0) <= 45 THEN '36 to 45 years'
            WHEN IFNULL(dis.age, 0) > 45 THEN '>45 Years'
            ELSE 'Other'
        END AS Age_Bucket,
        case 
            when tenure<=2 then 'ST'
            when tenure between 3 and 5 then 'MT'
            when tenure>=6 then 'LT'
            else 'others' 
        end AS tenure_bucket,
        UCIC_REPEAT_TYPE,
        FINANCIER_NAME,
        ONBOOK_RATIO,
        OFFBOOK_RATIO,
        (case
            when cibil_mix is null then 'NTC'
            when cibil_mix < 700 then '<700'
            when cibil_mix between 700 and 760 then '700--760'
            when cibil_mix > 760 then '>760'
            else 'NTC'
        end) as CIBIL_BUCKET,
        CUSTOMER_PROFILE as EMPLOYMENT_TYPE,
        (case 
            when INCOME_BAND is null then 'OTHER'
            when INCOME_BAND >100000 THEN '>100K'
            when INCOME_BAND >=75000 and INCOME_BAND<=100000 THEN '75K to 100K'
            when INCOME_BAND >=50000 and INCOME_BAND<75000 THEN '50K to 75K'
            when INCOME_BAND >=25000 and INCOME_BAND<50000 THEN '25K to 50K'
            when INCOME_BAND >=200 and INCOME_BAND<25000 THEN '200 to 25K'
        END) as CUSTOMER_MIX_INCOME,
        iff(CITY_NAME in (select city_name from ring_trf.transient.Apr25_dec25_top_50_city),'TOP_50_CITIES','OTHER') as CITIS_MIX,
        sum(LOAN_AMOUNT) as LOAN_AMOUNT,
        sum(LOAN_AMOUNT*ONBOOK_RATIO/100) as Onbook_Loan_Amt,   
        sum(LOAN_AMOUNT*OFFBOOK_RATIO/100) as Offboo_Loan_Amt,
        count(loan_reference_number) as LOAN_COUNT
from mis.core.loan_tape_FY23_25 dis where date(DISBURSEMENT_DATE) between  '2025-04-01' and '2025-12-31'
group by all order by all;
;