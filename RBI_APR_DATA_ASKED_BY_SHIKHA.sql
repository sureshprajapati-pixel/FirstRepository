select source_reference_number,apr from ring_source.mysql.kfs_data 
union
select  source_reference_number,apr from kissht_source.mysql.kfs_data  limit 10;

select source_reference_number,count(1) 
from ring_source.mysql.kfs_data 
group by source_reference_number having count(1)>1
limit 10;


create or replace table ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data as 
select 
date(lt.disbursement_date) as disbursement_date,
case when date(lt.disbursement_date) between '2024-04-01' and '2025-03-31' then 'FY25'
     when date(lt.disbursement_date) between '2025-04-01' and '2026-03-31' then 'FY26'
end as financier_year,
lt.loan_reference_number,
lt.financier_name,
lt.tenure,
case 
    when lt.tenure<6 then '<6m'
    when lt.tenure between 6 and 11 then '6 to <12m'
    when lt.tenure between 12 and 24 then '12m to =24m'
    when lt.tenure between 25 and 47 then '>24m to <48m'
    when lt.tenure>=48 then '>=48m'
end as tenure_bucket,
lt.interest_rate,
lt.onbook_ratio,
lt.loan_amount,
round(lt.loan_amount*lt.onbook_ratio/100,2) as onbook_loan_amount,
ifnull(kfs.apr,0) as apr
from  mis.core.loan_tape_FY23_25 lt
join (
    select loan_reference_number,transaction_reference_number from ring_source.bi.loans
    union
    select loan_reference_number,fb_transaction_id as transaction_reference_number from kissht_source.bi.loans
)l on lt.loan_reference_number=l.loan_reference_number
left join (
    select source_reference_number,apr from ring_source.mysql.kfs_data 
    union
    select  source_reference_number,apr from kissht_source.mysql.kfs_data
) kfs on l.transaction_reference_number=kfs.source_reference_number

where date(lt.disbursement_date) between '2024-04-01' and '2026-03-31'  and lt.offbook_ratio<>100
;
-- group by all order by all;



select count(1),avg(apr)
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
where financier_name='SICREVA'
and financier_year='FY25' ;
COUNT(1)	AVG(APR)
2690549	77.79716073

select financier_year,--tenure_bucket,
avg(apr)
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
-- where financier_name='SICREVA'
group by all order by 1;


select  APR,count(1)
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data where tenure_bucket='>=48m' group by all;

select *  from RING_REPORTS.TABLEAU.KISSHT_RING_LIFECYCLE_MERGE_BI limit 10;

select count(1),avg(APR) 
from RING_REPORTS.TABLEAU.KISSHT_RING_LIFECYCLE_MERGE_BI 
where date(disbursement_date)  between '2025-04-01' and '202-03-31' and product_name not ilike '%LAP%' and financier='SICREVA' limit 10;

-- COUNT(1)	AVG(APR)
-- 2690674	80.29755500

select 2690674-2690549;

select a.loan_reference_number,a.apr as kfs_data_apr,b.apr from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data a
join RING_REPORTS.TABLEAU.KISSHT_RING_LIFECYCLE_MERGE_BI  b on a.loan_reference_number=b.loan_reference_number
where date(a.disbursement_date)  between '2025-04-01' and '2026-03-31'  and ifnull(a.apr,0)<>ifnull(b.apr,1)
;

select * from ring_source.bi.loans where loan_reference_number='ILOS171112848848QS92';

-- OVERALL
with cte as 
(
select *, ONBOOK_LOAN_AMOUNT*APR as sum_product_APR,
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
)
select financier_year ,tenure_bucket
,sum(sum_product_APR)/sum(ONBOOK_LOAN_AMOUNT)
from cte
where financier_name='SICREVA'
group by all order by 1
;


-- OVERALL
with cte as 
(
select *, ONBOOK_LOAN_AMOUNT*APR as sum_product_APR,
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
)
select financier_year ,tenure_bucket
,sum(sum_product_APR)/sum(ONBOOK_LOAN_AMOUNT)
from cte
-- where financier_name='SICREVA'
group by all order by 1
;

-- SICREVA
with cte as 
(
select *, ONBOOK_LOAN_AMOUNT*APR as sum_product_APR,
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
)
select financier_year ,tenure_bucket
,sum(sum_product_APR)/sum(ONBOOK_LOAN_AMOUNT)
from cte
where financier_name='SICREVA'
group by all order by 1
;


select tenure_bucket,count(loan_reference_number)
from ring_trf.transient.FY25_FY26_lrn_apr_using_kfs_data 
where financier_year='FY25' 
and financier_name<>'SICREVA' 
and interest_rate=30
group by all
;

select 
    tenure,interest_rate,count(1)
from  mis.core.loan_tape_FY23_25 where date(disbursement_date) >= '2024-04-01'
group by all order by all;


select *  FROM MIS.CORE.ALM_KISSHT_MONTHLY where month_date = DATE('2026-03-01');


select * FROM MIS.CORE.ALM_RING_MONTHLY where month_date = DATE('2026-03-01');


create table ring_trf.transient.LRN_apr_data_against_loan_tape as 
select lt.loan_reference_number,kfs.apr
from  mis.core.loan_tape_FY23_25 lt
join (
    select loan_reference_number,transaction_reference_number from ring_source.bi.loans
    union
    select loan_reference_number,fb_transaction_id as transaction_reference_number from kissht_source.bi.loans
)l on lt.loan_reference_number=l.loan_reference_number
left join (
    select source_reference_number,apr from ring_source.mysql.kfs_data 
    union
    select  source_reference_number,apr from kissht_source.mysql.kfs_data
) kfs on l.transaction_reference_number=kfs.source_reference_number
;


select * from ring_source.mysql.kfs_data limit 10;

select iff(apr is null,0 ,1) ,count(1) from ring_trf.transient.LRN_apr_data_against_loan_tape group by 1 limit 10 ;


alter table mis.core.loan_tape_FY23_25 add APR	NUMBER(38,2);

update mis.core.loan_tape_FY23_25 a
set a.apr=b.apr
from ring_trf.transient.LRN_apr_data_against_loan_tape b
where a.loan_reference_number=b.loan_reference_number
;


select
-- 'FY24' as financial_year,
-- interest_rate,
 APR,count(1),
--  sum(loan_amount) as sum_loan_amount,
-- round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,
from mis.core.loan_tape_FY23_25 
where product in ('KISSHT','INSTALOAN') 
and date(disbursement_date) between '2024-04-01' and '2025-03-31' and onbook_ratio>0 and apr is not null
 group by all
 order by apr
;


select
case 
    when date(disbursement_date) between '2025-04-01' and '2025-06-30' then 'FY26-Q1'
    when date(disbursement_date) between '2025-07-01' and '2025-09-30' then 'FY26-Q2'
    when date(disbursement_date) between '2025-10-01' and '2025-12-31' then 'FY26-Q3'
    when date(disbursement_date) between '2026-01-01' and '2026-03-31' then 'FY26-Q4'
end  as financial_year,
tenure,
interest_rate,
 APR,count(1),
 sum(loan_amount) as sum_loan_amount,
round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,
from mis.core.loan_tape_FY23_25 
where product in ('KISSHT','INSTALOAN') 
and date(disbursement_date) between '2025-04-01' and '2026-03-31' and onbook_ratio>0 and apr is not null --limit 10
 group by all 
 order by all
;

select l.transaction_reference_number,mis.loan_reference_number,kd.apr,l.annual_percentage_rate,opf.fa_apr from mis.core.business_mis_lap mis
join ring_source.bi.loans l on mis.loan_reference_number=l.loan_reference_number
left join ring_source.mysql.kfs_data kd on l.transaction_reference_number=kd.source_reference_number
left join ring_reports.leadsquared.vw_opportunity_fields opf on opf.trn_id=l.transaction_reference_number
where month_date='2026-04-01' and date(mis.settlement_date) between '2025-04-01' and '2026-03-31' and mis.onbook_ratio>0  limit 10
order by l.annual_percentage_rate desc
;

select * from ring_reports.leadsquared.vw_opportunity_fields limit 10;


-- select * from ring_source.mysql.kfs_data  where source_reference_number='LAP17519553388161262'

LAP17544828976967688
LAP17519553388161262
LAP17674368595776443
LAP17618854393114496
LAP17484898526832259
LAP17498188867514289
LAP17669829421832439
LAPABTP7HFMODW52TXVA

select * from mis.core.loan_tape_FY23_25 limit 10;

select a.disbursement_date,l.status,t.status,a.loan_reference_number,l.transaction_reference_number --* -- product,count(1) 
from mis.core.loan_tape_FY23_25 a
join ring_source.bi.loans l on a.loan_reference_number=l.loan_reference_number
join ring_source.bi.transactions t on l.transaction_reference_number=t.transaction_reference_number
where date(a.disbursement_date) between '2023-04-01' and '2026-03-31' and a.apr is null and a.product in ('KISSHT','INSTALOAN') 
order by a.disbursement_date desc;



select 
case 
    when date(disbursement_date) between '2023-04-01' and '2024-03-31' then 'FY24'
    when date(disbursement_date) between '2024-04-01' and '2025-03-31' then 'FY25'
    when date(disbursement_date) between '2025-04-01' and '2026-03-31' then 'FY26'
end as FY,
financier_name,
onbook_ratio,
apr,
sum(loan_amount) as sum_loan_amount,
round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,

count(loan_reference_number) as loan_count
from mis.core.loan_tape_FY23_25 
where date(disbursement_date) between '2024-04-01' and '2025-03-31' and apr is not null and product in ('KISSHT','INSTALOAN') and onbook_ratio>0 and apr=98
group by all;

select round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,
from mis.core.loan_tape_FY23_25 
where date(disbursement_date) between '2023-04-01' and '2024-03-31' and apr is not null and product in ('KISSHT','INSTALOAN') and onbook_ratio>0 and apr=447
and financier_name='SICREVA'
-- group by all;
;


1855521720.00/74104426829.96

select distinct financier_name from mis.core.loan_tape_FY23_25 
where date(disbursement_date) between '2023-04-01' and '2026-03-31' and apr is not null and product in ('KISSHT','INSTALOAN') 
;



select 
round(sum(loan_amount*onbook_ratio/100),2) / 63172472095.90*100
from mis.core.loan_tape_FY23_25 
where date(disbursement_date) between '2025-04-01' and '2026-03-31' and apr is not null and product in ('KISSHT','INSTALOAN') and onbook_ratio>0 
-- and financier_name='SICREVA'
-- and apr between 101 and 331
-- and apr between 91 and 331
-- and apr between 81 and 90
and apr between 71 and 80
-- and apr between 61 and 70
-- and apr between 51 and 60
-- and apr between 41 and 50
-- and apr between 31 and 40
-- and apr <=30
;

-- >100% to 331% (Max APR)
-- >90% to <=100%
-- >80% to <=90%
-- >70% to <=80%
-- >60% to <=70%
-- >50% to <=60%
-- >40% to <=50%
-- >30% to <=40%
-- <=30%


-- using kfs_data APR
select
case 
    when date(disbursement_date) between '2025-04-01' and '2025-06-30' then 'FY26-Q1'
    when date(disbursement_date) between '2025-07-01' and '2025-09-30' then 'FY26-Q2'
    when date(disbursement_date) between '2025-10-01' and '2025-12-31' then 'FY26-Q3'
    when date(disbursement_date) between '2026-01-01' and '2026-03-31' then 'FY26-Q4'
end  as financial_year,
tenure,
interest_rate,
 APR,count(1),
 sum(loan_amount) as sum_loan_amount,
round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,
from mis.core.loan_tape_FY23_25 
where product in ('KISSHT','INSTALOAN') 
and date(disbursement_date) between '2025-04-01' and '2026-03-31' and onbook_ratio>0 and apr is not null --limit 10
 group by all 
 order by all
;


-- using TRANSACTION_APR
select
case 
    when date(disbursement_date) between '2025-04-01' and '2025-06-30' then 'FY26-Q1'
    when date(disbursement_date) between '2025-07-01' and '2025-09-30' then 'FY26-Q2'
    when date(disbursement_date) between '2025-10-01' and '2025-12-31' then 'FY26-Q3'
    when date(disbursement_date) between '2026-01-01' and '2026-03-31' then 'FY26-Q4'
end  as financial_year,
tenure,
interest_rate,
 transactionapr,count(1),
 sum(loan_amount) as sum_loan_amount,
round(sum(loan_amount*onbook_ratio/100),2) as sum_onbook_loan_amount,
from ring_trf.transient.FY_25_26_with_TRANSACTIONAPR
where product in ('KISSHT','INSTALOAN') 
and date(disbursement_date) between '2025-04-01' and '2026-03-31' and onbook_ratio>0 and apr is not null --limit 10
 group by all 
 order by all
;
