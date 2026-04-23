SET end_date='2025-12-31';



create table kissht_trf.transient.test_loan_count_raw_data_Dec25 as 
with cte as 
(
select loan_reference_number,system_banking_date,status,loan_closing_date,admin_lock,
case when date(loan_closing_date)>$end_date then null else loan_closing_date end as loan_closed_flag
from mis.core.vw_kissht_loans WHERE system_banking_date<=$end_date AND NVL(PRODUCT,'')<>'LAP'
union all 
select loan_reference_number,system_banking_date,status,loan_closing_date,admin_lock,
case when date(loan_closing_date)>$end_date then null else loan_closing_date end as loan_closed_flag
from mis.core.vw_ring_loans  WHERE system_banking_date<=$end_date AND NVL(PRODUCT,'')<>'LAP'
)
select a.*,b.pos,b.ios from cte a
join mis.core.vw_business_mis b on a.loan_reference_number=b.loan_reference_number and b.month_date=date_trunc(month,date($end_date))
where a.loan_closed_flag is null and a.status<>'CANCELLED' AND 
a.loan_reference_number NOT IN 
( select loan_reference_number from mis.core.daily_npa_wo_master where dt=$end_date AND WRITE_OFF_DATE IS NOT NULL)
;



create or replace table kissht_trf.transient.test_active_loan_raw_data_DEC25 as 
select date_trunc(month,date($end_date)) as month_date,a.*,b.dues_amount as pending_dues
from kissht_trf.transient.TEST_LOAN_COUNT_RAW_DATA_DEC25 a
left join 
(select loan_reference_number,sum(dues_amount) as dues_amount 
from kissht_source.mysql.dues where pending_amount>0 or payment_date>$end_date 
group by all
union
select loan_reference_number,sum(dues_amount) as dues_amount 
from ring_source.mysql.dues where pending_amount>0 or payment_date>$end_date 
group by all
)
b on a.loan_reference_number=b.loan_reference_number
;



select 
    case 
        when pos>0 and ios>0 and pending_dues>0 then 'POS & IOS & DUES_OS'
        when pos>0 and ios>0 and pending_dues=0 then 'POS & IOS'
        when pos>0 and ios=0 and pending_dues>0 then 'POS & DUES_OS'
        when pos=0 and ios>0 and pending_dues>0 then 'POS & DUES_OS'
        when pos>0  then 'POS'
        when ios>0  then 'IOS'
        when pending_dues>0 then 'DUES_OS'
    end as tagging_dec25,
    count(1)
from kissht_trf.transient.test_active_loan_raw_data_dec25 
where admin_lock<>'TRUE'
group by all order by all;



TAGGING_DEC25		COUNT(1)
DUES_OS				423074
IOS					119
POS					1682888
POS & DUES_OS		14342
POS & IOS			53
POS & IOS & DUES_OS	1190446
NULL				1683