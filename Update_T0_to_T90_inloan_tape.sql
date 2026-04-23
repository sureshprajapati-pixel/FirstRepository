-- RING
select loan_reference_number,count(1) c from RING_REPORTS.TEMP_TABLES.RING_ALL_LOAN_REPAYMENTS_07012026
WHERE month = 1
group by all 
having c>1
;

-- KISSHT
select loan_reference_number,count(1) c from KISSHT_REPORTS.TEMP_TABLES.KISSHT_ALL_LOAN_REPAYMENTS_07012026
WHERE month = 1
group by all 
having c>1
;


update  mis.core.loan_tape_FY23_25 a 
set 
    a.T_0=NULL,
    a.T_5=NULL,
    a.T_10=NULL,
    a.T_30=NULL,
    a.T_60=NULL,
    a.T_90=NULL
where date(a.DISBURSEMENT_DATE)>='2025-04-01';


update  mis.core.loan_tape_FY23_25 a 
set 
    a.T_0=b.T_0,
    a.T_5=b.T_5,
    a.T_10=b.T_10,
    a.T_30=b.T_30,
    a.T_60=b.T_60,
    a.T_90=b.T_90
-- from RING_REPORTS.ANALYTICS.ALL_REPAYMENTS_DATA_KISSHT_RING_29092025 b 
FROM (select 
    loan_reference_number,
    T_0,
    T_5,
    T_10,
    T_30,
    T_60,
    T_90
from RING_REPORTS.TEMP_TABLES.RING_ALL_LOAN_REPAYMENTS_07012026 
WHERE month = 1
UNION
SELECT 
    loan_reference_number,
    T_0,
    T_5,
    T_10,
    T_30,
    T_60,
    T_90
from KISSHT_REPORTS.TEMP_TABLES.KISSHT_ALL_LOAN_REPAYMENTS_07012026
WHERE month = 1
) b
where a.loan_reference_number=b.loan_reference_number and date(a.DISBURSEMENT_DATE)>='2025-04-01';
