-- Serial Number 

-- Prospect code / Application Number 

-- Customer ID / UCIC 

-- Nature of loan 

-- Login date 

-- Sanction date 

-- Sanction amount 

-- Disbursement date 

-- Total disbursement Amount 

-- Mode of payment 

-- ROI 

-- Processing Fees 

-- Bureau Score 

-- Term of loan / Tenure 

-- EMI amount 

-- EMI Due Date 

-- Loan status (closed/ongoing) 

-- Type of product(long term\/short term,etc) 

-- NACH status( registered/rejected/active/inactive) 

-- Loan closing date 

-- Special status (foreclosed/closed/settled) 

-- Financer 

-- KYC Tag (VCIP / Non-VCIP) 


create or replace table ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 as 
-- INSTALOAN
select 
    -- Serial Number ,
    'INSTALOAN' as platform,
    m.loan_reference_number as Application_Number,
    m.pan_hash as Customer_ID,
    m.user_reference_number,
    ifnull(l.product,'PL') as Nature_of_loan,
    date(l.created_at) as Login_date,
    date(m.settlement_date) as Sanction_date,
    m.loan_amount as Sanction_amount,
    date(m.settlement_date) as Disbursement_date,
    st.SETTLEMENT_AMOUNT as Total_disbursement_Amount,
    'ONLINE' as Mode_of_payment,
    l.interest as ROI, 
    t.pf_final_amount as Processing_Fees,
    null as Bureau_Score, 
    l.INSTALMENT_NO_MONTHS as Tenure,
    l.INSTALMENT_AMOUNT as EMI_amount,
    l.original_first_emi_date as EMI_Due_Date,
    l.status as Loan_status,
    iff(l.INSTALMENT_NO_MONTHS<6,'SHORT_TERM','LONG_TERM') as Type_of_product,
    null as NACH_status, -- ( registered/rejected/active/inactive) 
    l.loan_closing_date as Loan_closing_date,
    l.SPECIAL_STATUS,
    m.FINANCIER, 
    null as KYC_Tag -- (VCIP / Non-VCIP) 

from MIS.CORE.business_mis_ring_instaloan m
join ring_source.bi.loans l on m.loan_reference_number=l.loan_reference_number 
left join ring_source.bi.transactions t on l.transaction_reference_number=t.transaction_reference_number
left join ring_source.bi.settlements st on l.transaction_reference_number=st.transaction_reference_number
where month_date='2025-10-01' and date(m.settlement_date) between '2025-07-01' and '2025-09-30'
and m.status<>'CANCELLED'

union all

-- KISSHT
select 
   -- Serial Number ,
    'KISSHT' as platform,
    m.loan_reference_number as Application_Number,
    m.pan_hash as Customer_ID,
    m.user_reference_number,
    ifnull(l.product,'PL') as Nature_of_loan,
    date(l.created_at) as Login_date,
    date(m.settlement_date) as Sanction_date,
    m.loan_amount as Sanction_amount,
    date(m.settlement_date) as Disbursement_date,
    st.SETTLEMENT_AMOUNT as Total_disbursement_Amount,
    'ONLINE' as Mode_of_payment,
    l.interest as ROI,
    t.pf_final_amount as Processing_Fees,
    null as Bureau_Score, 
    l.INSTALMENT_NO_MONTHS as Tenure,
    l.INSTALMENT_AMOUNT as EMI_amount,
    l.original_first_emi_date as EMI_Due_Date,
    l.status as Loan_status,
    iff(l.INSTALMENT_NO_MONTHS<6,'SHORT_TERM','LONG_TERM') as Type_of_product,
    null as NACH_status, -- ( registered/rejected/active/inactive) 
    l.loan_closing_date as Loan_closing_date,
    l.SPECIAL_STATUS,
    m.FINANCIER, 
    null as KYC_Tag -- (VCIP / Non-VCIP) 
    
from MIS.CORE.business_mis_kissht m
join kissht_source.bi.loans l on m.loan_reference_number=l.loan_reference_number
left join kissht_source.bi.transactions t on l.fb_transaction_id=t.fb_transaction_id
left join kissht_source.bi.settlements st on l.fb_transaction_id=st.fb_transaction_id
where month_date='2025-10-01' and date(m.settlement_date) between '2025-07-01' and '2025-09-30' 
and m.loan_status<>'CANCELLED';


-- BUREAU_SCORE
create or replace table ring_trf.transient.bureau_score_details_for_data as 
select 
    t.user_reference_number,
    split_part(SUBSTRING(listagg(c1.score,',') within group(ORDER BY c1.created_at DESC), 1, 5),',',1) as Bureau_score 
from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 t
JOIN
(select user_reference_number,score,created_at from (
select ROW_NUMBER() OVER (PARTITION BY user_reference_number ORDER BY created_at desc) AS row_num,
b.* from KISSHT_SOURCE.MYSQL.BUREAU_DATA b
) where row_num<100) c1 ON t.user_reference_number = c1.user_reference_number and c1.created_at<=t.Disbursement_date
group by t.user_reference_number

union

select 
    t.user_reference_number,
    split_part(SUBSTRING(listagg(c1.score,',') within group(ORDER BY c1.created_at DESC), 1, 5),',',1) as Bureau_score 
from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 t
join (select user_reference_number,score,created_at from (
select ROW_NUMBER() OVER (PARTITION BY user_reference_number ORDER BY created_at desc) AS row_num,
b.* from RING_SOURCE.MYSQL.BUREAU_DATA b
) where row_num<100) c1 ON t.user_reference_number = c1.user_reference_number and c1.created_at<=t.Disbursement_date
group by t.user_reference_number
;


update ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
set a.BUREAU_SCORE=b.BUREAU_SCORE
from ring_trf.transient.bureau_score_details_for_data b 
where a.user_reference_number=b.user_reference_number;

-- KYC_Tag
select * from kissht_source.mysql.video_kyc_details where STATUS='VKYC_COMPLETED' limit 10;

select * from kissht_source.mysql.kyc_events limit 10;
select distinct source from kissht_source.mysql.kyc_audit_trail limit 10;


select a.user_reference_number,any_value(iff(b.STATUS='VKYC_COMPLETED','VCIP','NON-VCIP')) as KYC_Tag
from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
left join kissht_source.mysql.video_kyc_details b on a.user_reference_number=b.user_reference_number
group by a.user_reference_number
; 

update ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
set a.KYC_Tag=b.KYC_Tag
from (
    select a.user_reference_number,any_value(iff(b.STATUS='VKYC_COMPLETED','VCIP','NON-VCIP')) as KYC_Tag
    from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
    left join kissht_source.mysql.video_kyc_details b on a.user_reference_number=b.user_reference_number
    group by a.user_reference_number
    union
    select a.user_reference_number,any_value(iff(b.STATUS='VKYC_COMPLETED','VCIP','NON-VCIP')) as KYC_Tag
    from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
    left join ring_source.mysql.video_kyc_details b on a.user_reference_number=b.user_reference_number
    group by a.user_reference_number
) b 
where a.user_reference_number=b.user_reference_number;

select * from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 where KYC_Tag is null;



select * from ring_source.mysql.video_kyc_details

-- NACH_status
update ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
set a.NACH_status=b.NACH_status
from (
    select a.Application_Number,n.status as NACH_status
    from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
    join ring_source.bi.loans l on l.loan_reference_number=a.Application_Number
    left join ring_source.mysql.mandate_details n on l.nach_reference_number=n.MANDATE_REFERENCE_NUMBER
    union
    select a.Application_Number,n.status as NACH_status
    from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 a
    join kissht_source.bi.loans l on l.loan_reference_number=a.Application_Number
    left join kissht_source.mysql.nach_details n on l.nach_reference_number=n.nach_reference_number
) b 
where a.Application_Number=b.Application_Number;

select * from ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 where NACH_status is null;

-- Final Data shared
select 
    SERIAL_NUMBER,
    PLATFORM,
    APPLICATION_NUMBER,
    CUSTOMER_ID,
    NATURE_OF_LOAN,
    LOGIN_DATE,
    SANCTION_DATE,
    SANCTION_AMOUNT,
    DISBURSEMENT_DATE,
    TOTAL_DISBURSEMENT_AMOUNT,
    MODE_OF_PAYMENT,
    ROI,
    PROCESSING_FEES,
    BUREAU_SCORE,
    TENURE,
    EMI_AMOUNT,
    EMI_DUE_DATE,
    LOAN_STATUS,
    TYPE_OF_PRODUCT,
    NACH_STATUS,
    LOAN_CLOSING_DATE,
    SPECIAL_STATUS,
    FINANCIER,
    KYC_TAG
from (
SELECT 
    ROW_NUMBER() OVER (ORDER BY DISBURSEMENT_DATE) AS SERIAL_NUMBER,
    *
FROM ring_trf.transient.report_52_Loan_Tape_required_Jul25_to_Sep25 
where PLATFORM='INSTALOAN' -- 'KISSHT'
) order by SERIAL_NUMBER;






-- ================================== DEC25_&_FEB26 ==================================

create or replace table ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 as 
-- INSTALOAN
select 
    -- Serial Number ,
    'INSTALOAN' as platform,
    m.loan_reference_number as Application_Number,
    m.pan_hash as Customer_ID,
    m.user_reference_number,
    ifnull(l.product,'PL') as Nature_of_loan,
    date(l.created_at) as Login_date,
    date(m.settlement_date) as Sanction_date,
    m.loan_amount as Sanction_amount,
    date(m.settlement_date) as Disbursement_date,
    st.SETTLEMENT_AMOUNT as Total_disbursement_Amount,
    'ONLINE' as Mode_of_payment,
    l.interest as ROI, 
    t.pf_final_amount as Processing_Fees,
    null as BUREAU_SCORE_AT_LOAN_TIME, 
    l.INSTALMENT_NO_MONTHS as Tenure,
    l.INSTALMENT_AMOUNT as EMI_amount,
    l.original_first_emi_date as EMI_Due_Date,
    l.status as Loan_status,
    iff(l.INSTALMENT_NO_MONTHS<6,'SHORT_TERM','LONG_TERM') as Type_of_product,
    null as NACH_status, -- ( registered/rejected/active/inactive) 
    l.loan_closing_date as Loan_closing_date,
    l.SPECIAL_STATUS,
    m.FINANCIER, 
    null as KYC_Tag -- (VCIP / Non-VCIP) 

from MIS.CORE.business_mis_ring_instaloan m
join ring_source.bi.loans l on m.loan_reference_number=l.loan_reference_number 
left join ring_source.bi.transactions t on l.transaction_reference_number=t.transaction_reference_number
left join ring_source.mysql.vw_settlements st on l.transaction_reference_number=st.transaction_reference_number
where month_date='2026-02-01' and date(m.settlement_date) between '2025-12-01' and '2026-02-28' 
and m.status<>'CANCELLED'

union all

-- KISSHT
select 
   -- Serial Number ,
    'KISSHT' as platform,
    m.loan_reference_number as Application_Number,
    m.pan_hash as Customer_ID,
    m.user_reference_number,
    ifnull(l.product,'PL') as Nature_of_loan,
    date(l.created_at) as Login_date,
    date(m.settlement_date) as Sanction_date,
    m.loan_amount as Sanction_amount,
    date(m.settlement_date) as Disbursement_date,
    st.SETTLEMENT_AMOUNT as Total_disbursement_Amount,
    'ONLINE' as Mode_of_payment,
    l.interest as ROI,
    t.pf_final_amount as Processing_Fees,
    null as BUREAU_SCORE_AT_LOAN_TIME, 
    l.INSTALMENT_NO_MONTHS as Tenure,
    l.INSTALMENT_AMOUNT as EMI_amount,
    l.original_first_emi_date as EMI_Due_Date,
    l.status as Loan_status,
    iff(l.INSTALMENT_NO_MONTHS<6,'SHORT_TERM','LONG_TERM') as Type_of_product,
    null as NACH_status, -- ( registered/rejected/active/inactive) 
    l.loan_closing_date as Loan_closing_date,
    l.SPECIAL_STATUS,
    m.FINANCIER, 
    null as KYC_Tag -- (VCIP / Non-VCIP) 
    
from MIS.CORE.business_mis_kissht m
join kissht_source.bi.loans l on m.loan_reference_number=l.loan_reference_number
left join kissht_source.bi.transactions t on l.fb_transaction_id=t.fb_transaction_id
left join kissht_source.bi.settlements st on l.fb_transaction_id=st.fb_transaction_id
where month_date='2026-02-01' and date(m.settlement_date) between '2025-12-01' and '2026-02-28' 
and m.loan_status<>'CANCELLED';




-- BUREAU_SCORE
create or replace table ring_trf.transient.bureau_score_details_for_DEC25_FEB26_data as 
select 
    t.user_reference_number,
    split_part(SUBSTRING(listagg(c1.score,',') within group(ORDER BY c1.created_at DESC), 1, 5),',',1) as Bureau_score 
from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 t
JOIN
(select user_reference_number,score,created_at 
from 
(
    select ROW_NUMBER() OVER (PARTITION BY user_reference_number ORDER BY created_at desc) AS row_num,
    b.* from 
        (
            select user_reference_number , score, created_at,request_status from sicreva.mysql.bureau_data 
            union 
            select user_reference_number , score,created_at,request_status from sicreva.mysql.bureau_data_kissht
        )
    b where request_status='SUCCESS'
) where row_num<100
) c1 ON t.user_reference_number = c1.user_reference_number and date(c1.created_at)<=date(t.Disbursement_date)
group by t.user_reference_number

union

select 
    t.user_reference_number,
    split_part(SUBSTRING(listagg(c1.score,',') within group(ORDER BY c1.created_at DESC), 1, 5),',',1) as Bureau_score 
from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 t
join (select user_reference_number,score,created_at from (
select ROW_NUMBER() OVER (PARTITION BY user_reference_number ORDER BY created_at desc) AS row_num,
b.* from sicreva.mysql.bureau_data b where request_status='SUCCESS'
) where row_num<100) c1 ON t.user_reference_number = c1.user_reference_number and date(c1.created_at)<=date(t.Disbursement_date)
group by t.user_reference_number
;


SELECT * FROM ring_trf.transient.bureau_score_details_for_OCT25_NOV25_data LIMIT 10;

update ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
set a.BUREAU_SCORE_AT_LOAN_TIME=b.BUREAU_SCORE
from ring_trf.transient.bureau_score_details_for_DEC25_FEB26_data b 
where a.user_reference_number=b.user_reference_number;

-- 1215947
select * from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 where BUREAU_SCORE_AT_LOAN_TIME is null;


update ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
set a.BUREAU_SCORE_AT_LOAN_TIME=b.cibil_mix
from mis.core.loan_tape_fy23_25 b 
where a.application_number=b.loan_reference_number and a.BUREAU_SCORE_AT_LOAN_TIME is null;


-- KYC_Tag
update ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
set a.KYC_Tag=b.KYC_Tag
from (
    select a.user_reference_number,any_value(iff(b.STATUS='VKYC_COMPLETED','VCIP','NON-VCIP')) as KYC_Tag
    from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
    left join kissht_source.mysql.video_kyc_details b on a.user_reference_number=b.user_reference_number
    group by a.user_reference_number
    union
    select a.user_reference_number,any_value(iff(b.STATUS='VKYC_COMPLETED','VCIP','NON-VCIP')) as KYC_Tag
    from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
    left join ring_source.mysql.video_kyc_details b on a.user_reference_number=b.user_reference_number
    group by a.user_reference_number
) b 
where a.user_reference_number=b.user_reference_number;



-- NACH_status
update ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
set a.NACH_status=b.NACH_status
from (
    select a.Application_Number,n.status as NACH_status
    from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
    join ring_source.bi.loans l on l.loan_reference_number=a.Application_Number
    left join ring_source.mysql.mandate_details n on l.nach_reference_number=n.MANDATE_REFERENCE_NUMBER
    union
    select a.Application_Number,n.status as NACH_status
    from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 a
    join kissht_source.bi.loans l on l.loan_reference_number=a.Application_Number
    left join kissht_source.mysql.nach_details n on l.nach_reference_number=n.nach_reference_number
) b 
where a.Application_Number=b.Application_Number;


select 
    PLATFORM,
    APPLICATION_NUMBER,
    CUSTOMER_ID as UCIC,
    NATURE_OF_LOAN,
    LOGIN_DATE,
    SANCTION_DATE,
    SANCTION_AMOUNT,
    DISBURSEMENT_DATE,
    TOTAL_DISBURSEMENT_AMOUNT,
    MODE_OF_PAYMENT,
    ROI,
    PROCESSING_FEES,
    BUREAU_SCORE_AT_LOAN_TIME,
    TENURE,
    EMI_AMOUNT,
    EMI_DUE_DATE,
    LOAN_STATUS,
    TYPE_OF_PRODUCT,
    NACH_STATUS,
    LOAN_CLOSING_DATE,
    SPECIAL_STATUS,
    FINANCIER,
    KYC_TAG as KYC_TYPE
from ring_trf.transient.report_652_Loan_Tape_required_Dec25_to_Feb26 
where PLATFORM='KISSHT'  -- 'INSTALOAN'
order by SANCTION_DATE;