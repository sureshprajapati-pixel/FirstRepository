-- GET_DDL
SELECT GET_DDL('PROCEDURE', 'MIS.CORE.SP_BI_POS_DPD_DUES_INSTA(TIMESTAMP_NTZ, TIMESTAMP_NTZ)')


-- ACTIVE LOAN COUNT QUERY
select 
    '2026-03-31' as financial_year,count(distinct loan_reference_number) as active_loan_count 
from mis.core.vw_business_mis where month_date='2026-03-01' and pos>0 
-- and ((onbook_ratio>0 and write_off_date is null) or (offbook_ratio=100 and new_npa_date is null and write_off_date is null)); -- OVERALL_ACTIVE_LOAN_COUNT
and onbook_ratio>0 and onbook_ucic_dpd<=150 ; -- ONBOOK_ACTIVE_LOAN_COUNT

