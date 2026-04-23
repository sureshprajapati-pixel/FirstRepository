insert into ring_trf.transient.active_loans

select 
    '2024-12-31' as financial_year,count(distinct loan_reference_number) as active_loan_count 
from mis.core.vw_business_mis where month_date='2024-12-01' and pos>0 
and ((onbook_ratio>0 and write_off_date is null) or (offbook_ratio=100 and new_npa_date is null and write_off_date is null));


select *  from ring_trf.transient.active_loans where financial_year='2025-12-31'; 
