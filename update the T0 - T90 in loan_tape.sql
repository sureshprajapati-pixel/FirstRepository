-- Prajay Gujarathi
--   8:58 PM
-- All Repayment Flags all loans for Kissht and Ring @Roshankumar Sharma
-- RING_REPORTS.TEMP_TABLES.RING_ALL_LOAN_REPAYMENTS_07012026
-- KISSHT_REPORTS.TEMP_TABLES.KISSHT_ALL_LOAN_REPAYMENTS_07012026
-- cc @Himanshu Patel
-- Roshankumar Sharma
--   12:06 PM
-- @Suresh Prajapati pls update the T0 - T90 from Oct'25 onwards in the loan_tape table created for IR team using Above table shared by @Prajay Gujarathi.
-- cc @Sagar Shah @Durgesh Shukla @Nilam Kadam

update mis.core.loan_tape_FY23_25 a
set a.T_0=b.T_0,
    a.T_5=b.T_5,
    a.T_10=b.T_10,
    a.T_30=b.T_30,
    a.T_60=b.T_60,
    a.T_90=b.T_90
from (
select loan_reference_number,T_0,T_5,T_10,T_30,T_60,T_90 from RING_REPORTS.TEMP_TABLES.RING_ALL_LOAN_REPAYMENTS_07012026
union
select loan_reference_number,T_0,T_5,T_10,T_30,T_60,T_90 from KISSHT_REPORTS.TEMP_TABLES.KISSHT_ALL_LOAN_REPAYMENTS_07012026
) b
where a.loan_reference_number=b.loan_reference_number
and a.disbursement_date>='2025-10-01'
;