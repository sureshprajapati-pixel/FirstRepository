select * 
-- delete 
from MIS.CORE.WEB_PART_LOAN where loan_reference_number in 
(
select loan_reference_number from RING_TRF.TRANSIENT.DSA_COMMISSION_TILL_MAR26

)
;

insert into MIS.CORE.WEB_PART_LOAN
select loan_reference_number,'DSA_COMMISION' as CHANNEL_PARTNER, 0 as rate,amount as COMMISSION_AMOUNT_EXCL_GST from RING_TRF.TRANSIENT.DSA_COMMISSION_TILL_MAR26;
