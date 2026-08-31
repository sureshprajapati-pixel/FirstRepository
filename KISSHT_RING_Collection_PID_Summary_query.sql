-- KISSHT_RING_Collection_PID_Summary_query


-- Kissht
select
    'KISSHT' as system,
    date_trunc(month, date(coalesce(pm.created_at,p.banking_datetime,p.payment_receipt_date))) as month_date, 
    f.financier_code as financier_name,
    pm.mapping_type,
    d.dues_type,
    sum(iff(direction='REVERSE',-pm.applied_towards_principal,pm.applied_towards_principal)) as p,
    sum(iff(direction='REVERSE',-pm.applied_towards_interest,pm.applied_towards_interest)) as i,
    sum(iff(direction='REVERSE',-pm.applied_towards_dues,pm.applied_towards_dues)) as d
from kissht_source.bi.payment_mapping pm
join kissht_source.bi.payments p on pm.payment_reference_number=p.payment_reference_number
left join kissht_source.mysql.dues d on pm.mapping_type_reference_number=d.dues_reference_number
left join kissht_source.bi.loans l on l.loan_reference_number=pm.loan_reference_number
left join kissht_source.mysql.financier f on l.financier_reference_number=f.financier_reference_number
where date(coalesce(pm.created_at,p.banking_datetime,p.payment_receipt_date))  between '2026-04-01' and '2026-04-30'
and pm.direction='FORWARD' and pm.is_reversed=0
and p.payment_receipt_mode<>'NOTIONAL'
-- and d.dues_type<>'PROCESSING_FEES'
group by all

union

-- Ring
select 
    iff(ifnull(l.product,'PL')='LAP','LAP','RING') as system,
    date_trunc(month, date(coalesce(pm.created_at,p.banking_datetime,p.payment_receipt_date))) as month_date, 
    f.financier_code as financier_name,
    pm.mapping_type,
    d.dues_type,
    sum(iff(direction='REVERSE',-pm.applied_towards_principal,pm.applied_towards_principal)) as p,
    sum(iff(direction='REVERSE',-pm.applied_towards_interest,pm.applied_towards_interest)) as i,
    sum(iff(direction='REVERSE',-pm.applied_towards_dues,pm.applied_towards_dues)) as d
from ring_source.bi.payment_mapping pm
join ring_source.bi.payments p on pm.payment_reference_number=p.payment_reference_number
left join ring_source.mysql.dues d on pm.mapping_type_reference_number=d.dues_reference_number
left join ring_source.bi.loans l on l.loan_reference_number=pm.loan_reference_number
left join ring_source.mysql.financiers f on l.financier_reference_number=f.financier_reference_number
where date(coalesce(pm.created_at,p.banking_datetime,p.payment_receipt_date))  between '2026-04-01' and '2026-04-30'
and pm.direction='FORWARD' and pm.is_reversed=0
and p.payment_receipt_mode<>'NOTIONAL'
-- and ifnull(l.product,'NA')<>'LAP'
-- and d.dues_type<>'PROCESSING_FEES'
group by all

order by system,month_date,financier_name,mapping_type desc,dues_type 
;
