--524 AUM RAW DATA
with cte as 
(
     select *
    from mis.core.daily_npa_wo_master where dt='{to_date}' 
    and (new_npa_date<='{to_date}' or new_npa_date is null)
),
ever_dpd as (
select loan_reference_number,min(month_date) as ever_90_dt
from mis.core.business_mis_kissht mis
where month_date <= '{from_date}'
and dpd_number > 90
group by 1

union all

select loan_reference_number,min(month_date) as ever_90_dt
from mis.core.business_mis_ring_instaloan mis
where month_date <= '{from_date}'
and dpd_number > 90
group by 1

union all

select loan_reference_number,min(month_date) as ever_90_dt
from mis.core.business_mis_ring_txn_credit mis
where month_date <= '{from_date}'
and dpd_number > 90
group by 1

union all

select loan_reference_number,min(month_date) as ever_90_dt
from mis.core.business_mis_lap mis
where month_date <= '{from_date}'
and dpd_number > 90
group by 1
),
lender_details as
(
select lh.loan_reference_number,last_day(lh.allocation_date) as allocation_date,max_by(dp.name,lh.allocation_date) as lender_name
FROM  ring_source.mysql.loan_hypothecations lh
join ring_source.mysql.debt_partners dp on lh.debt_partner_reference_number = dp.debt_partner_reference_number
where date_trunc(month,lh.allocation_date)='{from_date}'
group by 1,2
union
select lh.loan_reference_number,last_day(lh.allocation_date) as allocation_date,max_by(dp.name,lh.allocation_date) as lender_name
FROM  kissht_source.mysql.loan_hypothecations lh
join kissht_source.mysql.debt_partners dp on lh.debt_partner_reference_number = dp.debt_partner_reference_number
where date_trunc(month,lh.allocation_date)='{from_date}'
group by 1,2
) 
-- ======================
--         KISSHT
-- ======================
select 
'{from_date}'  as month_date,
date(trunc(IFNULL(mis.settlement_date,MIS.CREATED_DATE),'MONTH')) as Disbursement_month,
DATE(IFNULL(mis.settlement_date,MIS.CREATED_DATE)) AS settlement_date,
'KISSHT' as Product,
mis.financier,
l.loan_reference_number,
mis.user_reference_number,
ifnull(l.is_colending,0) as is_colending,
l.instalment_no_months,
l.interest,
null as billing_start_date,
null as billing_end_date,
date(l.original_first_emi_date) as loan_maturity_date,
case 
    when l.instalment_no_months < 6 then '< 6M'
    when l.instalment_no_months >= 6 AND l.instalment_no_months < 12 then '6M - 11M'
    when l.instalment_no_months >=12 then '>= 12M'
end as product_split,
case 
    when mis.UCIC_MAX_DPD <= 0 then 'ONGOING'
    when mis.UCIC_MAX_DPD between 1 and 30 then '1--30'
    when mis.UCIC_MAX_DPD between 31 and 60 then '31-60'
    when mis.UCIC_MAX_DPD between 61 and 85 then '61-85'
    when mis.UCIC_MAX_DPD between 86 and 90 then '86-90'
    when mis.UCIC_MAX_DPD between 91 and 120 then '91-120'
    when mis.UCIC_MAX_DPD between 121 and 150 then '121-150'
    when mis.UCIC_MAX_DPD between 151 and 180 then '151-180'
    -- when mis.UCIC_MAX_DPD > 180 then '180+'
    when mis.UCIC_MAX_DPD between 181 and 270 then '181-270'
    when mis.UCIC_MAX_DPD between 271 and 365 then '271-365'
    when mis.UCIC_MAX_DPD > 365 then '365+'
    else null
end as DPD_Split,
mis.loan_amount,
mis.pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.05
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.1
        -- when mis.financier in ('POONAWALA','SICREVA') then mis.pos
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.2
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.1
        -- when mis.financier='PFL' then mis.pos*0.05
        -- else 0
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
,2) as Onbook_pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.95
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.9
        -- when mis.financier in ('POONAWALA','SICREVA') then 0
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.8
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.9
        -- when mis.financier='PFL' then mis.pos*0.95 
        -- else mis.pos
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
,2) as Offbook_pos,

mis.dpd_number as dpd_og,
mis.UCIC_MAX_DPD,
ifnull(mis.repeat_type,'Fresh') as repeat_type,
t.NEW_NPA_DATE,
t.WRITE_OFF_DATE,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount
,sd.securitisation_tagging,
mis.pan_hash as ucic,
l.interest as interest_rate,
case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
    when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd>150 then 'WRITE_OFF'--add on 19mar-krishna

    -- when mis.UCIC_MAX_DPD > 180 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging --After March_2024 

-- ,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING
,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING


,mis.OFFBOOK_WRITE_OFF_DATE

,sm.state_name
,mis.ucic_repeat_type
,iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd
,lds.lender_name
,air.TOTAL_ONBOOK_ACCRUED_INTEREST
,apr.ACCUMULATED_PROCESSING_FEES
,apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,r.last_emi_date
,apr.CGFEES
,apr.RISKPREMIUM
,apr.CGFEES_RISKPREMIUM_WITH_GST_9
,apr.ACCUMULATED_GC_FEE

from mis.core.business_mis_kissht mis
-- ring_trf.transient.mis_pos_dpd_dues_kissht mis
join KISSHT_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}')) 
  and mis.pos>0 
left join kissht_source.bi.transactions ta on l.fb_transaction_id=ta.fb_transaction_id
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.fldg_data as fd on fd.loan_reference_number=mis.loan_reference_number
left join mis.core.USER_FRAUD_DATA uf on l.loan_reference_number=uf.loan_reference_number and uf.fraud_date<='{to_date}' 
left join mis.core.securitisation_data sd on l.loan_reference_number=sd.loan_reference_number
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
LEFT JOIN KISSHT_SOURCE.MYSQL.USER_ADDRESS AS ua ON ta.present_address_reference_number=ua.address_reference_number
-- coalesce(u.present_address_reference_number,u.CUSTOMER_COMMUNICATION_ADDRESS_REFERENCE_NUMBER) = ua.address_reference_number
LEFT JOIN RING_SOURCE.mysql.PINCODE_MASTER pn ON pn.pincode = ua.pincode
LEFT JOIN RING_SOURCE.MYSQL.CITY_MASTER AS cm ON cm.city_id = pn.city_id
LEFT JOIN RING_SOURCE.MYSQL.STATE_MASTER AS sm ON sm.state_id = cm.state_id
LEFT JOIN lender_details lds on mis.loan_reference_number=lds.loan_reference_number and mis.month_date=date_trunc(month,date(lds.allocation_date)) 
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))
left join (
    select  m.loan_reference_number,max(scheduled_payment_date) as last_emi_date
    from mis.core.business_mis_kissht m
    join kissht_source.bi.repayments r on m.loan_reference_number=r.loan_reference_number and  m.month_date=date_trunc(month,date('{from_date}'))
    group by 1
) r on mis.loan_reference_number=r.loan_reference_number

union all

-- ======================
-- Ring TXN_CREDIT
-- ======================

select 
'{from_date}'  as month_date,
date(trunc(mis.created_at,'MONTH')) as Disbursement_month,
date(mis.created_at) as settlement_date,
'TXN_CREDIT' as Product,
mis.financier_name,
l.loan_reference_number,
mis.user_reference_number,
ifnull(l.is_colending,0) as is_colending,
l.instalment_no_months,
l.interest,
l.billing_start_date,
l.billing_end_date,
date(l.original_first_emi_date) as loan_maturity_date,
case 
    when l.instalment_no_months < 6 then '< 6M'
    when l.instalment_no_months >= 6 AND l.instalment_no_months < 12 then '6M - 11M'
    when l.instalment_no_months >=12 then '>= 12M'
end as product_split,
case 
    when mis.UCIC_MAX_DPD <= 0 then 'ONGOING'
    when mis.UCIC_MAX_DPD between 1 and 30 then '1--30'
    when mis.UCIC_MAX_DPD between 31 and 60 then '31-60'
    when mis.UCIC_MAX_DPD between 61 and 85 then '61-85'
    when mis.UCIC_MAX_DPD between 86 and 90 then '86-90'
    when mis.UCIC_MAX_DPD between 91 and 120 then '91-120'
    when mis.UCIC_MAX_DPD between 121 and 150 then '121-150'
    when mis.UCIC_MAX_DPD between 151 and 180 then '151-180'
    -- when mis.UCIC_MAX_DPD > 180 then '180+'
    when mis.UCIC_MAX_DPD between 181 and 270 then '181-270'
    when mis.UCIC_MAX_DPD between 271 and 365 then '271-365'
    when mis.UCIC_MAX_DPD > 365 then '365+'
    else null
end as DPD_Split,
mis.accumulated_loan_amount,
mis.pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.05
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.1
        -- when mis.financier_name in ('POONAWALA','SICREVA') then mis.pos
        -- WHEN (mis.financier_name='PCHFL' and date(mis.created_at)>'2024-09-30') then mis.pos*0.2
        -- WHEN (mis.financier_name='PCHFL' and date(mis.created_at)<='2024-09-30') or (mis.financier_name='MAS' and l.is_colending=1) then mis.pos*0.1
        -- when mis.financier_name='PFL' then mis.pos*0.05
        -- else 0
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
,2) as Onbook_pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.95
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.9
        -- when mis.financier_name in ('POONAWALA','SICREVA') then 0
        -- WHEN (mis.financier_name='PCHFL' and date(mis.created_at)>'2024-09-30') then mis.pos*0.8
        -- WHEN (mis.financier_name='PCHFL' and date(mis.created_at)<='2024-09-30') or (mis.financier_name='MAS' and l.is_colending=1) then mis.pos*0.9
        -- when mis.financier_name='PFL' then mis.pos*0.95 
        -- else mis.pos
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
,2) as Offbook_pos,

mis.dpd_number as dpd_og,
mis.UCIC_MAX_DPD,
ifnull(mis.repeat_type,'Fresh') as repeat_type,
-- iff(ifnull(mis.repeat_type,0)=0,'Fresh','Repeat') as repeat_type,

t.NEW_NPA_DATE,
t.WRITE_OFF_DATE,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging,
mis.pan_hash as ucic,
l.interest as interest_rate,
case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
    when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd>150 then 'WRITE_OFF'
    -- when mis.UCIC_MAX_DPD > 180 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging --After March_2024 

-- ,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING
,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING

,mis.OFFBOOK_WRITE_OFF_DATE

,SM.STATE_NAME
,mis.ucic_repeat_type
,iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd
,lds.lender_name
,air.TOTAL_ONBOOK_ACCRUED_INTEREST
,apr.ACCUMULATED_PROCESSING_FEES
,apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,r.last_emi_date
,apr.CGFEES
,apr.RISKPREMIUM
,apr.CGFEES_RISKPREMIUM_WITH_GST_9
,apr.ACCUMULATED_GC_FEE

from mis.core.business_mis_ring_txn_credit mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}' ))  
and mis.pos>0
left JOIN RING_SOURCE.BI.TRANSACTIONS ta ON L.TRANSACTION_REFERENCE_NUMBER=TA.TRANSACTION_REFERENCE_NUMBER
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.fldg_data as fd on fd.loan_reference_number=mis.loan_reference_number
left join mis.core.USER_FRAUD_DATA uf on l.loan_reference_number=uf.loan_reference_number  and uf.fraud_date<='{to_date}'
left join mis.core.securitisation_data sd on l.loan_reference_number=sd.loan_reference_number
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.USER_ADDRESS AS ua ON ta.present_address_reference_number=ua.address_reference_number
LEFT JOIN RING_SOURCE.mysql.PINCODE_MASTER pn ON pn.pincode = ua.pincode
LEFT JOIN RING_SOURCE.MYSQL.CITY_MASTER AS cm ON cm.city_id = pn.city_id
LEFT JOIN RING_SOURCE.MYSQL.STATE_MASTER AS sm ON sm.state_id = cm.state_id
LEFT JOIN lender_details lds on mis.loan_reference_number=lds.loan_reference_number and mis.month_date=date_trunc(month,date(lds.allocation_date)) 
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))
left join (
    select  m.loan_reference_number,max(scheduled_payment_date) as last_emi_date
    from mis.core.business_mis_ring_txn_credit m
    join ring_source.bi.repayments r on m.loan_reference_number=r.loan_reference_number and  m.month_date=date_trunc(month,date('{from_date}'))
    group by 1
) r on mis.loan_reference_number=r.loan_reference_number

union all

-- ======================
-- RING Instaloan
-- ======================

select 
'{from_date}'  as month_date,
date(trunc(mis.settlement_date,'MONTH')) as Disbursement_month,
date(mis.settlement_date) as settlement_date,
'INSTALOAN' as Product,
mis.financier,
l.loan_reference_number,
mis.user_reference_number,
ifnull(l.is_colending,0) as is_colending,
l.instalment_no_months,
l.interest,
null as billing_start_date,
null as billing_end_date,
date(l.original_first_emi_date) as loan_maturity_date,
case 
    when l.instalment_no_months < 6 then '< 6M'
    when l.instalment_no_months >= 6 AND l.instalment_no_months < 12 then '6M - 11M'
    when l.instalment_no_months >=12 then '>= 12M'
end as product_split,
case 
    when mis.UCIC_MAX_DPD <= 0 then 'ONGOING'
    when mis.UCIC_MAX_DPD between 1 and 30  then '1--30'
    when mis.UCIC_MAX_DPD between 31 and 60 then '31-60'
    when mis.UCIC_MAX_DPD between 61 and 85 then '61-85'
    when mis.UCIC_MAX_DPD between 86 and 90 then '86-90'
    when mis.UCIC_MAX_DPD between 91 and 120 then '91-120'
    when mis.UCIC_MAX_DPD between 121 and 150 then '121-150'
    when mis.UCIC_MAX_DPD between 151 and 180 then '151-180'
    --- when mis.UCIC_MAX_DPD > 180 then '180+'
    when mis.UCIC_MAX_DPD between 181 and 270 then '181-270'
    when mis.UCIC_MAX_DPD between 271 and 365 then '271-365'
    when mis.UCIC_MAX_DPD > 365 then '365+'
    else null
end as DPD_Split,
mis.loan_amount,
mis.pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.05
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.1
        -- when mis.financier in ('POONAWALA','SICREVA') then mis.pos
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.2
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.1
        -- when mis.financier='PFL' then mis.pos*0.05
        -- else 0
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
,2) as Onbook_pos,


round(
    case 
        when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.95
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.9
        -- when mis.financier in ('POONAWALA','SICREVA') then 0
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.8
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.9
        -- when mis.financier='PFL' then mis.pos*0.95 
        -- else mis.pos
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
,2) as Offbook_pos,
mis.dpd_number as dpd_og,
mis.UCIC_MAX_DPD,
ifnull(mis.repeat_type,'Fresh') as repeat_type,
-- iff(ifnull(mis.repeat_type,0)=0,'Fresh','Repeat') as repeat_type,
t.NEW_NPA_DATE,
t.WRITE_OFF_DATE,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount
,sd.securitisation_tagging
,mis.pan_hash as ucic,
l.interest as interest_rate,
case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
    when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd>150 then 'WRITE_OFF'
    -- when mis.UCIC_MAX_DPD > 180 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging --After March_2024

-- ,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING
,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING

,mis.OFFBOOK_WRITE_OFF_DATE

,sm.state_name
,mis.ucic_repeat_type
,iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd
,lds.lender_name
,air.TOTAL_ONBOOK_ACCRUED_INTEREST
,apr.ACCUMULATED_PROCESSING_FEES
,apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,r.last_emi_date
,apr.CGFEES
,apr.RISKPREMIUM
,apr.CGFEES_RISKPREMIUM_WITH_GST_9
,apr.ACCUMULATED_GC_FEE

from mis.core.business_mis_ring_instaloan mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}' )) 
and mis.pos>0 
left join ring_source.bi.transactions ta on ta.transaction_reference_number=l.transaction_reference_number
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.fldg_data as fd on fd.loan_reference_number=mis.loan_reference_number
left join mis.core.USER_FRAUD_DATA uf on l.loan_reference_number=uf.loan_reference_number and uf.fraud_date<='{to_date}' 
left join mis.core.securitisation_data sd on l.loan_reference_number=sd.loan_reference_number
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.USER_ADDRESS AS ua ON ta.present_address_reference_number=ua.address_reference_number
LEFT JOIN RING_SOURCE.mysql.PINCODE_MASTER pn ON pn.pincode = ua.pincode
LEFT JOIN RING_SOURCE.MYSQL.CITY_MASTER AS cm ON cm.city_id = pn.city_id
LEFT JOIN RING_SOURCE.MYSQL.STATE_MASTER AS sm ON sm.state_id = cm.state_id
LEFT JOIN lender_details lds on mis.loan_reference_number=lds.loan_reference_number and mis.month_date=date_trunc(month,date(lds.allocation_date)) 
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))
left join (
    select  m.loan_reference_number,max(scheduled_payment_date) as last_emi_date
    from mis.core.business_mis_ring_instaloan m
    join ring_source.bi.repayments r on m.loan_reference_number=r.loan_reference_number and  m.month_date=date_trunc(month,date('{from_date}'))
    group by 1
) r on mis.loan_reference_number=r.loan_reference_number

union all

-- ======================
-- RING LAP
-- ======================


select 
'{from_date}'  as month_date,
date(trunc(mis.created_at,'MONTH')) as Disbursement_month,
date(mis.settlement_date) as settlement_date,
'LAP' as Product,
mis.financier,
l.loan_reference_number,
mis.user_reference_number,
ifnull(l.is_colending,0) as is_colending,
l.instalment_no_months,
l.interest,
null as billing_start_date,
null as billing_end_date,
date(l.original_first_emi_date) as loan_maturity_date,
case 
    when l.instalment_no_months < 6 then '< 6M'
    when l.instalment_no_months >= 6 AND l.instalment_no_months < 12 then '6M - 11M'
    when l.instalment_no_months >=12 then '>= 12M'
end as product_split,
case 
    when mis.UCIC_MAX_DPD <= 0 then 'ONGOING'
    when mis.UCIC_MAX_DPD between 1 and 30  then '1--30'
    when mis.UCIC_MAX_DPD between 31 and 60 then '31-60'
    when mis.UCIC_MAX_DPD between 61 and 85 then '61-85'
    when mis.UCIC_MAX_DPD between 86 and 90 then '86-90'
    when mis.UCIC_MAX_DPD between 91 and 120 then '91-120'
    when mis.UCIC_MAX_DPD between 121 and 150 then '121-150'
    when mis.UCIC_MAX_DPD between 151 and 180 then '151-180'
    -- when mis.UCIC_MAX_DPD > 180 then '180+'
    when mis.UCIC_MAX_DPD between 181 and 270 then '181-270'
    when mis.UCIC_MAX_DPD between 271 and 365 then '271-365'
    when mis.UCIC_MAX_DPD > 365 then '365+'
    else null
end as DPD_Split,
mis.loan_amount,
mis.pos,
round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.1
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.05
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.1
        -- when mis.financier in ('POONAWALA','SICREVA') then mis.pos
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.2
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.1
        -- when mis.financier='PFL' then mis.pos*0.05
        -- else 0
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
,2) as Onbook_pos,


round(
    case 
        -- when upper(sd.securitisation_tagging)='BAJAJ DA' and mis.month_date>='2025-01-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 2' and mis.month_date>='2025-08-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='BAJAJ DA 3' and mis.month_date>='2025-10-01' then mis.pos*0.9
        -- when upper(sd.securitisation_tagging)='MAS DA 1' and mis.month_date>='2025-06-01' then mis.pos*0.95
        -- when upper(sd.securitisation_tagging) like 'NAC-DA%' and mis.month_date>='2025-09-01' then mis.pos*0.9
        -- when mis.financier in ('POONAWALA','SICREVA') then 0
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)>'2024-09-30') then mis.pos*0.8
        -- WHEN (mis.financier='PCHFL' and date(mis.settlement_date)<='2024-09-30') or (mis.financier='MAS' and l.is_colending=1) then mis.pos*0.9
        -- when mis.financier='PFL' then mis.pos*0.95 
        -- else mis.pos
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
,2) as Offbook_pos,
mis.dpd_number as dpd_og,
mis.UCIC_MAX_DPD,
ifnull(mis.repeat_type,'Fresh') as repeat_type,
-- iff(ifnull(mis.repeat_type,0)=0,'Fresh','Repeat') as repeat_type,
t.NEW_NPA_DATE,
t.WRITE_OFF_DATE,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging
,mis.pan_hash as ucic,
l.interest as interest_rate,
case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
    -- when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd>150 then 'WRITE_OFF'
    -- when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 455) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    -- when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 455 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging --After March_2024


-- ,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING
,iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING

,mis.OFFBOOK_WRITE_OFF_DATE

,sm.state_name
,mis.ucic_repeat_type
,iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd
,lds.lender_name
,air.TOTAL_ONBOOK_ACCRUED_INTEREST
,apr.ACCUMULATED_PROCESSING_FEES
,apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,r.last_emi_date
,apr.CGFEES
,apr.RISKPREMIUM
,apr.CGFEES_RISKPREMIUM_WITH_GST_9
,apr.ACCUMULATED_GC_FEE

from mis.core.business_mis_lap mis
-- ring_trf.transient.mis_dpd_pos_dues_others mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}' )) 
and mis.pos>0 
left join ring_source.bi.transactions ta on ta.transaction_reference_number=l.transaction_reference_number
and mis.PRODUCT='LAP'
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.fldg_data as fd on fd.loan_reference_number=mis.loan_reference_number
left join mis.core.USER_FRAUD_DATA uf on l.loan_reference_number=uf.loan_reference_number and uf.fraud_date<='{to_date}' 
left join mis.core.securitisation_data sd on l.loan_reference_number=sd.loan_reference_number
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.USER_ADDRESS AS ua ON ta.present_address_reference_number=ua.address_reference_number
LEFT JOIN RING_SOURCE.mysql.PINCODE_MASTER pn ON pn.pincode = ua.pincode
LEFT JOIN RING_SOURCE.MYSQL.CITY_MASTER AS cm ON cm.city_id = pn.city_id
LEFT JOIN RING_SOURCE.MYSQL.STATE_MASTER AS sm ON sm.state_id = cm.state_id
LEFT JOIN lender_details lds on mis.loan_reference_number=lds.loan_reference_number and mis.month_date=date_trunc(month,date(lds.allocation_date)) 
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))
left join (
    select  m.loan_reference_number,max(scheduled_payment_date) as last_emi_date
    from mis.core.business_mis_lap m
    join ring_source.bi.repayments r on m.loan_reference_number=r.loan_reference_number and  m.month_date=date_trunc(month,date('{from_date}'))
    group by 1
) r on mis.loan_reference_number=r.loan_reference_number
;