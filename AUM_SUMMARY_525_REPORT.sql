--ring 525 AUM SUMMARY REPORT
with cte as 
(
    select *
    -- from ring_trf.transient.daily_dpd_swapped_users_bkp_16062025_double_wo_date 
    from mis.core.daily_npa_wo_master
    where dt='{to_date}'
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
) 
,
AUM_SUMMARY_525 as (
-- ======================
-- Kissht
-- ======================
select 
    '{from_date}' as month_date,
    -- date(trunc(mis.settlement_date,'MONTH')) as Disbursement_month,
    date(trunc(IFNULL(mis.settlement_date,MIS.CREATED_DATE),'MONTH')) as Disbursement_month,
    'KISSHT' as Product,
    
    concat(mis.financier,'-',

    ROUND(CASE
        WHEN mis.financier IN ('POONAWALA', 'SICREVA') THEN 100
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND L.IS_COLENDING = 1 THEN COALESCE(CL.ACTIVE_LOAN_RATIO, 100)
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND COALESCE(L.IS_COLENDING, 0) = 0 THEN COALESCE(CL.PASSIVE_LOAN_RATIO, 100)
        ELSE 100
    END)) AS financier_ratio,
    
    ifnull(l.is_colending,0) as is_colending,
    nvl(mis.repeat_type,'Fresh') as repeat_type,
    mis.UCIC_REPEAT_TYPE,
    
case 
    when l.instalment_no_months = 1 and l.bullet_loan_days = 7 then '7D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 15 then '15D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 30 then '30D'
    when l.instalment_no_months = 1  then '1M'
    when l.instalment_no_months = 2 and l.bullet_loan_days = 62 then '62D'
    when l.instalment_no_months = 2  then '2M'
    else concat(l.instalment_no_months,'M') 
end product_type,

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
    when mis.UCIC_MAX_DPD between 181 and 210 then '181-210'
    when mis.UCIC_MAX_DPD between 211 and 240 then '211-240'
    when mis.UCIC_MAX_DPD between 241 and 270 then '241-270'
    when mis.UCIC_MAX_DPD between 271 and 300 then '271-300'
    when mis.UCIC_MAX_DPD between 301 and 330 then '301-330'
    when mis.UCIC_MAX_DPD between 331 and 360 then '331-360'
    when mis.UCIC_MAX_DPD > 360 then '360+'
    else null
end as UCIC_DPD_Split,
case 
    when mis.DPD_NUMBER <= 0 then 'ONGOING'
    when mis.DPD_NUMBER between 1 and 30 then '1--30'
    when mis.DPD_NUMBER between 31 and 60 then '31-60'
    when mis.DPD_NUMBER between 61 and 85 then '61-85'
    when mis.DPD_NUMBER between 86 and 90 then '86-90'
    when mis.DPD_NUMBER between 91 and 120 then '91-120'
    when mis.DPD_NUMBER between 121 and 150 then '121-150'
    when mis.DPD_NUMBER between 151 and 180 then '151-180'
    when mis.DPD_NUMBER between 181 and 210 then '181-210'
    when mis.DPD_NUMBER between 211 and 240 then '211-240'
    when mis.DPD_NUMBER between 241 and 270 then '241-270'
    when mis.DPD_NUMBER between 271 and 300 then '271-300'
    when mis.DPD_NUMBER between 301 and 330 then '301-330'
    when mis.DPD_NUMBER between 331 and 360 then '331-360'
    when mis.DPD_NUMBER > 360 then '360+'
    else null
end as LOAN_DPD_Split,

l.interest,

sum(mis.pos) as POS,

count(mis.pos) as loan_count,


case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
     when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd > 150 then 'WRITE_OFF'
    -- when mis.UCIC_MAX_DPD > 180 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging, --After March_2024 

-- iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,
iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,

iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging,

round(sum(
    case 
       
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
),2) as Onbook,


round(sum (
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
),2) as Offbook,


round(sum (
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_ST,
    
round(sum (
    iff( l.instalment_no_months >= 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_LT,


round(sum ( 
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_ST,

round(sum ( 
    iff( l.instalment_no_months >= 6 ,
     case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_LT,
sum(mis.loan_amount) AS LOAN_AMOUNT
,sum(air.TOTAL_ONBOOK_ACCRUED_INTEREST) as TOTAL_ONBOOK_ACCRUED_INTEREST
,sum(apr.ACCUMULATED_PROCESSING_FEES) as ACCUMULATED_PROCESSING_FEES
,sum(apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,sum(apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,null as DSA_Commision_fees_with_GST_9
,mis.processing_fee as pf_without_gst
,sum(apr.CGFEES) as CGFEES
,sum(apr.RISKPREMIUM) as RISKPREMIUM
,sum(apr.CGFEES_RISKPREMIUM_WITH_GST_9) as CGFEES_RISKPREMIUM_WITH_GST_9
,sum(apr.ACCUMULATED_GC_FEE) as ACCUMULATED_GC_FEE

from mis.core.business_mis_kissht mis
join KISSHT_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}')) and mis.pos>0 
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.securitisation_data sd on sd.loan_reference_number=l.loan_reference_number
left join mis.core.fldg_data fd on fd.loan_reference_number=l.loan_reference_number
left join KISSHT_SOURCE.MYSQL.COLENDING_LOAN_DETAILS AS CL ON L.LOAN_REFERENCE_NUMBER = CL.LOAN_REFERENCE_NUMBER
AND NVL(L.IS_COLENDING,0)=1
left join mis.core.USER_FRAUD_DATA uf on uf.loan_reference_number=l.loan_reference_number and uf.fraud_date <='{to_date}'
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))


group by all  

union all

-- ======================
-- Ring Txn Credit
-- ======================
select 
'{from_date}' as month_date,
date(trunc(mis.created_at,'MONTH')) as Disbursement_month,'TXN_CREDIT' as Product,

concat(mis.financier_name,'-',

    ROUND(CASE
        WHEN mis.financier_name IN ('POONAWALA', 'SICREVA') THEN 100
        WHEN mis.financier_name NOT IN ('POONAWALA', 'SICREVA') AND L.IS_COLENDING = 1 THEN COALESCE(CL.ACTIVE_LOAN_RATIO, 100)
        WHEN mis.financier_name NOT IN ('POONAWALA', 'SICREVA') AND COALESCE(L.IS_COLENDING, 0) = 0 THEN COALESCE(CL.PASSIVE_LOAN_RATIO, 100)
        ELSE 100
    END)) AS financier_ratio,

ifnull(l.is_colending,0) as is_colending,

  nvl(mis.repeat_type,'Fresh') as repeat_type,
  mis.ucic_repeat_type,
 
case 
    when l.instalment_no_months = 1 and l.bullet_loan_days = 7 then '7D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 15 then '15D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 30 then '30D'
    when l.instalment_no_months = 1  then '1M'
    when l.instalment_no_months = 2 and l.bullet_loan_days = 62 then '62D'
    when l.instalment_no_months = 2  then '2M'
    else concat(l.instalment_no_months,'M') 
end product_type,

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
    when mis.UCIC_MAX_DPD between 181 and 210 then '181-210'
    when mis.UCIC_MAX_DPD between 211 and 240 then '211-240'
    when mis.UCIC_MAX_DPD between 241 and 270 then '241-270'
    when mis.UCIC_MAX_DPD between 271 and 300 then '271-300'
    when mis.UCIC_MAX_DPD between 301 and 330 then '301-330'
    when mis.UCIC_MAX_DPD between 331 and 360 then '331-360'
    when mis.UCIC_MAX_DPD > 360 then '360+'
   
    else null
end as UCIC_DPD_Split,
case 
    when mis.DPD_NUMBER <= 0 then 'ONGOING'
    when mis.DPD_NUMBER between 1 and 30 then '1--30'
    when mis.DPD_NUMBER between 31 and 60 then '31-60'
    when mis.DPD_NUMBER between 61 and 85 then '61-85'
    when mis.DPD_NUMBER between 86 and 90 then '86-90'
    when mis.DPD_NUMBER between 91 and 120 then '91-120'
    when mis.DPD_NUMBER between 121 and 150 then '121-150'
    when mis.DPD_NUMBER between 151 and 180 then '151-180'
    when mis.DPD_NUMBER between 181 and 210 then '181-210'
    when mis.DPD_NUMBER between 211 and 240 then '211-240'
    when mis.DPD_NUMBER between 241 and 270 then '241-270'
    when mis.DPD_NUMBER between 271 and 300 then '271-300'
    when mis.DPD_NUMBER between 301 and 330 then '301-330'
    when mis.DPD_NUMBER between 331 and 360 then '331-360'
    when mis.DPD_NUMBER > 360 then '360+'
 
    else null
end as LOAN_DPD_Split,

l.interest,

sum(mis.pos) AS POS,

count(mis.pos) as loan_count,

case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
     when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd > 150 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging, --After March_2024 

-- iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,
iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,

iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging,

round(sum(
    case 
       
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
),2) as Onbook,


round(sum (
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
),2) as Offbook,


round(sum (
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_ST,
    
round(sum (
    iff( l.instalment_no_months >= 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_LT,


round(sum ( 
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_ST,

round(sum ( 
    iff( l.instalment_no_months >= 6 ,
     case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_LT,
sum(mis.accumulated_loan_amount) AS LOAN_AMOUNT
,sum(air.TOTAL_ONBOOK_ACCRUED_INTEREST) as TOTAL_ONBOOK_ACCRUED_INTEREST
,sum(apr.ACCUMULATED_PROCESSING_FEES) as ACCUMULATED_PROCESSING_FEES
,sum(apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,sum(apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,null as DSA_Commision_fees_with_GST_9
,null as pf_without_gst
,sum(apr.CGFEES) as CGFEES
,sum(apr.RISKPREMIUM) as RISKPREMIUM
,sum(apr.CGFEES_RISKPREMIUM_WITH_GST_9) as CGFEES_RISKPREMIUM_WITH_GST_9
,sum(apr.ACCUMULATED_GC_FEE) as ACCUMULATED_GC_FEE

from  mis.core.business_mis_ring_txn_credit mis
--ring_trf.transient.mis_dpd_pos_dues mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}')) and mis.pos>0
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.securitisation_data sd on sd.loan_reference_number=l.loan_reference_number
left join mis.core.fldg_data fd on fd.loan_reference_number=l.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.COLENDING_LOAN_DETAILS AS CL ON L.LOAN_REFERENCE_NUMBER = CL.LOAN_REFERENCE_NUMBER
AND NVL(L.IS_COLENDING,0)=1
left join mis.core.USER_FRAUD_DATA uf on uf.loan_reference_number=l.loan_reference_number and uf.fraud_date <='{to_date}'
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))

group by all 

union all

-- ======================
-- Instaloan
-- ======================

select 
'{from_date}' as month_date,
date(trunc(mis.settlement_date,'MONTH')) as Disbursement_month,'INSTALOAN' as Product,
concat(mis.financier,'-',

    ROUND(CASE
        WHEN mis.financier IN ('POONAWALA', 'SICREVA') THEN 100
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND L.IS_COLENDING = 1 THEN COALESCE(CL.ACTIVE_LOAN_RATIO, 100)
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND COALESCE(L.IS_COLENDING, 0) = 0 THEN COALESCE(CL.PASSIVE_LOAN_RATIO, 100)
        ELSE 100
    END)) AS financier_ratio,


ifnull(l.is_colending,0) as is_colending,
  nvl(mis.repeat_type,'Fresh') as repeat_type,
   mis.ucic_repeat_type,

 
case 
    when l.instalment_no_months = 1 and l.bullet_loan_days = 7 then '7D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 15 then '15D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 30 then '30D'
    when l.instalment_no_months = 1  then '1M'
    when l.instalment_no_months = 2 and l.bullet_loan_days = 62 then '62D'
    when l.instalment_no_months = 2  then '2M'
    else concat(l.instalment_no_months,'M') 
end product_type,

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
    when mis.UCIC_MAX_DPD between 181 and 210 then '181-210'
    when mis.UCIC_MAX_DPD between 211 and 240 then '211-240'
    when mis.UCIC_MAX_DPD between 241 and 270 then '241-270'
    when mis.UCIC_MAX_DPD between 271 and 300 then '271-300'
    when mis.UCIC_MAX_DPD between 301 and 330 then '301-330'
    when mis.UCIC_MAX_DPD between 331 and 360 then '331-360'
    when mis.UCIC_MAX_DPD > 360 then '360+'
   
    else null
end as UCIC_DPD_Split,
case 
    when mis.DPD_NUMBER <= 0 then 'ONGOING'
    when mis.DPD_NUMBER between 1 and 30 then '1--30'
    when mis.DPD_NUMBER between 31 and 60 then '31-60'
    when mis.DPD_NUMBER between 61 and 85 then '61-85'
    when mis.DPD_NUMBER between 86 and 90 then '86-90'
    when mis.DPD_NUMBER between 91 and 120 then '91-120'
    when mis.DPD_NUMBER between 121 and 150 then '121-150'
    when mis.DPD_NUMBER between 151 and 180 then '151-180'
    when mis.DPD_NUMBER between 181 and 210 then '181-210'
    when mis.DPD_NUMBER between 211 and 240 then '211-240'
    when mis.DPD_NUMBER between 241 and 270 then '241-270'
    when mis.DPD_NUMBER between 271 and 300 then '271-300'
    when mis.DPD_NUMBER between 301 and 330 then '301-330'
    when mis.DPD_NUMBER between 331 and 360 then '331-360'
    when mis.DPD_NUMBER > 360 then '360+'
   
    else null
end as LOAN_DPD_Split,

l.interest,

sum(mis.pos) AS POS,

count(mis.pos) as loan_count,


case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
     when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd > 150 then 'WRITE_OFF'
    -- when mis.UCIC_MAX_DPD > 180 then 'WRITE_OFF'
    when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging, --After March_2024 

-- iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,
iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,

iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging,

round(sum(
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
),2) as Onbook,


round(sum (
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
),2) as Offbook,


round(sum (
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_ST,
    
round(sum (
    iff( l.instalment_no_months >= 6 ,
    case 
       
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_LT,


round(sum ( 
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_ST,

round(sum ( 
    iff( l.instalment_no_months >= 6 ,
     case 
       
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_LT,
sum(mis.loan_amount) AS LOAN_AMOUNT
,sum(air.TOTAL_ONBOOK_ACCRUED_INTEREST) as TOTAL_ONBOOK_ACCRUED_INTEREST
,sum(apr.ACCUMULATED_PROCESSING_FEES) as ACCUMULATED_PROCESSING_FEES
,sum(apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,sum(apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,null as DSA_Commision_fees_with_GST_9
,mis.processing_fee as pf_without_gst
,sum(apr.CGFEES) as CGFEES
,sum(apr.RISKPREMIUM) as RISKPREMIUM
,sum(apr.CGFEES_RISKPREMIUM_WITH_GST_9) as CGFEES_RISKPREMIUM_WITH_GST_9
,sum(apr.ACCUMULATED_GC_FEE) as ACCUMULATED_GC_FEE

from mis.core.business_mis_ring_instaloan mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}')) and mis.pos>0 
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.securitisation_data sd on sd.loan_reference_number=l.loan_reference_number
left join mis.core.fldg_data fd on fd.loan_reference_number=l.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.COLENDING_LOAN_DETAILS AS CL ON L.LOAN_REFERENCE_NUMBER = CL.LOAN_REFERENCE_NUMBER
AND NVL(L.IS_COLENDING,0)=1
left join mis.core.USER_FRAUD_DATA uf on uf.loan_reference_number=l.loan_reference_number and uf.fraud_date <='{to_date}'
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))

group by all 


union all

-- ======================
-- RING LAP
-- ======================

select 
'{from_date}' as month_date,
date(trunc(mis.settlement_date,'MONTH')) as Disbursement_month, 'LAP' as Product,

concat(mis.financier,'-',

    ROUND(CASE
        WHEN mis.financier IN ('POONAWALA', 'SICREVA') THEN 100
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND L.IS_COLENDING = 1 THEN COALESCE(CL.ACTIVE_LOAN_RATIO, 100)
        WHEN mis.financier NOT IN ('POONAWALA', 'SICREVA') AND COALESCE(L.IS_COLENDING, 0) = 0 THEN COALESCE(CL.PASSIVE_LOAN_RATIO, 100)
        ELSE 100
    END)) AS financier_ratio,

ifnull(l.is_colending,0) as is_colending,
  nvl(mis.repeat_type,'Fresh') as repeat_type,
 mis.ucic_repeat_type,
 
case 
    when l.instalment_no_months = 1 and l.bullet_loan_days = 7 then '7D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 15 then '15D'
    when l.instalment_no_months = 1 and l.bullet_loan_days = 30 then '30D'
    when l.instalment_no_months = 1  then '1M'
    when l.instalment_no_months = 2 and l.bullet_loan_days = 62 then '62D'
    when l.instalment_no_months = 2  then '2M'
    else concat(l.instalment_no_months,'M') 
end product_type,

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
    when mis.UCIC_MAX_DPD between 181 and 210 then '181-210'
    when mis.UCIC_MAX_DPD between 211 and 240 then '211-240'
    when mis.UCIC_MAX_DPD between 241 and 270 then '241-270'
    when mis.UCIC_MAX_DPD between 271 and 300 then '271-300'
    when mis.UCIC_MAX_DPD between 301 and 330 then '301-330'
    when mis.UCIC_MAX_DPD between 331 and 360 then '331-360'
    when mis.UCIC_MAX_DPD > 360 then '360+'
   
    else null
end as UCIC_DPD_Split,
case 
    when mis.DPD_NUMBER <= 0 then 'ONGOING'
    when mis.DPD_NUMBER between 1 and 30 then '1--30'
    when mis.DPD_NUMBER between 31 and 60 then '31-60'
    when mis.DPD_NUMBER between 61 and 85 then '61-85'
    when mis.DPD_NUMBER between 86 and 90 then '86-90'
    when mis.DPD_NUMBER between 91 and 120 then '91-120'
    when mis.DPD_NUMBER between 121 and 150 then '121-150'
    when mis.DPD_NUMBER between 151 and 180 then '151-180'
    when mis.DPD_NUMBER between 181 and 210 then '181-210'
    when mis.DPD_NUMBER between 211 and 240 then '211-240'
    when mis.DPD_NUMBER between 241 and 270 then '241-270'
    when mis.DPD_NUMBER between 271 and 300 then '271-300'
    when mis.DPD_NUMBER between 301 and 330 then '301-330'
    when mis.DPD_NUMBER between 331 and 360 then '331-360'
    when mis.DPD_NUMBER > 360 then '360+'
    
    else null
end as LOAN_DPD_Split,

l.interest,

sum(mis.pos) AS POS,

count(mis.pos) as loan_count,


case 
    when uf.fraud_date is not null then 'WRITE_OFF'
    when t.WRITE_OFF_DATE is not null then 'WRITE_OFF'
     -- when ifnull(l.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd > 150 then 'WRITE_OFF'
    -- when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 455) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
    -- when l.instalment_no_months < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
    when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
    when t.NEW_NPA_DATE is not null then 'NPA'
    when t.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 455 and l.instalment_no_months>=6 then 'NPA'
    else 'STANDARD'
end as Tagging, --After March_2024 

-- iff(mis.OFFBOOK_WRITE_OFF_DATE is not null,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,
iff(mis.OFFBOOK_WRITE_OFF_DATE is not null or mis.dpd_number>90,'OFFBOOK_WRITE_OFF','OFFBOOK_STANDARD') as OFFBOOK_WRITE_OFF_TAGGING,

iff(ed.loan_reference_number=mis.loan_reference_number,'Y','N') as ever_dpd,
fd.fldg_date AS fldg_date,
uf.fraud_date,
uf.total_fraud_amount,
sd.securitisation_tagging,


round(sum(
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end
),2) as Onbook,


round(sum (
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end
),2) as Offbook,


round(sum (
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_ST,
    
round(sum (
    iff( l.instalment_no_months >= 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100
    end,
    0)
),2) as Onbook_LT,


round(sum ( 
    iff( l.instalment_no_months < 6 ,
    case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_ST,

round(sum ( 
    iff( l.instalment_no_months >= 6 ,
     case 
        
        WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100
    end,
        0)
),2) as Offbook_LT,
sum(mis.loan_amount) AS LOAN_AMOUNT
,sum(air.TOTAL_ONBOOK_ACCRUED_INTEREST) as TOTAL_ONBOOK_ACCRUED_INTEREST
,sum(apr.ACCUMULATED_PROCESSING_FEES) as ACCUMULATED_PROCESSING_FEES
,sum(apr.ACCUMULATED_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_FACILIATION_FEES_WITH_GST_9
,sum(apr.ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9) as ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9
,ROUND(sum(ifnull(wp.COMMISSION_AMOUNT_EXCL_GST,0)* 1.09),2) as DSA_Commision_fees_with_GST_9
,ROUND(sum(ifnull(l.processing_fee,0)/1.18),2) as pf_without_gst
,sum(apr.CGFEES) as CGFEES
,sum(apr.RISKPREMIUM) as RISKPREMIUM
,sum(apr.CGFEES_RISKPREMIUM_WITH_GST_9) as CGFEES_RISKPREMIUM_WITH_GST_9
,sum(apr.ACCUMULATED_GC_FEE) as ACCUMULATED_GC_FEE


from mis.core.business_mis_lap mis
join RING_SOURCE.BI.LOANS l on mis.loan_reference_number=l.loan_reference_number and  mis.month_date=date_trunc(month,date('{from_date}')) and mis.pos>0 
left join cte t on t.loan_reference_number=mis.loan_reference_number 
left join mis.core.securitisation_data sd on sd.loan_reference_number=l.loan_reference_number
left join mis.core.fldg_data fd on fd.loan_reference_number=l.loan_reference_number
LEFT JOIN RING_SOURCE.MYSQL.COLENDING_LOAN_DETAILS AS CL ON L.LOAN_REFERENCE_NUMBER = CL.LOAN_REFERENCE_NUMBER
AND NVL(L.IS_COLENDING,0)=1
left join mis.core.USER_FRAUD_DATA uf on uf.loan_reference_number=l.loan_reference_number and uf.fraud_date <='{to_date}'
left join ever_dpd ed on ed.loan_reference_number=mis.loan_reference_number
left join mis.core.vw_accured_interest_report air on air.loan_reference_number= mis.loan_reference_number and air.month_date=date_trunc(month,date('{from_date}'))
left join mis.core.vw_accumulated_pf_report apr on apr.loan_reference_number= mis.loan_reference_number and apr.month_date=date_trunc(month,date('{from_date}'))
left join MIS.CORE.WEB_PART_LOAN wp on wp.loan_reference_number=mis.loan_reference_number
where mis.PRODUCT='LAP'
group by all 

)

SELECT *,
ACCUMULATED_FACILIATION_FEES_WITH_GST_9 * 100/109 AS ACCUMULATED_FACILIATION_FEES_WITHOUT_GST,
ACCUMULATED_FACILIATION_FEES_WITH_GST_9 * 9/109 AS ACCUMULATED_FACILIATION_FEES_GST,

ifnull(Onbook,0) + ifnull(TOTAL_ONBOOK_ACCRUED_INTEREST,0) - ifnull(ACCUMULATED_PROCESSING_FEES,0) + ifnull(ACCUMULATED_FACILIATION_FEES_WITH_GST_9,0) + ifnull(ACCUMULATED_MERCHANT_FACILIATION_FEES_WITH_GST_9,0) as Total_Onbook_IndAS_AUM,

Total_Onbook_IndAS_AUM + IFF(TAGGING='STANDARD',OFFBOOK,0) AS Total_AUM_Standalone,
Total_AUM_Standalone-ACCUMULATED_FACILIATION_FEES_WITHOUT_GST AS Total_AUM_Consol,
CASE 
    WHEN TAGGING<>'STANDARD' THEN 'Stage-3'
    WHEN UCIC_DPD_SPLIT IN ('ONGOING','1--30') THEN 'Stage-1'
    WHEN UCIC_DPD_SPLIT IN ('31-60','61-85','86-90') THEN 'Stage-2'
    -- WHEN UCIC_DPD_SPLIT IN ('91-120','121-150','151-180','181-210','211-240','241-270','271-300','301-330','331-360','360+') THEN 'Stage-3'
    ELSE 'Stage-3' 
END AS STAGE

FROM AUM_SUMMARY_525 ;