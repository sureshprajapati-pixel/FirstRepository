-- AUM Query till Mar26
select 
mis.system,
ifnull(mis.PRODUCT,'PL') as product,	
iff(sd.SECURITISATION_TAGGING ilike '%DA%',sd.SECURITISATION_TAGGING,IFF(mis.FINANCIER_NAME='POONAWALA','SICREVA',mis.FINANCIER_NAME)) as FINANCIER,
iff(sd.SECURITISATION_TAGGING ilike '%DA%',sd.onbook_ratio,iff(mis.onbook_ratio=0,mis.offbook_ratio,mis.onbook_ratio)) AS financier_ratio,	
u.KYC_RISK_TAGGING,	
COUNT(mis.LOAN_REFERENCE_NUMBER) as LOAN_COUNT,
COUNT(DISTINCT mis.USER_REFERENCE_NUMBER) AS USER_COUNT,
COUNT(DISTINCT IFF(mis.LOAN_STATUS='ONGOING', mis.USER_REFERENCE_NUMBER,NULL)) AS USER_COUNT1,		
sum(mis.pos) as AUM,
round(sum(case WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.onbook_ratio/100
        else mis.pos * mis.onbook_ratio/100 end),2) as Onbook,
round(sum (case WHEN sd.SECURITISATION_TAGGING is not null and mis.month_date>=sd.EFFECTIVE_DATE then mis.pos * sd.offbook_ratio/100
        else mis.pos * mis.offbook_ratio/100 end),2) as Offbook
from mis.core.vw_business_mis mis
left join (
select user_reference_number,KYC_RISK_TAGGING from RING_SOURCE.MYSQL.USERS 
union all
select user_reference_number,KYC_RISK_TAGGING from kissht_SOURCE.MYSQL.USERS 
) u ON mis.USER_REFERENCE_NUMBER = u.USER_REFERENCE_NUMBER
LEFT JOIN MIS.CORE.SECURITISATION_DATA SD ON SD.LOAN_REFERENCE_NUMBER=mis.LOAN_REFERENCE_NUMBER 
where mis.month_date='2026-03-01' and mis.pos>0  and system<>'LAP'
group by all;