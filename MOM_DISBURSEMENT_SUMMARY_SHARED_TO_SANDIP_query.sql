-- Top 50 city
create or replace table ring_trf.transient.Apr25_Mar26_top_50_city as 
select city_name,count(loan_reference_number) as loan_count,sum(loan_amount) as loan_amount
from mis.core.loan_tape_FY23_25
where date(disbursement_date) between '2025-04-01' and '2026-03-31'
group by city_name order by loan_count desc limit 50;


-- Loan Tape Summary
select        
        date_trunc(month,date(DISBURSEMENT_DATE)) as month_date,
        'APR25-MAR26' as PERIOD,
        CASE
            WHEN IFNULL(dis.age, 0) < 25 THEN '<25 years'
            WHEN IFNULL(dis.age, 0) >= 25 AND IFNULL(dis.age, 0) <= 35 THEN '25 to 35 Years'
            WHEN IFNULL(dis.age, 0) > 35 AND IFNULL(dis.age, 0) <= 45 THEN '36 to 45 years'
            WHEN IFNULL(dis.age, 0) > 45 THEN '>45 Years'
            ELSE 'Other'
        END AS Age_Bucket,
        tenure,
        case 
            when tenure<=2 then 'ST'
            when tenure between 3 and 5 then 'MT'
            when tenure>=6 then 'LT'
            else 'others' 
        end AS tenure_bucket,
        UCIC_REPEAT_TYPE,
        FINANCIER_NAME,
        ONBOOK_RATIO,
        OFFBOOK_RATIO,
        (case
            when cibil_mix is null then 'NTC'
            when cibil_mix < 700 then '<700'
            when cibil_mix between 700 and 760 then '700--760'
            when cibil_mix > 760 then '>760'
            else 'NTC'
        end) as CIBIL_BUCKET,
        CUSTOMER_PROFILE as EMPLOYMENT_TYPE,
        (case 
            when INCOME_BAND is null then 'OTHER'
            when INCOME_BAND >100000 THEN '>100K'
            when INCOME_BAND >=75000 and INCOME_BAND<=100000 THEN '75K to 100K'
            when INCOME_BAND >=50000 and INCOME_BAND<75000 THEN '50K to 75K'
            when INCOME_BAND >=25000 and INCOME_BAND<50000 THEN '25K to 50K'
            when INCOME_BAND >=200 and INCOME_BAND<25000 THEN '200 to 25K'
        END) as CUSTOMER_MIX_INCOME,
        iff(CITY_NAME in (select city_name from ring_trf.transient.Apr25_Mar26_top_50_city),'TOP_50_CITIES','OTHER') as CITIS_MIX,
        sum(LOAN_AMOUNT) as LOAN_AMOUNT,
        sum(LOAN_AMOUNT*ONBOOK_RATIO/100) as Onbook_Loan_Amt,   
        sum(LOAN_AMOUNT*OFFBOOK_RATIO/100) as Offboo_Loan_Amt,
        count(loan_reference_number) as LOAN_COUNT
from mis.core.loan_tape_FY23_25 dis where date(DISBURSEMENT_DATE) between  '2025-04-01' and '2026-03-31'
group by all order by all;