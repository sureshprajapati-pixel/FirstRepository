WITH prev_month_ongoing AS (
    SELECT
        mis.system,
        mis.loan_reference_number,
        mis.loan_status AS loan_status_prev_month,
        mis.pos AS pos_prev_month,
        ROUND(mis.pos * mis.onbook_ratio / 100, 2)  AS onbook_pos_prev_month,
        mis.ucic_max_dpd AS ucic_max_dpd_prev_month,
        mis.financier_name,
        mis.onbook_ratio,
        mis.offbook_ratio,
        mis.instalment_no_month
    FROM mis.core.vw_business_mis mis
    WHERE mis.month_date   = date_trunc(month,date('2026-03-01'))
          AND mis.onbook_ratio > 0
          AND mis.pos > 0
          AND mis.ucic_max_dpd <= 0
          AND mis.loan_status <> 'CLOSED'
),
curr_month_data AS (
    SELECT
        a.*,
        mis.loan_status AS loan_status_curr_month,
        mis.ucic_max_dpd AS ucic_max_dpd_curr_month,
        mis.pos AS pos_curr_month,
        ROUND(mis.pos * mis.onbook_ratio / 100, 2)  AS onbook_pos_curr_month,                                     
        case 
            when uf.fraud_date is not null then 'WRITE_OFF'
            when mis.WRITE_OFF_DATE is not null then 'WRITE_OFF'
            when ifnull(mis.product,'PL') <> 'LAP' and (mis.month_date) >'2025-09-01' and mis.ucic_max_dpd>150 then 'WRITE_OFF'
            when (mis.month_date>='2025-09-01' and mis.UCIC_MAX_DPD > 150) or (mis.month_date<'2025-09-01' and mis.UCIC_MAX_DPD > 180) then 'WRITE_OFF'
            when mis.instalment_no_month < 6 and mis.UCIC_MAX_DPD > 90 then 'WRITE_OFF'
            when mis.UCIC_MAX_DPD <= 0 then 'STANDARD'
            when mis.NEW_NPA_DATE is not null then 'NPA'
            when mis.new_npa_date is null and mis.UCIC_MAX_DPD between 91 and 180 and mis.instalment_no_month>=6 then 'NPA'
            else 'STANDARD'
        end AS tagging_curr_month
        
    FROM prev_month_ongoing        a
    JOIN mis.core.vw_business_mis mis ON mis.loan_reference_number = a.loan_reference_number AND mis.month_date = dateadd(month,1,date('2026-03-01'))
    LEFT JOIN mis.core.user_fraud_data uf ON uf.loan_reference_number = mis.loan_reference_number 
                                            AND uf.fraud_date<=last_day(dateadd(month,1,date('2026-03-01')))
)

SELECT
    'ONGOING' AS status_as_on_prev_month,
    CASE
        WHEN ucic_max_dpd_curr_month <= 0 THEN 'ONGOING'
        WHEN ucic_max_dpd_curr_month BETWEEN  1 AND  30 THEN '1--30'
        WHEN ucic_max_dpd_curr_month BETWEEN 31 AND  60 THEN '31-60'
        WHEN ucic_max_dpd_curr_month BETWEEN 61 AND  90 THEN '61-90'
        WHEN ucic_max_dpd_curr_month BETWEEN 91 AND 150 THEN '91-150'
        WHEN ucic_max_dpd_curr_month > 150 THEN '150+'
    END AS dpd_bucket_curr_month,
    SUM(onbook_pos_prev_month) AS onbook_pos_prev_month
FROM curr_month_data
WHERE loan_status_curr_month <> 'CLOSED'
  AND tagging_curr_month     <> 'WRITE_OFF'
GROUP BY ALL
;