-- =======================================
-- 1. Registered user base
-- =======================================


WITH cte AS (
    SELECT 
        mobile_number,
        MIN(user_creation_date) AS user_creation_date
    FROM (
        SELECT 
            user_reference_number,
            mobile_number,
            DATE(created_at) AS user_creation_date
        FROM ring_source.mysql.users
        WHERE mobile_number IS NOT NULL
          AND DATE(created_at) BETWEEN '2025-07-01' AND CURRENT_DATE

        UNION ALL 

        SELECT 
            user_reference_number,
            mobile_number,
            DATE(created_at) AS user_creation_date
        FROM kissht_source.mysql.users
        WHERE mobile_number IS NOT NULL
          AND DATE(created_at) BETWEEN '2025-07-01' AND CURRENT_DATE
    ) a
    GROUP BY mobile_number
)
, monthly_base AS (
    SELECT
        DATE_TRUNC(month, user_creation_date) AS month_date,
        COUNT(DISTINCT mobile_number) AS registered_user_base_count
    FROM cte
    GROUP BY DATE_TRUNC(month, user_creation_date)
)
SELECT
    month_date,
    -- registered_user_base_count,
    SUM(registered_user_base_count) OVER (
        ORDER BY month_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_registered_user_base_count
FROM monthly_base
ORDER BY month_date;


-- =======================================
-- 2. Customer base
-- =======================================

WITH cte_1 AS (
    SELECT 
        pan_hash,
        MIN(system_banking_date) AS disbursement_date
    FROM (
        SELECT 
            a.loan_reference_number,
            a.user_reference_number,
            pd.pan_hash,
            system_banking_date 
        FROM mis.core.vw_kissht_loans a
        JOIN KISSHT_source.BI.settlements b 
            ON a.fb_transaction_id = b.fb_transaction_id 
           AND is_settled = 'DISBURSED'
        LEFT JOIN KISSHT_SOURCE.mysql.pan_data pd  
            ON a.user_reference_number = pd.user_reference_number 
           AND is_primary = 1 
        WHERE status <> 'CANCELLED' 
          AND DATE(COALESCE(b.banking_datetime, SETTLEMENT_DATE)) 
              BETWEEN '2025-07-01' AND CURRENT_DATE
    
        UNION ALL 
    
        SELECT 
            a.loan_reference_number,
            a.user_reference_number,
            pd.pan_hash,
            system_banking_date 
        FROM mis.core.vw_ring_loans a
        JOIN ring_source.BI.settlements b 
            ON a.transaction_reference_number = b.transaction_reference_number 
           AND is_settled = 'DISBURSED'
        LEFT JOIN ring_SOURCE.mysql.pan_data pd  
            ON a.user_reference_number = pd.user_reference_number 
           AND is_primary = 1
        WHERE status <> 'CANCELLED'
          AND DATE(COALESCE(b.banking_datetime, SETTLEMENT_DATE)) 
              BETWEEN '2025-07-01' AND CURRENT_DATE
    ) rs
    GROUP BY pan_hash
),
monthly_base AS (
    SELECT
        DATE_TRUNC(month, disbursement_date) AS month_date,
        COUNT(DISTINCT pan_hash) AS customer_base_count
    FROM cte_1  
    WHERE DATE(disbursement_date) >= '2025-07-01'
    GROUP BY DATE_TRUNC(month, disbursement_date)
)
SELECT
    month_date,
    -- customer_base_count,
    SUM(customer_base_count) OVER (
        ORDER BY month_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_customer_base_count
FROM monthly_base
ORDER BY month_date;


-- =======================================
-- 3. Average Age
-- =======================================
select 
    date_trunc(month,a.disbursement_date) as month_date,
    sum(a.age) as sum_of_age,
    count(distinct a.loan_reference_number) as loan_count,
    sum(a.age)/count(distinct a.loan_reference_number) as average_age
from mis.core.loan_tape_FY23_25 a
where disbursement_date>='2025-12-01'
group by all order by all
;

