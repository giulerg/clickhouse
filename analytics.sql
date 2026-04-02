-- DAU / WAU / MAU и Stickiness

WITH dau_cte AS (
    SELECT
        s.session_date AS dt,
        uniq(s.user_id) AS dau,
        round(countIf(has_purchase==True)  / count() * 100, 2)  AS conv,
        round(avg(page_views), 2) AS avg_views,
        round(avg(is_bounce), 2) AS avg_bounce
    FROM silver.fact_sessions s
    WHERE  s.session_date    >= toStartOfMonth(addMonths(today(), - 1))
            AND s.session_date < toStartOfMonth(today())
    GROUP BY s.session_date
), mau_CTE AS (
    SELECT
        uniq(s.user_id) AS mau
    FROM silver.fact_sessions s
    WHERE  s.session_date    >= toStartOfMonth(addMonths(today(), - 1))
            AND s.session_date < toStartOfMonth(today())
)   
SELECT
    dt,
    dau,
    conv,
    avg_views,
    avg_bounce,
    mau,
    round(dau / mau * 100, 2) AS stickiness
FROM dau_cte
LEFT JOIN mau_CTE ON 1=1;

select * from bronze.raw_orders

-- когортный анализ удержания
WITH cohort_cte AS (
    SELECT
        user_id,
        MIN(toStartOfMonth(r.paid_at)) AS first_pay
    FROM bronze.raw_orders  r
    WHERE user_id IS NOT NULL
        AND paid_at IS NOT NULL
        AND  order_status IN ('delivered','paid')
    GROUP BY user_id
), all_orders_cte AS (
    SELECT
        user_id,
        toStartOfMonth(r.paid_at) AS  pay_date,
        round(SUM(gross_amount), 2) AS amount

    FROM bronze.raw_orders  r
    WHERE user_id IS NOT NULL
        AND paid_at IS NOT NULL
        AND  order_status IN ('delivered','paid')
    GROUP BY user_id, pay_date
    )
SELECT
    a.user_id,
    date_diff('month', first_pay, pay_date) AS month_number

FROM all_orders_cte a
JOIN cohort_cte c  ON a.user_id = c.user_id;
WHERE month_number <=12
