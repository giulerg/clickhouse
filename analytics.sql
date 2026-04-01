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




