CREATE TABLE IF NOT EXISTS silver.dim_channels (
	channel_name String,
	channel_type String,
    monthly_budget_rub Decimal(18,2),
	is_active UInt8
)
ENGINE  = ReplacingMergeTree()
ORDER BY channel_name;


-- 
INSERT INTO silver.dim_channels  VALUES 
 ('google',  'paid', 500000, 1),
 ('yandex',  'paid', 300000, 1),
 ('vk',   'paid', 150000, 1),
 ('direct',  'organic', 0,   1),
 ('organic', 'organic', 0,   1),
 ('email',   'owned',   20000,  1);

-- создание словаря.

CREATE DICTIONARY dic_channels (
	channel_name String,
	channel_type String,
    monthly_budget_rub Decimal(18,2)
)
PRIMARY KEY channel_name
SOURCE(CLICKHOUSE(TABLE 'dim_channels' DB 'silver'))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());


--Создание бд
CREATE DATABASE IF NOT EXISTS  gold;

-- создание таблицы
DROP TABLE IF EXISTS gold.mart_daily_revenue;
CREATE TABLE IF NOT EXISTS gold.mart_daily_revenue (
    revenue_date DATETIME,
    channel LowCardinality(String),
    orders_count AggregateFunction(count, UInt64),
    gmv AggregateFunction(sum, DECIMAL(18,2)),
    unique_buyers AggregateFunction(uniq, String),
    avg_order_value AggregateFunction(avg, Decimal(18,2))
)
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(revenue_date)
    ORDER  BY (revenue_date,  channel);

-- создание вьюхи
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_delivered_orders  TO gold.mart_daily_revenue AS
SELECT
    toDate(paid_at) AS revenue_date,
    source_channel AS channel,
    countState() AS orders_count,
    sumState(gross_amount) AS gmv,
    uniqState(user_id) AS unique_buyers,
    avgState(gross_amount) AS avg_order_value
FROM bronze.raw_orders
WHERE order_status = 'delivered'
GROUP BY toDate(paid_at) , source_channel;

--вставляю исторические данные
INSERT INTO gold.mart_daily_revenue
SELECT
    toDate(paid_at) AS revenue_date,
    source_channel AS channel,
    countState() AS orders_count,
    sumState(gross_amount) AS gmv,
    uniqState(user_id) AS unique_buyers,
    avgState(gross_amount)  AS avg_order_value
FROM bronze.raw_orders
WHERE order_status = 'delivered'
GROUP BY toDate(paid_at), source_channel;


--мерджим
SELECT
    revenue_date,
    channel,
    countMerge(orders_count) AS orders_count,
    sumMerge(gmv) AS gmv,
    uniqMerge(unique_buyers) AS unique_buyers,
    round(avgMerge(avg_order_value), 2) AS avg_order_value
FROM gold.mart_daily_revenue
GROUP BY revenue_date, channel
LIMIT 100;