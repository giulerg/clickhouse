-- создание бд

CREATE DATABASE IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.fact_sessions (
	session_id UInt64, 
	user_id Nullable(UInt64), 
	session_start DateTime, 
	session_end DateTime, 
	session_date Date,
	duration_seconds UInt16,
	page_views UInt16,
	events_count UInt16,
	products_viewed UInt16,
	add_to_cart_count UInt16,
	is_bounce Boolean,
	has_purchase Boolean,
	utm_source          LowCardinality(String),
    utm_medium          LowCardinality(String),
    device_type         LowCardinality(String),
    entry_page_type     LowCardinality(String),
    processed_at        DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY  toYYYYMM(session_date)
ORDER BY (session_date, session_id);


-- заливка данных
INSERT INTO silver.fact_sessions (
	session_id,
	user_id,
	session_start,
	session_end,
	session_date,
	duration_seconds,
	page_views,
	events_count,
	products_viewed,
	add_to_cart_count,
	has_purchase,
	utm_source,
	utm_medium,
	device_type,
	entry_page_type,
	processed_at
)
SELECT
	f.session_id,
    any(user_id),
	MIN(event_timestamp),
	MAX(event_timestamp),
	toDate(MIN(event_timestamp)),
	dateDiff('second', MIN(event_timestamp), MAX(event_timestamp)),
	countIf(event_type = 'age_view'),
	count(),
	countIf(event_type = 'product_view'),
	countIf(event_type = 'add_to_cart'),
	countIf(event_type = 'purchase')  > 0,
	COALESCE(argMin(utm_source, event_timestamp), ''),
	COALESCE(argMin(utm_medium, event_timestamp), ''),
	COALESCE(argMin(device_type, event_timestamp), ''),
	COALESCE(argMin(page_type, event_timestamp), ''),  
	MAX(loaded_at)
FROM bronze.raw_app_events f
GROUP BY f.session_id;

-- создание материализованной вьюхи, которая будет заливать автоматом
 
CREATE MATERIALIZED VIEW  silver.mv_sessions
TO silver.fact_sessions
AS SELECT
	f.session_id,
    any(user_id) AS user_id, 
 	MIN(event_timestamp) AS session_start,
 	MAX(event_timestamp) AS session_end, 
	toDate(MIN(event_timestamp))  AS session_date,
	dateDiff('second', MIN(event_timestamp), MAX(event_timestamp)) AS duration_seconds,
	countIf(event_type = 'page_view') AS page_views,
	count() AS events_count, 
	countIf(event_type = 'product_view') AS products_viewed,
	countIf(event_type = 'add_to_cart') AS add_to_cart_count,
	toUInt8(countIf(event_type = 'purchase')  > 0) AS has_purchase,
	COALESCE(argMin(utm_source, event_timestamp), '') AS utm_source ,
	COALESCE(argMin(utm_medium, event_timestamp), '') AS utm_medium,
	COALESCE(argMin(device_type, event_timestamp), '') AS device_type ,
	COALESCE(argMin(page_type, event_timestamp), '')  AS entry_page_type,
	MAX(loaded_at) AS  processed_at
FROM bronze.raw_app_events f
GROUP BY f.session_id;

--Проверим кол-во

SELECT COUNT(*) FROM silver.fact_sessions;  

-- Добавим 1000 новых записей в bronze,чтоб проверить,что они перешли в silver

INSERT INTO 
	bronze.raw_app_events 
	(event_id, session_id, user_id, event_type, event_timestamp, utm_source, device_type, os, browser, ip, properties, page_type, page_url)
SELECT 
	number AS  event_id,
	if(
		 user_id IS NULL,
		 cityHash64(number, intDiv(number, 10)),
    	 cityHash64(user_id, intDiv(number, 10))
	) AS session_id,
	user_id,
	'page_view' AS event_type,
	now() - toUInt32(rand() % (90*24*60*60))   AS event_timestamp,
	arrayElement(['google', 'yandex', 'vk', 'direct', NULL], rand() % 5 + 1) AS  utm_source,
	'mobile'  device_type,
	arrayElement(['ios', 'android'], rand() % 2 + 1) AS os,
  	arrayElement(['opera', 'google chrome', 'safari', 'yandex'], rand() % 4 + 1) AS browser,
  	concat(
        toString(rand() % 256), '.',
        toString(rand() % 256), '.',
        toString(rand() % 256), '.',
        toString(rand() % 256)
    ) AS ip,
    '{}' AS properties,
    arrayElement(['home','catalog','product', 'cart','checkout'], 1 + rand() % 5) AS page_type,
	'' AS page_url 
FROM 
(
	SELECT 
		number,
		rand() % 100 AS device_rand,
		if(rand() % 100 < 30, NULL, rand() % 10000)  as user_id
	FROM numbers(1000)
);

-- Проверим,изменилось ли количество.
SELECT COUNT(*) FROM silver.fact_sessions;  

--
SELECT COUNT(*) FROM silver.fact_sessions WHERE processed_at > now() - INTERVAL 2 MINUTE;