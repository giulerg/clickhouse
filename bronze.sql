--v

SELECT version();

--Создание бд 
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS  gold;

--Создание таблиц
CREATE TABLE IF NOT EXISTS bronze.raw_app_events (
	event_id UInt64,
 	session_id UInt64,
 	user_id Nullable(UInt64),
	event_type LowCardinality(String), 
	event_timestamp DateTime,
	page_url String,
	page_type LowCardinality(String),
	device_type LowCardinality(String),
	os LowCardinality(String),
    browser LowCardinality(String),
    ip String,
    utm_source LowCardinality(Nullable(String)),
    utm_medium LowCardinality(Nullable(String)),
    utm_campaign Nullable(String),
    properties  String,
	product_id Nullable(UInt64),
	category_id Nullable(UInt64),
	loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION   BY toYYYYMM(event_timestamp)
ORDER BY (event_type, toDate(event_timestamp), event_id);

--orders

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
 	order_id        String,
    user_id         String,
    order_status    LowCardinality(String),
    created_at      DateTime,
    paid_at         Nullable(DateTime),
    delivered_at    Nullable(DateTime),
    returned_at     Nullable(DateTime),
    gross_amount    Decimal(18,2),
    discount_amount Decimal(18,2),
    net_amount      Decimal(18,2),
    shipping_cost   Decimal(18,2),
    currency        FixedString(3),
    source_channel  LowCardinality(String),
    promo_code      Nullable(String),
    loaded_at      DateTime DEFAULT now()
)
ENGINE =  ReplacingMergeTree(loaded_at)
PARTITION   BY toYYYYMM(created_at)
ORDER BY (order_status, toDate(created_at), order_id);

-- загрузка тестовых данных 1000000 строк

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
	arrayElement( ['page_view', 'search', 'product_view', 'add_to_cart', 'checkout_start', 'purchase'], rand() % 6 + 1) AS event_type,
	  AS event_timestamp,
	arrayElement(['google', 'yandex', 'vk', 'direct', NULL], rand() % 5 + 1) AS  utm_source,
	CASE
		WHEN device_rand < 60 THEN 	'mobile' 
		WHEN  device_rand <  95   THEN 'desktop'  
		ELSE 'tablet'
	END AS device_type,
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
	FROM	numbers(1000000)
)

-- генерация заказов
INSERT INTO bronze.raw_orders (order_id, order_status, currency, source_channel, gross_amount, shipping_cost,
	discount_amount, user_id, paid_at, delivered_at, returned_at)
	
SELECT
	 number AS order_id,
	 order_status,
	'RUB' AS currency,
	'direct' AS source_channel,
	 toDecimal64(500 + rand() % 49500, 2)  AS gross_amount,
	 toDecimal64(100 + rand() % 900, 2)  AS shipping_cost,
	 toDecimal64((500 + rand() % 49500) * (rand() % 20) / 100, 2) AS discount_amount,
	 if(rand() % 100 < 10, NULL, rand() % 10000)  as user_id,
	 now() - toUInt32(rand() % (90*24*60*60)) AS paid_at,
	 IF(order_status='delivered', now() - toUInt32(rand() % (90*24*60*60)), NULL) AS delivered_at,
	 IF(order_status='returned', now() - toUInt32(rand() % (90*24*60*60)), NULL) AS returned_at 
FROM 
(
	SELECT number,
	arrayElement(['paid', 'delivered', 'returned'], 1+ rand() % + 3) AS order_status 
	FROM numbers (200000)
)

 
     
 
     
  
   