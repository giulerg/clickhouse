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