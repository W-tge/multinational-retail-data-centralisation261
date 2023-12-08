ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::varchar,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar,
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::varchar,
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::smallint;
