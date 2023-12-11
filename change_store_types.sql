ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(255), -- Adjust the VARCHAR length as needed.
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::float, -- Assuming the remaining latitude column is the correct one.
ALTER COLUMN country_code TYPE VARCHAR(10), -- Adjust the VARCHAR length as needed.
ALTER COLUMN continent TYPE VARCHAR(255);
