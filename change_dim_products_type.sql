BEGIN;

-- Rename the 'removed' column to 'still_available'
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

-- Change data types of the specified columns
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN "weight (Kg)" TYPE FLOAT USING "weight (Kg)"::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17), -- Updated to 17 characters based on the data
    ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11), -- Updated to 11 characters based on the data
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END,
    ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14); -- Updated to 14 characters based on the data

COMMIT;
