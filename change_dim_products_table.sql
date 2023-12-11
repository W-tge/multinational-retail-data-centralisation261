BEGIN;

-- Add a new column for weight class
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(255);

-- Update the new weight_class column based on weight ranges
UPDATE dim_products
SET weight_class = CASE
    WHEN "weight (Kg)" < 2 THEN 'Light'
    WHEN "weight (Kg)" >= 2 AND "weight (Kg)" < 40 THEN 'Mid_Sized'
    WHEN "weight (Kg)" >= 40 AND "weight (Kg)" < 140 THEN 'Heavy'
    WHEN "weight (Kg)" >= 140 THEN 'Truck_Required'
    ELSE 'Unknown' -- Handles any cases that do not fit into the above categories
END;

-- Remove the £ symbol from product_price and convert to FLOAT
UPDATE dim_products
 SET product_price = REPLACE(product_price, '£', '')::FLOAT;

COMMIT;
