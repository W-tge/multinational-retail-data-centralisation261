BEGIN;

-- Change the data types of the specified columns to their required types with the appropriate lengths
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(10),
    ALTER COLUMN year TYPE VARCHAR(10),
    ALTER COLUMN day TYPE VARCHAR(10),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

COMMIT;
