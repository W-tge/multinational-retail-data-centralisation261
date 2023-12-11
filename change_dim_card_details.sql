-- Start a transaction
-- NB Running this code more than once without rerunning main.py will cause an error
BEGIN;

-- Normalize the 'date_payment_confirmed' dates to a consistent format
-- For dates like 'December 2021 17', assuming the day is always at the end
UPDATE dim_card_details
SET date_payment_confirmed = TO_DATE(date_payment_confirmed, 'FMMonth YYYY DD')
WHERE date_payment_confirmed ~ '^[A-Za-z]+ \d{4} \d{1,2}$';

-- For dates like '2005 July 01', assuming the day always has two digits
UPDATE dim_card_details
SET date_payment_confirmed = TO_DATE(date_payment_confirmed, 'YYYY FMMonth DD')
WHERE date_payment_confirmed ~ '^\d{4} [A-Za-z]+ \d{1,2}$';

-- For dates like '2017/05/15', which are already in the correct format but using slashes
UPDATE dim_card_details
SET date_payment_confirmed = TO_DATE(date_payment_confirmed, 'YYYY/MM/DD')
WHERE date_payment_confirmed ~ '^\d{4}/\d{2}/\d{2}$';

-- For dates like 'May 1998 09', assuming the day always has two digits
UPDATE dim_card_details
SET date_payment_confirmed = TO_DATE(date_payment_confirmed, 'FMMonth YYYY DD')
WHERE date_payment_confirmed ~ '^[A-Za-z]+ \d{4} \d{1,2}$';

-- Alter the column types as per the task requirements
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Commit the transaction
COMMIT;
