DELETE FROM orders_table
WHERE (user_uuid IS NOT NULL AND user_uuid NOT IN (SELECT user_uuid FROM dim_users))
   OR (card_number IS NOT NULL AND card_number NOT IN (SELECT card_number FROM dim_card_details))
   OR (store_code IS NOT NULL AND store_code NOT IN (SELECT store_code FROM dim_store_details))
   OR (product_code IS NOT NULL AND product_code NOT IN (SELECT product_code FROM dim_products));
