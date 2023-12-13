SELECT country_code, COUNT(*) AS num_stores_country
FROM orders_table
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY country_code
ORDER BY num_stores_country DESC;