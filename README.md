# Multinational Retail Data Centralisation Project

## Project Summary
This project aims to centralize data management for a multinational retail chain. It automates the extraction of data from various sources like APIs, AWS S3 buckets, and PDF documents, processes and cleans the data through a defined pipeline, and then uploads the refined datasets to a PostgreSQL database. This centralized approach is designed to streamline analytics and decision-making processes for the retail chain.

## Files Description
- `main.py`: Orchestrates data processing and database operations.
- `database_utils.py`: Manages database connections.
- `data_extraction.py`: Extracts data from different sources.
- `data_cleaning.py`: Cleans and prepares data for database insertion.
- `requirements.txt`: Lists Python package dependencies.

## Configuration
Ensure that your database credentials are secure and properly configured in `db_creds.yaml` and `local_db.yaml`.

## Execution Order
To properly set up and run the project, follow the steps below in the given order:

1. Run `main.py` to orchestrate the data processing and database operations.
2. Execute all SQL files beginning with 'change_' in the following order to modify the database schema:
   - `change_dim_card_details.sql`
   - `change_dim_date_time.sql`
   - `change_dim_products_table.sql`
   - `change_dim_products_type.sql`
   - `change_dim_users_types.sql`
   - `change_orders_table_types.sql`
   - `change_store_types.sql`
3. After the schema changes, run `delete_rows.sql` to remove any rows that may violate foreign key constraints.
4. Execute `primary_keys.sql` to establish primary keys within the dimension tables.
5. Finally, run `star_schema.sql` to add foreign key constraints and complete the star schema setup.

Please ensure that the SQL files are executed in the correct sequence as they build upon the changes made by the previous files. Failure to follow the order may result in errors or incomplete setup.
