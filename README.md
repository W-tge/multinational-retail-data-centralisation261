# Multinational Retail Data Centralisation Project

## Table of Contents
- [Project Summary](#project-summary)
- [Files Description](#files-description)
- [Configuration](#configuration)
- [Execution Order](#execution-order)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)
- [License](#license)
- [Progress](#progress)

## Project Summary
This project aims to centralize data management for a multinational retail chain. It automates the extraction of data from various sources like APIs, AWS S3 buckets, and PDF documents, processes and cleans the data through a defined pipeline, and then uploads the refined datasets to a PostgreSQL database. This centralized approach is designed to streamline analytics and decision-making processes for the retail chain. The project also includes extensive SQL querying to generate insights and reports from the processed data.

## Files Description
- `main.py`: Orchestrates data processing and database operations.
- `database_utils.py`: Manages database connections.
- `data_extraction.py`: Extracts data from different sources.
- `data_cleaning.py`: Cleans and prepares data for database insertion.
- `requirements.txt`: Lists Python package dependencies.
- `data_querying/`: Contains SQL files used for querying the database and generating reports.

## Configuration
Ensure that your database credentials are secure and properly configured in `db_creds.yaml` and `local_db.yaml`.

## Execution Order
To properly set up and run the project, follow the steps below in the given order:

1. Run `main.py` to orchestrate the data processing and database operations.
2. Execute all SQL files in the `data_querying` folder in the following order to modify the database schema:
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

## Installation Instructions
To set up this project, clone the repository and install the required dependencies listed in `requirements.txt` using the command:

'pip install -r requirements.txt'

## Usage Instructions
After installation, run the following command to start the data processing:

'python main.py'

Then, execute the SQL scripts in the `data_querying` folder using your preferred SQL management tool.

## File Structure
The project is structured as follows:
- `data_querying/`: Contains all SQL scripts for database operations.
- `src/`: Contains all the Python scripts for data extraction and cleaning.
- `configs/`: Contains YAML configuration files for database connections.

## License
This project is open-sourced this means you are free to use, modify, and distribute the work as you see fit. 


