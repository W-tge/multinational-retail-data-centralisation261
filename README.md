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


