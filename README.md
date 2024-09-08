# Case Live Class Data Orchestrations - Week 2

## Overview
This project is an Apache Airflow pipeline designed for profiling data quality. It extracts data from a source database, processes it, and loads it into a target database while ensuring data quality and profiling.

## DAGs Overview
The project contains two main Directed Acyclic Graphs (DAGs):
### 1. Profiling Quality Init
- **DAG ID**: `profiling_quality_init`
- **Start Date**: September 1, 2024
- **Schedule**: Once
- **Purpose**: Initializes the data profiling process.
- **Tasks**:
    - **Create Function** : Executes a SQL script to create a function (data_profile_quality) in the dellstore_db database. This function generates a data profile and quality report for all tables in the database.
    - **Create Bucket** : Checks if a MinIO bucket named data-profile-quality exists. If not, it creates the bucket to store data profiles.
    - **Create Table** : Executes a SQL script to create a table (data_profile_quality) in the profile_quality_db database to store the profiling results.

### 2. Profiling Quality Pipeline
- **DAG ID**: `profiling_quality_pipeline`
- **Start Date**: September 1, 2024
- **Schedule**: Daily
- **Purpose**: Extracts data from the source database, transforms it, and loads it into the target database while ensuring data profile and quality.
- **Tasks**:
    - **Extract** : Retrieve data from the data_profile_quality table in the dellstore_db database. The data is processed and saved as a CSV file in the MinIO bucket.
    - **Transform and Load** : Read the CSV file from MinIO, adds additional columns (person_in_charge and source), and inserts the transformed data into the profile_quality_db database.
## Requirements
The project requires the following Python packages:
- `pandas`
- `pendulum`
- `minio`

These dependencies are specified in the `requirements.txt` file.

## Usage

1. Run airflow standalone container :
    ```
    docker compose up --detach
    ```
2. Check generated username and password (for Airflow UI login) :
    ```
    docker logs airflow | grep username
    ```
3. Login to airflow UI. Open your browser with address http://localhost:8080
4. Unpaused `profiling_quality_init` Dags
5. Unpuased `profiling_quality_pipeline` Dags
## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.