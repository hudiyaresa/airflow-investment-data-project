
# Case Live Class Data Orchestrations - Week 1

## Directory Structure
```bash
.
├── dags/
│   ├── create_and_copy_files/
│   │   ├── run.py
│   │   └── __init__.py
│   ├── date_generators/
│   │   ├── run.py
│   │   └── __init__.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── start.sh
├── .gitignore
└── .env
```
## DAGs Overview

### 1. Create and Copy Files

- **DAG ID**: `create_and_copy_files`
- **Start Date**: September 1, 2024
- **Schedule**: Once
- **Tasks**:
  - Create source directory
  - Create destination directory
  - Create a date file in the source directory
  - Copy the date file to the destination directory

### 2. Date Generators

- **DAG ID**: `date_generators`
- **Start Date**: September 1, 2024
- **Schedule**: Daily
- **Tasks**:
  - Generate a logical date entry and append it to the date file in the source directory
  - Copy the updated date file to the destination directory

## Usage

1. Ensure Airflow is installed and configured.
2. Create `.env` file.
    ```
    AIRFLOW_DB_URI=...
    AIRFLOW_DB_USER=...
    AIRFLOW_DB_PASSWORD=...
    AIRFLOW_DB_NAME=...

    PROFILE_QUALITY_DB_USER=...
    PROFILE_QUALITY_DB_PASSWORD=...
    PROFILE_QUALITY_DB_NAME=...

    DELLSTORE_DB_USER=...
    DELLSTORE_DB_PASSWORD=...
    DELLSTORE_DB_NAME=...

    MINIO_ROOT_USER=...
    MINIO_ROOT_PASSWORD=...
    ```
2. Place the DAG files in the appropriate `dags/` directory.
3. Start the Airflow scheduler and web server.
4. Trigger the DAGs from the Airflow UI or let them run based on their schedules.

## Requirements

- Apache Airflow
- Pendulum