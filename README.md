
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

1. Run airflow standalone container :
    ```
    docker compose up --detach
    ```
2. Check generated username and password (for Airflow UI login) :
    ```
    docker logs airflow | grep username
    ```

## Requirements

- Apache Airflow
- Pendulum