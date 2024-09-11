# Project Overview

This project aims to streamline the process of extracting, transforming, and loading data from diverse sources into a staging database for advanced analysis and processing. Apache Airflow is leveraged to manage and orchestrate the workflows involved in the ETL process, ensuring a seamless and efficient data integration process.

## Project Structure

The project is structured into two primary components:

1. **etl_init**: This component is responsible for initializing the staging database by creating the necessary tables and setting up the environment for data extraction and loading. This includes database schema creation, data type definition, and indexing for optimal performance.
2. **etl_pipeline**: This component is the core of the project, responsible for extracting data from various sources (databases, APIs, and spreadsheets), transforming the data as needed, and loading it into the staging database. This involves data cleansing, data mapping, and data validation to ensure data consistency and integrity.

## Technologies Used

The project leverages the following technologies to achieve its objectives:

* **Apache Airflow**: For workflow management and orchestration, ensuring a scalable and fault-tolerant ETL process.
* **Python**: For scripting and data processing, utilizing its extensive libraries for data manipulation and analysis.
* **PostgreSQL**: For database operations, providing a robust and scalable relational database management system.
* **Minio**: For object storage, offering a highly available and durable storage solution for large datasets.
* **Google Sheets API**: For spreadsheet data extraction, enabling seamless integration with Google Sheets for data retrieval.
* **Requests library**: For API data extraction, simplifying HTTP requests for data retrieval from APIs.
* **Pandas**: For data manipulation and analysis, providing a powerful library for data processing and analysis.

## How to Use

To utilize this project effectively, follow these steps:

1. **Dependency Installation**: Ensure all necessary dependencies are installed, including Apache Airflow, PostgreSQL, Minio, and the required Python libraries.
2. **Airflow Configuration**: Configure the necessary connections in Airflow, including the PostgreSQL database, Minio object storage, and Google Sheets API.
3. **etl_init DAG Trigger**: Trigger the **etl_init** DAG to initialize the staging database, ensuring all necessary tables and schema are created.
4. **etl_pipeline DAG Trigger**: Trigger the **etl_pipeline** DAG to start the ETL process, extracting, transforming, and loading data into the staging database.

## Contributing

Contributions to this project are highly valued. If you'd like to contribute, please follow these steps:

1. **Repository Forking**: Fork the repository to create a copy for modification.
2. **Branch Creation**: Create a new branch for your feature or fix to isolate your changes.
3. **Changes and Commit**: Make your changes and commit them with a descriptive message.
4. **Branch Push**: Push your branch to your forked repository.
5. **Pull Request**: Submit a pull request to the original repository, detailing your changes and their impact.

## License

This project is licensed under the Apache License 2.0, ensuring it is open-source and freely available for use and modification.
