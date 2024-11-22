
```markdown
# Amazon Books Data Fetcher and Postgres Loader

This project is an Airflow Directed Acyclic Graph (DAG) to fetch book data from Amazon, transform the data, and load it into a PostgreSQL database. The process includes three main tasks: fetching data (extract), cleaning the data (transform), and loading the cleaned data into a PostgreSQL database.

## Requirements

- Apache Airflow
- PostgreSQL
- Redis (for CeleryExecutor)
- Python 3.x
- Necessary Python libraries:
  - `requests`
  - `pandas`
  - `beautifulsoup4`
  - `airflow`

You can use the provided Docker Compose file to run Airflow, Redis, and PostgreSQL services.

## DAG Overview

### Tasks:

1. **Fetch Amazon Data (Extract)**  
   Fetch book data from Amazon, including the title, author, price, and rating for a specified number of books.  
   - **Operator**: PythonOperator  
   - **Function**: `get_amazon_data_books`

2. **Clean Data (Transform)**  
   The fetched data is cleaned by removing duplicates based on book titles.  
   - **Operator**: PythonOperator  
   - **Function**: `get_amazon_data_books`

3. **Create and Store Data in Postgres (Load)**  
   Create a table in PostgreSQL if it doesn't exist, and store the cleaned book data in the table.  
   - **Operator**: PostgresOperator  
   - **Hook**: `PostgresHook`  
   - **Function**: `insert_book_data_into_postgres`

### Operators Used:

- **PythonOperator**: Used for tasks that require custom Python code (fetching Amazon data, cleaning data).
- **PostgresOperator**: Used to execute SQL queries for creating tables and inserting data into PostgreSQL.
  
### Hooks Used:

- **PostgresHook**: Allows connection to PostgreSQL to execute SQL queries.

### Dependencies:

1. Fetch data (`fetch_book_data_task`)
2. Create table in Postgres (`create_table_task`)
3. Insert the cleaned data into Postgres (`insert_book_data_task`)

### DAG Execution Flow:

```
fetch_book_data_task >> create_table_task >> insert_book_data_task
```

## Setting Up PostgreSQL

To run the DAG, you need a PostgreSQL connection set up in Airflow. Ensure you have a connection ID `books_connection` configured to connect to your PostgreSQL instance. If you don't have a connection, create one in the Airflow UI with the following details:

- **Host**: Your PostgreSQL host (e.g., `localhost` or `postgres`)
- **Schema**: Your database schema (e.g., `airflow`)
- **Login**: PostgreSQL user (e.g., `airflow`)
- **Password**: PostgreSQL password (e.g., `airflow`)

## Airflow Configuration

Ensure that you have the following environment variables set for your Airflow configuration:

- `AIRFLOW_UID`: The user ID for Airflow containers (default: 50000)
- `AIRFLOW_PROJ_DIR`: Base path for the project files (default: `.`)

To run the project with Docker, use the provided `docker-compose.yml` configuration to set up the necessary services (Airflow, Redis, PostgreSQL, PgAdmin).

## Running the Project

1. Build the Docker containers by running:

   ```bash
   docker-compose up --build
   ```

2. Once the containers are running, navigate to Airflow's web UI at `http://localhost:8080` and trigger the DAG `fetch_and_store_amazon_books`.

3. You can also monitor the task execution through the Airflow UI.

## Docker Setup

The provided `docker-compose.yml` file will set up the following services:

- **PostgreSQL**: For storing the fetched book data.
- **PgAdmin**: For managing your PostgreSQL instance through a web interface.
- **Redis**: Used by the CeleryExecutor in Airflow for task queueing.
- **Airflow (Webserver, Scheduler, Worker, Triggerer)**: For managing and executing the tasks in the DAG.

To configure Airflow and PostgreSQL services, modify the `.env` file or the `docker-compose.yml` as needed.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for more information.

## Acknowledgments

- [Apache Airflow](https://airflow.apache.org/)
- [Requests](https://docs.python-requests.org/en/latest/)
- [Pandas](https://pandas.pydata.org/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)

```

This `README.md` outlines the core functionality of your project, the setup instructions, and a brief overview of the services and dependencies. Adjust it according to your needs.