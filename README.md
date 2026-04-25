# Databricks Binance Pipeline

This project is an automated data pipeline that extracts, processes, and loads cryptocurrency market data from the Binance API. It leverages Apache Airflow for orchestration and is containerized using Docker for easy deployment and scalability.

## Architecture & Workflow

The pipeline is designed with a modular ETL (Extract, Transform, Load) architecture:

1. **Extract (`src/ingestion/binance_api.py`)**:
   - Connects to the Binance API to fetch historical trades data for specified symbols (e.g., BTCUSDT, ETHUSDT).
   - Manages state by keeping track of the latest fetched timestamp to enable incremental data loads.
   - Saves the raw fetched data locally as temporary CSV files.

2. **Transform (`src/processing/process.py`)**:
   - Reads the raw CSV data.
   - Performs data cleaning operations such as casting data types and renaming columns based on predefined configurations.
   - Converts Unix timestamps into human-readable datetime formats.
   - Outputs the processed data into temporary CSV files for the next stage.

3. **Load (`src/destination/s3.py`)**:
   - Reads the transformed data.
   - Converts the dataset into an optimized columnar format (Parquet) using `fastparquet`.
   - Stores the Parquet files into a data lake storage layer (S3) partitioned by date.

## Orchestration

Apache Airflow orchestrates the pipeline execution.
- DAGs (e.g., `airflow/dags/btcusdt_dag.py`) are scheduled to run hourly.
- They execute the Extract, Transform, and Load scripts sequentially via the `BashOperator`.

## Infrastructure

The entire infrastructure is defined in `docker-compose.yaml`, utilizing the `CeleryExecutor` for Airflow to support distributed task execution. The stack includes:
- **Airflow Webserver & Scheduler**: For DAG management and scheduling.
- **Airflow Worker**: For executing pipeline tasks.
- **PostgreSQL**: Serving as the Airflow metadata database.
- **Redis**: Acting as the message broker for Celery.

## Deployment

The pipeline is deployed on an **AWS EC2 instance**. The Dockerized stack runs on the EC2 host, with the instance configured to expose the Airflow UI and communicate with S3 for data lake storage. AWS credentials are managed via environment variables (e.g., in a `.env` file) and are passed into the Docker containers at runtime.

## Prerequisites & Setup

To run this project locally, ensure you have Docker and Docker Compose installed.

1. Clone the repository.
2. Ensure you have your environment variables set up (e.g., in a `.env` file for AWS credentials).
3. Start the infrastructure:
```bash
   docker-compose up -d
```
4. Access the Airflow UI at `http://localhost:8081` (default credentials: airflow/airflow) and enable your DAGs.