# NBA Airflow

This project automates NBA data workflows using Apache Airflow. It is designed to extract, transform, and load NBA-related data efficiently for analytics and reporting.

## Features

- Automated data pipelines for NBA stats
- Scheduling and monitoring with Airflow
- Modular and extensible workflow design
- Interactions with Amazon S3 to store Parquet files for scalable and efficient data storage

## Main Libraries Used

- **Apache Airflow**: Workflow orchestration and scheduling
- **polars**: Data manipulation and analysis
- **boto3**: Interacting with Amazon S3 for file storage

## Getting Started

1. Clone the repository.
2. Install dependencies:  
    ```bash
    uv sync
    ```
3. Launch the Airflow through the docker-compose.yaml.

    To start:
    ```bash
    docker compose up -d
    ```

    To stop:
    ```bash
    docker compose down
    ```    

## License

MIT License