# Extend the official Airflow image
FROM apache/airflow:3.0.0-python3.12

# Install uv
USER root
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Install polars
# RUN pip install --no-cache-dir polars

# Back to airflow user
USER airflow
WORKDIR /opt/airflow

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Sync deps into system Python
RUN uv pip install -r pyproject.toml --no-cache-dir 

# Optional: Copy DAGs/plugins
COPY ./dags ./dags
COPY ./plugins ./plugins