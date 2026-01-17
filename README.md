# 🏀 NBA Airflow

> A production-ready data orchestration platform for NBA analytics

An enterprise-grade Apache Airflow solution that automates the extraction, transformation, and loading of NBA statistical data into a scalable data warehouse. Built with performance and maintainability in mind, this project demonstrates modern data engineering best practices.

---

## 🎯 What This Project Does

Transform raw NBA data into actionable insights through automated, scheduled workflows. The system intelligently orchestrates multi-stage data pipelines that fetch player statistics, team performance metrics, and seasonal data, then processes and stores everything in an optimized columnar format for rapid analytics.

**Key Results:**
- ⚡ Fully automated data collection and processing
- 📊 Real-time pipeline monitoring and alerting
- 💾 Petabyte-scale storage with efficient Parquet compression
- 🔄 Fault-tolerant, resumable workflows

---

## 🏗️ Architecture & Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | Workflow scheduling, monitoring, and failure recovery |
| **Data Processing** | Polars | Lightning-fast DataFrame operations |
| **Cloud Storage** | AWS S3 + Boto3 | Scalable, cost-effective data lake |
| **Containerization** | Docker & Compose | Reproducible environments, easy deployment |

---

## ✨ Key Features

✅ **Modular Pipeline Design** – Reusable DAG components for players, teams, and seasons  
✅ **Dynamic Configuration** – Environment-based settings via `parameters.yml`  
✅ **Cloud-Native Storage** – S3 integration for distributed, durable data storage  
✅ **Observable & Maintainable** – Comprehensive logging and error handling  
✅ **Development-Ready** – Docker Compose setup for instant local development  

---

## 📁 Project Structure

```
├── dags/                    # Airflow DAGs
│   ├── players/            # Player statistics workflows
│   ├── teams/              # Team performance workflows
│   ├── seasons/            # Seasonal data aggregation
│   ├── games/              # Game-level data
│   └── config/             # S3 & database configuration
├── config/                 # Airflow configuration
├── tests/                  # Unit & integration tests
├── docker-compose.yaml     # Local environment setup
└── Dockerfile              # Container image definition
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- `uv` package manager

### Installation & Setup

```bash
# Clone the repository
git clone <repository-url>
cd nba-airflow

# Install dependencies
uv sync

# Start Airflow with Docker Compose
docker compose up -d

# Access Airflow UI
# Navigate to http://localhost:8080
```

### Stopping the Services

```bash
docker compose down
```

---

## 💡 Use Cases

- 📈 **Real-time Analytics Dashboards** – Power BI/Tableau visualizations
- 📊 **Statistical Analysis** – Detect trends and patterns in player performance
- 🤖 **Machine Learning Pipelines** – Feed clean, preprocessed data to ML models
- 📋 **Business Intelligence** – Automated reporting and KPI tracking

---

## 🔧 Development

### Running Tests

```bash
pytest tests/
```

### View Logs

```bash
docker compose logs -f airflow-webserver
```

---

## 📜 License

MIT License – Feel free to use this project as a reference or foundation for your own data engineering work.