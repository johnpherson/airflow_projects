# airflow_projects

## How to run locally

### Create virtual env
`python3.9 -m venv airflow_venv`

### Activate Virtual Env
`source airflow_venv/bin/activate`

### Install Airflow if nt already
`export AIRFLOW_VERSION=2.5.0
export PYTHON_VERSION="3.9"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`

### Start the Airflow scheduler
- `airflow scheduler`

### Start the Airflow web server
- `airflow webserver --port 8080`

### Access the UI
- http://localhost:8080


