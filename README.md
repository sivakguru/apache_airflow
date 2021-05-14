# Airflow 2.0.2 Ubuntu Installation
### **An introduction to Apache airflow for beginers**
---
Before installing, we can set up the default airflow home address:

```bash
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=~/airflow
```

First install the following dependencies:

```bash
sudo apt-get install libmysqlclient-dev
```
*(dependency for airflow[mysql] package)*

```bash
sudo apt-get install libssl-dev
```
*(dependency for airflow[cryptograph] package)*

```bash
sudo apt-get install libkrb5-dev
```
*(dependency for airflow[kerbero] package)*

```bash
sudo apt-get install libsasl2-dev
```
*(dependency for airflow[hive] package)*

### **Then airflow Installation**
---

```bash
AIRFLOW_VERSION=2.0.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.8
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.6.txt
pip install --upgrade "apache-airflow[postgres,celery,redis,tableau]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

```bash
sudo apt-get install libpq-dev python-dev
sudo pip install psycopg2
```
*(dependency for psycopg2)*

Check if the installation has completed
```bash
airflow version  # -> 2.0.2
```

Configure Airflow
By default, Airflow will look for its configuration in a file located at
```bash
$HOME/airflow/airflow.cfg
```

1. **Metadata database connection:** We need to tell Airflow how to access its metadata database, which we do by setting the sql_alchemy_conn value. That will point to the local Postgres installation we previously configured.
1. **Webserver authentication:** Airflow supports a few different authentication schemes, but we'll use the default which just requires a username and password.
1. **Celery Executor selection:** Airflow uses the SequentialExecutor by default, but we'll change that to use the more useful CeleryExecutor.


```bash
[core]

# Connection string to the local Postgres database
sql_alchemy_conn = postgresql://airflow:password@localhost:5432/airflow

# Class name of the executor
executor = CeleryExecutor


[webserver]

# Run a single gunicorn process for handling requests.
parallelism = 8
dag_concurrency = 4
max_active_runs_per_dag = 4
workers = 4
worker_concurrency = 4
worker_autoscale = 4,2

[celery]

broker_url = redis://localhost:6379/0
result_backend = sqla+postgres://airflow:password@localhost:5432/airflow
```

```bash
# initialize the database
airflow db init
```

## **user creation**
---
```bash
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
    --password <password>
```

### **Start the airflow services**
---
For our installation, we'll need to run three Airflow services: the Webserver, the Scheduler, and the Worker.

1. **The scheduler** is responsible for determining whether a DAG should be executed.
1. **The webserver** is responsible for creating the DAG visualizations and "front end" that you can access from the web browser. If your DAGs are not running, it is likely an issue with the scheduler and not the webserver.
1. **The worker** is responsible for actually executing the jobs in a DAG. After a working finishes running a DAG's job, it will log the status of the job in the Airflow metadata database.

```bash
airflow webserver --daemon
airflow scheduler --daemon
airflow worker --daemon
```
