# data-pipeline_currency-exchange

---

This project constructs a data pipeline for daily update of euro values (value in terms of euro currency) and exchange rates for all currencies using Google Cloud BigQuery and Airflow. The pipeline is followed by detailed data analysis.

---

---
## Airflow Pipeline
---

## Setup 

* Running Airflow
* Create a service account (Cloud Console)
* Setup a Google Cloud Connection in Airflow
* Enter the config variables


### Running Airflow

- Start the Airflow environment with the docker compose file

```
docker-compose -f docker-compose-bquery.yml up
```

- Stop the Airflow environment when you are finished

```
docker-compose -f docker-compose-bquery.yml down
```

### Google Cloud Service Account

Create the service account. It is mandatory to create a private key. This remote repository omits it for security reasons.

Make sure the JSON private key has Editor's rights. Also, the service account needs to have permission to access the GCS bucket and Bigquery dataset.

### Airflow GCP Connection

After having the GCP key, we need to create a connection in `Admin -> Connections` using your key.

In Airflow we need to define the *my_conn_id* named connection to your project:

Supply the path to your downloaded private key, supply the *project_id* and define the minimum scope of *https://www.googleapis.com/auth/cloud-platform*

### Enter the config variables

After connection has been set up, we need to upload the config variables in `Admin -> Variables`

Then, you can go to the [Data Pipeline DAG](./airflow-pipeline/data_pipeline.py), and enter the value of config variables:
- __project_id__: the bigquery project you are working on
- __bq_conn_id__: the bigquery connection id
- __dataset_id__: the bigquery dataset you are working on


### Test the DAG

After connection and config variables has been set up, you can now test and run your DAG. 

- Using the command below to test specific task in the DAG:

```
docker-compose -f docker-compose-bquery.yml run --rm webserver airflow test [DAG_ID] [TASK_ID] [EXECUTION_DATE]
```

- Examples: 

```
# Task 1
docker-compose -f docker-compose-bquery.yml run --rm webserver airflow test bigquery_github_trends bq_check_githubarchive_day 2018-12-01

# Task 2
docker-compose -f ddocker-compose-bquery.yml run --rm webserver airflow test bigquery_github_trends bq_check_hackernews_full 2018-12-01
```

---
## Data Analysis
---

First, create a python virtual environment

```
python3 -m venv airflow_venv
```
Then, activate the environment

```
source airflow_venv/bin/activate
```

install the analysis_requirement.txt

```
(airflow_venv) pip3 install -r analysis_requirement.txt
```
Finally, the analysis could be performed using the Jupyter Notebook Data_Analysis (./data_analysis.ipynb)

To deactivate the virtual environment, just type

```
deactivate
```



