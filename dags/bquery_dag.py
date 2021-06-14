'''
====================
@author = Sanjeet Maisnam
sanjeetmaisnam122@gmail.com

We first create a connection to BigQuery and create a dataset (database) there.
We then define the list of python tasks to be used later as DAG task.
====================
'''

'''
Big Query connection
'''

#create bigquery connection
from google.cloud import bigquery
from google.oauth2 import service_account


import airflow
from airflow.models import Variable


#credentials
#key = service_account.Credentials.from_service_account_file('big-query-key.json')

# Config variables
dag_config = Variable.get("bigquery_variables", deserialize_json=True)
#key = dag_config["private_key_id"]
project_id = dag_config["project_id"]
bq_conn_id = dag_config["conn_id"]

#client = bigquery.Client(credentials=key, project=project_id, location="europe-west1")

#dataset and table ids
dataset_id = dag_config["dataset"]
#table_id = "currency_statistics"

#create dataset
#client.create_dataset(dataset_id)

# create a new table in the dataset
#client.create_table(f"{project_id}.{dataset_id}.{table_id}")

'''
Pre-requisites for the Python tasks: Data retrieval from the source file
'''
import pandas_gbq

'''
////
#retrieve the data from the file
import pandas as pd


data = pd.read_csv("./Webstat_Export_20210609.csv", delimiter=";")

#assign headers
headers = ["Metrics_Dates"]+[code_name.split(".")[2] for code_name in data.iloc[1][1:]]
data.columns = headers
'''

'''
Task 1 = upload the raw data to BigQuery
'''
'''
#python callable function to upload the date
def raw_data_upload():
   return pandas_gbq.to_gbq(data, f"{dataset_id}.{table_id}", project_id=project_id, if_exists= 'replace')
   ////
'''





'''
========================
Airflow Scheduling
========================
'''

#import modules



from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator

from datetime import datetime,timedelta




#default args
default_args = {
    'owner': 'Sanjeet Maisnam',
    'depends_on_past': False,
    'start_date': datetime(2007,3,20), #'2007-03-20', #airflow.utils.dates.days_ago(1),
    'email': ['sanjeetmaisnam122@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2007,3,23),
}


#DAG
dag = DAG(
    'trial_run',
    default_args=default_args,
    description='A simple trial run for DAG',
    schedule_interval=timedelta(days=1),
)




#tasks
t1 = BashOperator(
        dag=dag,
        task_id="start",
        bash_command="echo 'start'"
    )


'''
Task 2 = Query the database based on the execution date
'''

'''t2 = BigQueryCheckOperator(
        task_id='retrieve_daily_stats',
        sql='''        '''.format(project_id, dataset_id),
        use_legacy_sql=False,
        bigquery_conn_id=bq_conn_id,
        dag=dag
    )
    
        #bigquerySQL
        SELECT
            *
        FROM
            `{0}.{1}.raw_data`
        WHERE
            Metrics_Dates = {{ ds_nodash }}
'''

def get_data():
    sql = """
    SELECT 
        *
    FROM 
        `{0}.{1}.raw_data` 
    WHERE
        Metrics_Dates = {{ ds_nodash }}
    """.format(project_id, dataset_id)

    # data-integration-316423.currency_exchange

    return pandas_gbq.read_gbq(sql, project_id=project_id)



t2 = PythonOperator(
    dag=dag,
    task_id='trail_run_for_python_script',
    python_callable= get_data,

)

'''t2 = BigQueryGetDataOperator(
    task_id="get_data",
    dataset_id=dataset_id,
    table_id="raw_data",
    max_results=10,
    selected_fields="AUD,INR",
    dag = dag,
)'''

t3 = BashOperator(
        dag=dag,
        task_id="completed",
        bash_command="echo 'completed'"
    )

#dependencies

t1 >> t2 >> t3


