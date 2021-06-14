'''
====================
@author = Sanjeet Maisnam
sanjeetmaisnam122@gmail.com

We first create a connection to BigQuery and create a dataset (database) there.
We then define the list of python tasks to be used later as DAG task.
====================
'''



'''
========================
Airflow Scheduling
========================
'''

# import modules

import airflow
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator

from datetime import datetime, timedelta

import pandas_gbq
import pandas as pd


# Config variables
dag_config = Variable.get("bigquery_variables", deserialize_json=True)
project_id = dag_config["project_id"]
bq_conn_id = dag_config["conn_id"]
dataset_id = dag_config["dataset"]


#raw_data table_id
raw_data_table_id = "raw_data"

# default args
default_args = {
    'owner': 'Sanjeet Maisnam',
    'depends_on_past': False,
    'start_date': datetime(1999, 1, 4),
    'email': ['sanjeetmaisnam122@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2021, 6, 9),
}

# DAG
dag = DAG(
    'blablacar_dag',
    default_args=default_args,
    description='DAG to create the tables of blablacar in BigQuery',
    schedule_interval= '@daily', #or timedelta(days=1), #daily execution
)

#Tasks
t_start = BashOperator(
    dag=dag,
    task_id="start",
    bash_command="echo 'Start'"
)

'''
Task 1 = Query the database to see if the data exists based on the execution date
'''

t1 = BigQueryCheckOperator(
        task_id='check_daily_existence',
        sql='''
        #bigquerySQL
        SELECT
            *
        FROM
            `{0}.{1}.{2}`
        WHERE
            Metrics_Dates = {{ ds_nodash }}
        '''.format(project_id, dataset_id, raw_data_table_id),
        use_legacy_sql=False,
        bigquery_conn_id=bq_conn_id,
        dag=dag
    )


'''
Task 2 = Read the daily data of euro_value based on the execution date and create the dim_currency table
'''

#python function to collect the data, clean, and upload to a new table
dim_currency_table_id = "dim_currency"

def get_data():
    sql = """
    SELECT * FROM 
    `{0}.{1}.{2}` 
    WHERE
    Metrics_Dates = {{ ds_nodash }}
    """.format(project_id, dataset_id, raw_data_table_id)

    return pandas_gbq.read_gbq(sql, project_id=project_id)


def clean_data(input_data):
    input_data = input_data.fillna("-")
    input_data = input_data.replace("-", "1.00")
    input_data = input_data.iloc[0][1:].apply(lambda x: float(str(x).replace(",", ".")))

    return input_data

def get_code_serie():
    sql = """
    SELECT * FROM
    `{0}.{1}.{2}`
    where
    not AUD = "Dollar australien (AUD)"
    limit
    1
    """.format(project_id, dataset_id, raw_data_table_id)

    return pandas_gbq.read_gbq(sql, project_id=project_id)


def create_dim_table(input_data):
    data_dict = input_data.to_dict()
    tmp_df = pd.DataFrame(
        {
            "curr_code": pd.Series(data_dict.keys()),
            "one_euro_value": pd.Series(data_dict.values()),
            "last_updated_date": {{ds_nodash}},
            "Serial_code": get_code_serie(),
        }
    )



def upload_table(table):

    pandas_gbq.to_gbq(table, f"{dataset_id}.{dim_currency_table_id}", project_id=project_id, if_exists='replace')
    return None



def create_dim_curr():
    daily_data = get_data()
    clean_version = clean_data(daily_data)
    dim_table = create_dim_table(clean_version)
    dim_curr = upload_table(dim_table)
    return dim_curr



t2 = PythonOperator(
    dag=dag,
    task_id='get_daily_euro_value',
    python_callable=create_dim_curr,

)




'''
Task 3 = Retrieve the dim_curr table data and create table fact_exchange_rate_history
'''
table_name = "fact_exchange_rate_history"

#retrieve the daily euro_value table

def get_daily_euro_value():
    sql = """
    SELECT * FROM 
    `{0}.{1}.{2}` 
    WHERE
    last_updated_date = {{ ds_nodash }}
    """.format(project_id, dataset_id, dim_currency_table_id)

    return pandas_gbq.read_gbq(sql, project_id=project_id)


# function to generate euro value of currencies for a date

def generate_curr_euro_value_by_date(date):

    dim_currency_df = get_daily_euro_value()
    currency_codes = dim_currency_df["curr_code"].unique()
    curr_from = dict.fromkeys(currency_codes, None)

    for key, value in curr_from.items():
        curr_from[key] = dim_currency_df[(dim_currency_df["last_updated_date"] == date)
                                         & (dim_currency_df["curr_code"] == key)]["one_euro_value"].to_list()[0]

    return curr_from

#function for exchange rate for one currency for a date
def exchange_rate_one_curr(base_curr, curr_euro_value_dict):
    result = {}
    for key, val in curr_euro_value_dict.items():
        if key != base_curr:
            result[key] = curr_euro_value_dict[base_curr]/val
    return result



def create_daily_exchange_rate():

    t_df = pd.DataFrame()

    # generate the euro value of currencies for each date
    currencies_euro_value = generate_curr_euro_value_by_date({{ ds_nodash }})

    # exchange rates between the currencies for a date
    for key, value in currencies_euro_value.items():
        tm_df = pd.DataFrame(
            {
                "history_date": {{ ds_nodash }},
                "from_cur_code": key,
                "to_cur_code": pd.Series(exchange_rate_one_curr(key, currencies_euro_value).keys()),
                "exchange_rate": pd.Series(exchange_rate_one_curr(key, currencies_euro_value).values()),
            }
        )

        t_df = pd.concat([t_df, tm_df], ignore_index=True)

    pandas_gbq.to_gbq(t_df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='append')

    return None

t3 = PythonOperator(
    dag=dag,
    task_id='get_daily_exchange_value',
    python_callable=create_daily_exchange_rate,

)




#task to complete
t_end = BashOperator(
    dag=dag,
    task_id="completed",
    bash_command="echo 'Completed'"
)

# dependencies

t_start >> t1 >> t2 >> t3 >> t_end


