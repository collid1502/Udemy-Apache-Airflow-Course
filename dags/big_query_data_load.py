"""
Create a DAG for loading data from a google storage bucket to BigQuery 
(BigQuery is like Redshift on AWS) 


extra notes:
Check airflow docs to find templated fields in tasks. If a field is templated, you can use airflow "Jinja" templating
to basically pass macros in (a bit like SAS) that contain values that callable within the task. This is useful for creating
parameterised DAGs
"""

# imports 
from airflow import DAG
from airflow.models import Variable  # used to access variables stored in the airflow env. See "variables/variables_in_airlfow.txt" for details 
from airflow.utils.dates import days_ago 
import os 


# set the GOOGLE_APPLICATION_CREDENTIALS environment variable to the JSON key path
# this will be needed for Airflow default google connection to work with GCP resources
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/c/Users/Dan/airflow/secrets/keyfile.json"

# check it worked 
gcp_api_cred = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
print(gcp_api_cred)

# import the existing operator for google cloud storage to Big Query 
# required a downgrade of `pandas-gbq` to package version = 0.14.1 
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator 

# import big query operator, which allows us to run a SQL in a big query database & write results to new table 
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators.python_operator import PythonOperator 
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook 


# accessing variables set in Airflow environment - see imported module above to utilise this 
# these variables can then be called in your DAG, instead of hard coding 
PROJECT_ID = Variable.get('project')  
LANDING_BUCKET = Variable.get('landing_bucket') 
BACKUP_BUCKET = Variable.get('backup_bucket') 


# define default arguments
default_arguments = {
    'owner': 'Dan Collins',
    'start_date': days_ago(1)
}

# create a function that will list objects within a passed google cloud bucket, to be called in DAG
def list_objects(bucket=None):
    hook = GoogleCloudStorageHook()
    storage_objects = hook.list(bucket) 
    return storage_objects 


# create another function, which uses a hook to move files from one GCP stoarge bucket to another
# we set **kwargs to access the XCom call method in this task 
def move_objects(source_bucket=None, destination_bucket=None, prefix=None, **kwargs):
    storage_objects = kwargs['ti'].xcom_pull(task_id='list_files')  # the list_files task is specified below in the DAG, as a python callable using list_objects() 
    hook = GoogleCloudStorageHook()
    for storage_object in storage_objects:
        destination_object = storage_object 

        if prefix:
            destination_object = "{}/{}".format(prefix, storage_object) 

        hook.copy(source_bucket, storage_object, destination_bucket, destination_object)  # copies `storage_object` from source_bucket to destination bucket specified 
        hook.delete(source_bucket, storage_object)   # then deletes that same file from the source bucket having copied it 


"""
1. Notes for below task ... 
The destination is format: project_name.dataset_name.table_name  now we already had project name, and had created 
dataset_name = vehicle_analytics. We don't yet have a `table_name`, so we can just choose whatever we would like to call it
as we are creating a history table for this example, we can call it `history` 

2. for biq query connection, we will have already modified the 'google_cloud_default' connection in the Admin page on Airflow
webserver. Will have passed it the JSON security key from our Google Cloud instance IAM.
Thus, use that default connection detail now, inside the task connection id below 
"""

# initialse DAG as a context mgr
with DAG(
    'bigquery_data_load', 
    schedule_interval='@hourly', 
    catchup=False, 
    default_args=default_arguments,
    max_active_runs=1,
) as dag:

    # utilise the bucket object listing function created above 
    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_objects,  # provide the function to the python callable 
        op_kwargs={'bucket': 'dmc-logistics-landing-bucket'}, # specify the bucket name to pass to function
    )

    # create a task using the GoogleCloudStorageToBigQueryOperator
    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data',
        bucket='dmc-logistics-landing-bucket',  # bucket I created on google cloud free tier with 2 CSVs loaded into it 
        source_objects=['*'],  # use the wildcard * to pick up all the objects in the bucket. Otherwise, could specify a pattern to collect
        source_format='CSV',  # file format type
        skip_leading_rows=1,  # skip the top row, as the files contain headers 
        field_delimeter=',',  # specify this field delimeter, in this case comma
        destination_project_dataset_table='dmc-airflow-tutorial-demo.vehicle_analytics.history',  # see notes 1. above 
        create_disposition='CREATE_IF_NEEDED',  # specified if airflow should create the table or not
        write_disposition='WRITE_APPEND',  # should airflow 'append', 'overwrite' or only write to table when table is empty?
        biqquery_conn_id='google_cloud_default',  # see notes 2. above  
        google_cloud_storage_conn_id='google_cloud_default'  # in our case, the same connection details as for Big Query 
    )

    # create SQL query for selecting latest vehicle information only from History data, for analysts to use 
    # use the CSV columns date, hour & minute to create a "time stamp" to rank by for each vehicle ID.
    # select latest rank (so rank=1) only & use bigQuery 'except' keyword to specify we don't want to keep rank column in output
    example_query = """
    SELECT * except (rank)
    FROM ( 
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_id ORDER BY DATETIME(date, Time(hour, minute, 0)) DESC 
            ) as rank 
        FROM `dmc-airflow-tutorial-demo.vehicle_analytics.history`
        ) as latest
    WHERE rank = 1 ;
    """

    # now create task using Big Query operator to run the SQL above & create 'latest' table from 'history' table 
    create_table = BigQueryOperator(
        task_id='create_table',
        sql=example_query,
        destination_dataset_table='dmc-airflow-tutorial-demo.vehicle_analytics.latest',
        write_disposition='WRITE_TRUNCATE',  # will overwrite all data in table each time 
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,  # by default, BigQuery uses legacy SQL. But the SQL we used is standard SQL, so specify False
        location='europe-west2',  # specify the cloud location BigQuery should run the query in. Europe West 2 is London (matching bucket etc) 
        bigQuery_conn_id="google_cloud_default"  # use our Admin default connection for google cloud we set up & configured 
    )


    # new task, using a python operator, to move the files 
    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_objects,
        op_kwargs={
            'source_bucket': 'dmc-logistics-landing-bucket', 
            'destination_bucket': 'dmc-logistics-landing-bucket-backup'
        },
        provide_context=True,    # this allows the python callable in this task, to access the XCom call
    )


# link tasks 
list_files >> load_data >> create_table >> move_files 
