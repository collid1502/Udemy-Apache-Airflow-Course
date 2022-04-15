"""
First airflow python script 
"""

from airflow import DAG 
from airflow.utils.dates import days_ago 

# the bash operator is one of the most useful airflow operators, it can run bash code and set environment variables 
from airflow.operators.bash_operator import BashOperator

# the python operator is also useful, allowing the running of python code 
from airflow.operators.python_operator import PythonOperator 

# import helpers like 'chain' and 'cross_downstream' 
from airflow.utils.helpers import chain, cross_downstream 

# these two default arguments are a bare min
# each task will have these passed to it 
default_arguments = {
    'owner': 'Dan Collins',
    'start_date': days_ago(1)
}

# create a dag, give it a dag_id called 'core_concepts' and give it a schedule of daily at midnight through a cron pre-set
# also, change the `catchup` from default True to False
# dag = DAG('core_concepts', schedule_interval='@daily', catchup=False)   


# now, define the DAG as a context manager instead 
with DAG('core_concepts', schedule_interval='@daily', catchup=False, default_args=default_arguments) as dag:

    # now, let's specify some operators 
    """
    Types of operators include:
        Action Operators - They perform actions or tell a system to perform an action 
        Transfer Operators - They move data 
        Sensors - They keep running until a criteria is met 
    """
    # create a simple bash task 
    bash_task = BashOperator(
        task_id='bash_command',                          # specify the task id {name} 
        bash_command='echo "Todays date is $TODAY"',     # specify the bash code to run. We will print a string with env var $TODAY to terminal 
        env={"TODAY": "2022-03-05"}                      # set var $TODAY with a value of 2022-03-05 
        )


    # create a simple python function, that our python task can then call 
    def print_name(first_name=None, last_name=None):
        print(f"This person's name is {first_name} {last_name}") 

    # now create a simple python task to use that function 
    python_task = PythonOperator(
        task_id='python_print_function',    # specify the task name
        python_callable=print_name,         # choose the python callable function, in our case, the `print_name`
        op_args=['Ron', 'Swanson']          # provide the positiona; arguments for the python function called above 
    )


# now set the relationships between tasks 
bash_task >> python_task 


"""
Note, say we wanted to do something like:

    task1 >> task2 >> task3 >> task4 

we could also use the airfow helpers 'chain' method like so:

    chain(task1, task2, task3, task4) 

cross downstream can be used to set tasks between lists, like so:

    cross_downstream([op1, op2] , [op3, op4]) 

is the same as:

    [op1, op2] >> op3
    [op1, op2] >> op4 
"""
#chain(bash_task, python_task) 

