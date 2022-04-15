import unittest 
from airflow.models import DagBag 


class TestCoreConceptsDAG(unittest.TestCase):

    def setup(self):
        self.dagbag = DagBag() 
        self.dag = self.dagbag.get_dag(dag_id='core_concepts') 

    
    def test_dag_loaded(self):
        self.assertDictEqual(self.dagbag.import_errors, {})  # import_errors is an attribute of the `dagbag`. We are checking if it is an empty dictionary 
        self.assertIdNotNone(self.dag) # check that the DAG itself is not None


    def test_contain_tasks(self):
        self.assertListEqual(self.dag.task_ids, ['bash_command', 'python_function']) # check is the DAG task id's match a list you pass as an argument 

    
    def test_dependecies_of_bash_command(self):
        bash_task = self.dag.get_task('bash_command') 

        self.assertEqual(bash_task.upstream_task_ids, set())   # the upstream_task_ids returns a `set` of any upstream tasks. Hence, you can test for correct upstream tasks,
                                                               # or if any exists etc. 
        self.assertEqual(bash_task.downstream_task_ids, set(["python_function"])) 


