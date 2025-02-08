from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

# pake fungsi

@dag() # didalamnya isi ini sama kaya yang di with statement
# kalo decorator nya ga diisi, dia bakal pake nama function nya
# tapi kalo diisi, misal dag_id = x, walopun nama fungsi nya beda, dia bakal tetap pake nama parameter di dag_id

def test_dag_ke55():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

test_dag_ke55()