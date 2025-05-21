from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 2
}

# "m h dom mon dow"
# m: phút (0 → phút 0)
# h: giờ (9 → 9h sáng, 11 → 11h trưa)
# dom: ngày trong tháng (* = mọi ngày)
# mon: tháng (* = mọi tháng)
# dow: thứ (* = mọi thứ)

dag = DAG(
    dag_id = 'final_examination',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

crawl_gold = BashOperator(
    task_id = 'crawl_gold',
    bash_command = 'python /var/tmp/app/crawl_data.py',
    dag=dag
)

transform_gold = BashOperator(
    task_id = 'transform_gold',
    bash_command = 'python /var/tmp/app/transform.py',
    dag=dag
)

save_gold = BashOperator(
    task_id='save_gold',
    bash_command = 'python /var/tmp/app/save.py',
    dag=dag
)

crawl_gold >> transform_gold >> save_gold