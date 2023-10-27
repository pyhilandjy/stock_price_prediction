from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
import Crawl
#kst=Timezone('Asia/Seoul')

with DAG(
    dag_id='Crawl_Schedule',
    description='DAG purpose on Scheduling',
    schedule_interval='@once',
    start_date=datetime(2022, 10, 12),
    tags=['test', 'call_trigger_sample']
) as dag:
    def crawling() -> None:
        Crawl.dailyCrawl()
    def learning() -> None:
        print("Learning...")
        sleep(3)
    def nothing() -> None:
        print("ending...")
        sleep(3)

    crawl = PythonOperator(
        task_id='crawling',
        python_callable=crawling
    )

     # call_trigger = TriggerDagRunOperator(
     #   task_id='learning',
     #   trigger_dag_id='learning',
     #   trigger_run_id=None,
     #   execution_date=None,
     #   reset_dag_run=False,
     #   wait_for_completion=False,
     #   poke_interval=60,
     #   allowed_states=["success"],
     #   failed_states=None,
     #)

     # trigger rule을 사용하지 않으면 바로 end task가 실행된다
     # 바로 실행 되지 않고 trigger를 기다리고 싶다면 trigger rule을 사용
     # email보내는 것과 같이 그냥 보내고 다른 task를 진행하고 싶다면 trigger rule을 사용하지 않고 진행
    learn = PythonOperator(
        task_id='learning',
        python_callable=nothing
    )

    crawl >> learn
