import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from currency.operators.corrency_operatop import CurrencyOperator
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator


def on_failure_callback(context):
    """ Уведомление об ошибке дага в tg """
    ti = context['ti']
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_id',
        chat_id='-1001745042397',
        text=f"Ошибка в Даге:{ti.dag_id} задачa: {ti.task_id}, Дата запуска: {context['ds']}")
    return send_message.execute(context)

with DAG(dag_id='CURRENSY_PIPELINE',
         start_date=datetime(2021, 12, 1),
         end_date=datetime(2021, 12, 31),
         on_failure_callback=on_failure_callback,
         schedule_interval='@daily',
         tags=['IVAN'],
         default_args={'owner': 'Ivan.L'}) as dag:

    """ Получаем курс валюты по API и сохраняет результат в XCOM"""
    get_rate = CurrencyOperator(task_id='get_rate')
    
    @task
    def sql_query(ds=None):
        """ Получает данные из БД за день выполнения DAG и сохраняет результат в XCOM """

        pg_hook_book = PostgresHook(postgres_conn_id='pipeline_1')
        engine = pg_hook_book.get_sqlalchemy_engine()

        sql = f"""
               SELECT
       	        nt."date"
       	        ,nt.currency
       	        ,nt.value
               FROM public.newtable nt
               WHERE nt."date" = '{ds}'
               """
        df = pd.read_sql(sql=sql, con=engine)
        sql_result = df.to_json()
        return sql_result

    @task()
    def transform(sql_result):
        """ Получет данные из XCOM обогащает данные из базы курсом валют и загружает отчет в csv файл """
        context = get_current_context()
        currency_rate = context['ti'].xcom_pull(task_ids='get_rate')
        df = pd.read_json(sql_result)
        df['rub_rate'] = currency_rate
        df['sum_usd_rub'] = df.apply(lambda row: round(row['value'] * row['rub_rate'], 2), axis=1)
        df.to_csv(f"/home/master/airflow/data/report_{context['ds']}.csv", index=False)

    @task
    def load_to_db():
        """ Загружает отчет в базу """
        context = get_current_context()
        pg_hook = PostgresHook('pipeline_1')
        engine = pg_hook.get_sqlalchemy_engine()
        
        df = pd.read_csv(f"/home/master/airflow/data/report_{context['ds']}.csv")
        df.to_sql(name='report', con=engine, schema='main', if_exists='append', index=False)

    """ Удаляем старые отчеты """
    clear_dir = BashOperator(task_id='clear_dir',
                             bash_command="rm /home/master/airflow/data/report_{{ ds }}.csv")
    
    sql_query = sql_query()
    transform = transform(sql_result=sql_query)
    load_to_db = load_to_db()
    [get_rate, sql_query] >> transform >> load_to_db >> clear_dir
