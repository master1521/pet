import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator

@dag(
    dag_id='EMAIL_8',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 5),
    schedule_interval='00 10 * * *',
    tags=['IVAN'],
    max_active_runs=1,  # из-за удаления файлов, можно подумать про нейминг файлов с датой и убрать ограничение
    default_args={'owner': 'Ivan.L'})
def report_to_email():

    @task(retries=5)
    def extract_currency(ds=None) -> pd.DataFrame:
        """ Выгружает курс валюты EUR к USD за день
        :param ds: Дата от запуска дага date_start
        """
        import requests

        params = {
            'start_date': ds,
            'end_date': ds,
            'base': 'EUR',
            'symbols': 'USD',
            'format': 'csv'
        }
        url = 'https://api.exchangerate.host/timeseries'
        response = requests.get(url, params=params)
        currency = pd.read_csv(response.url)
        print(type(currency))
        print(currency.head())
        currency.to_csv('/home/master/airflow/data/currency.csv', index=False)

    @task(retries=5)
    def extract_data(ds=None) -> pd.DataFrame:
        """
        Выгружает данные из гита и возвращает pandas.DataFrame
        :param ds: Дата от запуска дага date_start
        """
        url = f"https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{ds}.csv"
        data = pd.read_csv(url)
        data.to_csv('/home/master/airflow/data/data.csv', index=False)

    waiting_file_currency = FileSensor(
                task_id='waiting_for_file_1',
                filepath='/home/master/airflow/data/currency.csv',
                poke_interval=30,)
                # mode='reschedule')

    waiting_file_data = FileSensor(
        task_id='waiting_for_file_2',
        filepath='/home/master/airflow/data/data.csv',
        poke_interval=5,)
        # mode='reschedule')

    @task
    def insert_to_db() -> None:
        """"
        Загружает данные по валюте и из гита в базу
        """
        pg_hook_book = PostgresHook(postgres_conn_id='PG_book')
        engine = pg_hook_book.get_sqlalchemy_engine()
        
        currency = pd.read_csv('/home/master/airflow/data/currency.csv')
        data = pd.read_csv('/home/master/airflow/data/data.csv')

        currency.to_sql(name='data_currency', con=engine, schema='main', if_exists='append', index=False)
        data.to_sql(name='data_git', con=engine, schema='main', if_exists='append', index=False)

    delet = BashOperator(task_id='delet_data', bash_command="rm /home/master/airflow/data/currency.csv /home/master/airflow/data/data.csv")


    @task
    def sql_query_for_report(ds=None):
        pg_hook_book = PostgresHook(postgres_conn_id='PG_book')
        engine = pg_hook_book.get_sqlalchemy_engine()

        sql = f"""
        SELECT
        	dg."date",
        	dg.currency,
        	dg.value,
        	dc.code,
        	dc.rate,
        	dc.base,
        	dc.start_date,
        	dc.end_date
        FROM main.data_git dg
        INNER JOIN main.data_currency dc ON dc."date" = dg."date"
        AND dc.base = dg.currency
        WHERE dg."date" = '{ds}'
        """
        df = pd.read_sql(sql=sql, con=engine)
        df.to_csv('/home/master/airflow/data/report.csv', index=False)
        df.to_html('/home/master/airflow/data/report.html', index=False)
        print(df.head(15))


    send_email = EmailOperator(task_id='send_email',
                          to=['stepikairflowcourse@yandex.ru'],
                          subject='Report {{ ds }}',
                          files=['data/report.csv'],
                          html_content="""
                          <h1>Отчет от {{ ds }} </h1>
                          """)

    @task
    def load_report_to_db():
        pg_hook_book = PostgresHook(postgres_conn_id='PG_book')
        engine = pg_hook_book.get_sqlalchemy_engine()

        df = pd.read_csv('/home/master/airflow/data/report.csv')
        df.to_sql(name='data_report', con=engine, schema='main', if_exists='append', index=False)


    extract = extract_currency()
    data = extract_data()
    insert = insert_to_db()
    sql_for_report = sql_query_for_report()
    report = load_report_to_db()

    # extract >> waiting_file_currency
    # data >> waiting_file_data
    insert << [waiting_file_data, waiting_file_currency]
    insert >> delet >> sql_for_report
    sql_for_report >> send_email
    sql_for_report >> report

report = report_to_email()



