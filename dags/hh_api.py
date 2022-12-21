import datetime
import requests
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

args = {
    'owner': 'my_team',
    'start_date':datetime.datetime(2018, 11, 1),
    'provide_context':True
}

def extract_data(**kwargs):
    only_w_salary = True
    ti = kwargs['ti']
    page = kwargs['page']
    title = kwargs['title']

    url = 'https://api.hh.ru/vacancies'
    par = {'text': title, 'area': '113', 'per_page': '10', 'page': page, 'only_with_salary': only_w_salary}  # 113 – Russia
    response = requests.get(url, params=par)

    if response.status_code==200:
        json_data = response.json()
        json_data['key_skill'] = title
        Variable.set(f'vacancies_json_{page}_{title.replace(" ", "")}', json_data, serialize_json=True)
        ti.xcom_push(key=f'vacancies_json_{page}_{title.replace(" ", "")}', value=json_data)

def transform_data(**kwargs):
    ti = kwargs['ti']
    page = kwargs['page']
    title = kwargs['title']
    json_data = Variable.get(f'vacancies_json_{page}_{title.replace(" ", "")}',  deserialize_json=True)

    df_vacancies = pd.DataFrame(columns=['remote_id', 'key_skill', 'alternate_url', 'name', 'city', 'employer',
                                         'vacancy_compensation_low', 'vacancy_compensation_high', 'created_at'])
    if json_data != None: 
        for ind in range(len(json_data['items'])):
            item = json_data['items'][ind]
            df_vacancies.loc[ind] = [
                item['id'], json_data['key_skill'], item['alternate_url'], item['name'], item['area']['name'],
                item['employer']['name'], item['salary']['from'], item['salary']['to'], item['created_at']]

    sql = "INSERT INTO vacantions(remote_id, key_skill, alternate_url, name, city, employer, vacancy_compensation_low, vacancy_compensation_high, created_at) VALUES " + ', '.join([f"""(
     {vacancy['remote_id']},
    '{vacancy['key_skill']}',
    '{vacancy['alternate_url']}',
    '{vacancy['name']}',
    '{vacancy['city']}',
    '{vacancy['employer']}',
     {vacancy['vacancy_compensation_low'] if vacancy['vacancy_compensation_low'] != None else 'NULL'},
     {vacancy['vacancy_compensation_high'] if vacancy['vacancy_compensation_high'] != None else 'NULL'},
    '{vacancy['created_at']}')""" for _, vacancy in df_vacancies.iterrows()]) + ";"
    if len(json_data['items']) == 0:
        sql = "SELECT 1;"
    ti.xcom_push(key=f'sql_query_{page}_{title.replace(" ", "")}', value=sql)

with DAG('load_hh_data', description='load_hh_data', schedule_interval='0 0 * * *',  catchup=False,default_args=args) as dag:
    create_cloud_table = PostgresOperator(
                        task_id=f"create_table",
                        postgres_conn_id="database_PG",
                        sql="""
                            CREATE TABLE IF NOT EXISTS vacantions (
                            id SERIAL PRIMARY KEY,
                            remote_id INTEGER,
                            key_skill VARCHAR(50),
                            alternate_url VARCHAR(100),
                            name VARCHAR(100),
                            city VARCHAR(100),
                            employer VARCHAR(100),
                            vacancy_compensation_low FLOAT,
                            vacancy_compensation_high FLOAT,
                            created_at TIMESTAMP);
                        """,
                        )
    # drop_table = PostgresOperator(
    #                     task_id=f"drop_table",
    #                     postgres_conn_id="database_PG",
    #                     sql="""
    #                         DROP TABLE vacantions; 
    #                     """,
    #                     )

    titles = ['Kafka', 'Airflow', 'Apache Spark', 'Apach Beam', 'MLflow', 'Kuberflow', 'Hadoop', 'DVC', 'Feast']
    extract_data_operators = []
    for title in titles:
        for page in range(20):
            instance = f'{page}_{title.replace(" ", "")}'
            extract_data_operator = (PythonOperator(
                task_id=f'extract_data_{instance}',
                python_callable=extract_data,
                op_kwargs={'title': title, 'page': page}))
            
            transform_data_operator = PythonOperator(
                task_id=f'transform_data_{instance}',
                python_callable=transform_data,
                trigger_rule='one_success',
                op_kwargs={'title': title, 'page': page})

            insert_in_table = PostgresOperator(
                        task_id=f"insert_clouds_table_{instance}",
                        postgres_conn_id="database_PG",
                        sql=f"""{{{{ti.xcom_pull(key='sql_query_{instance}', task_ids=['transform_data_{instance}'])[0]}}}}"""
                        )

            create_cloud_table >> extract_data_operator >> transform_data_operator >> insert_in_table
