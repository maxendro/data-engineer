from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# Название DAG-файла
DAG_NAME = 'final_kuznetsov_dag2'
# Коннект, созданный в web-интерфейсе в Airflow
GP_CONN_ID = 'kuznetsov_conn'

sql_drop_t = f"drop table if exists final_kuznetsov_warm cascade;"
sql_create_t = f"CREATE TABLE final_kuznetsov_warm (user_id  int4,content_id int4,start_s timestamp,stop_s timestamp, channel_id int, region_id int) WITH (	appendonly=true, orientation=column,	compresstype=zstd,	compresslevel=1)DISTRIBUTED RANDOMLY;"
sql_insert_t = f"insert into final_kuznetsov_warm select * from final_kuznetsov_ext e where e.start_s > date_trunc('day'::text, now() - '6 mons'::interval)";
sql_drop_v = f"drop view if exists v_final_kuznetsov_hot;"
sql_create_v = f"create or replace view v_final_kuznetsov_hot as \
 SELECT l.user_id, \
    l.content_id, \
    fkc.content_name,\
    l.start_s,\
    l.stop_s,\
    fkch.channel_name,\
    l.region_id,\
    fkr.region_name,\
    fkr.code\
   FROM final_kuznetsov_warm l\
     JOIN final_kuznetsov_content fkc ON l.content_id = fkc.content_id\
     JOIN final_kuznetsov_regions fkr ON l.region_id = fkr.region_id\
     JOIN final_kuznetsov_channels fkch ON l.channel_id = fkch.channel_id\
where\
	l.start_s::date between date_trunc('week',\
	current_date)::date - 7 \
          and date_trunc('week',\
	current_date)::date;"


args = {'owner': 'kuznetsov',
        'start_date': datetime(2023, 11, 10), # дата запуска
        'retries': 3,    # количество попыток в случае аварии
        'retry_delay': timedelta(seconds=300)}  # время между попытками

# конструктор DAG
with DAG(DAG_NAME, description='kuznetsov_DAG2',
         schedule_interval='0 * * * *', #расписание, формат CRON
         catchup=False, # если упал DAG, догонять очередь не надо
         max_active_runs=1, # не запускать пока висит не завершенный
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    #PythonOperator (выполнение функции Python)  - отправляем сообщение в Telegramm

    # PostgresOperator (выполнени DML в PG)  - вставляем запись в таблицу
    drop_t = PostgresOperator(task_id="drop_t",
                                sql=sql_drop_t,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)
    create_t = PostgresOperator(task_id="create_t",
                                    sql=sql_create_t,
                                    postgres_conn_id=GP_CONN_ID,
                                    autocommit=True)
    insert_t  = PostgresOperator(task_id="insert_t",
                                sql=sql_insert_t,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)
    drop_v  = PostgresOperator(task_id="drop_v",
                                sql=sql_drop_v  ,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)
    create_v  = PostgresOperator(task_id="create_v",
                                sql=sql_create_v,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)

    # Граф выполнения (выстраивание процесса последовательности)
    drop_v >> drop_t >> create_t >> insert_t >> create_v
