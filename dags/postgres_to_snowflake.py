from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'sync_postgres_to_snowflake_data',
    default_args=default_args,
    description='Sync data from PostgreSQL to Snowflake using alternative approach',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['postgres', 'snowflake'],
)

# Define your tables mapping from PostgreSQL to Snowflake
table_mapping = {
    'categories': 'CATEGORIES',
    'customers': 'CUSTOMERS',
    'employee_territories': 'EMPLOYEE_TERRITORIES',
    'employees': 'EMPLOYEES',
    'order_details': 'ORDER_DETAILS',
    'orders': 'ORDERS',
    'products': 'PRODUCTS',
    'regions': 'REGIONS',
    'shippers': 'SHIPPERS',
    'suppliers': 'SUPPLIERS',
    'territories': 'TERRITORIES'
}

# Define PostgreSQL connection ID
postgres_conn_id = 'postgres-new'

# Define Snowflake connection ID
snowflake_conn_id = 'snowflake-new'

# Function to execute SQL query and insert data into Snowflake table
def load_data_to_snowflake(pg_table, sf_table, **kwargs):
    # Execute SQL query in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f'SELECT * FROM public.{pg_table};')
    pg_data = pg_cursor.fetchall()

    # Generate placeholders dynamically based on the number of columns in the Snowflake table
    num_columns = len(pg_data[0])
    placeholders = ','.join(['%s'] * num_columns)

    # Construct the SQL query
    query = f'INSERT INTO {sf_table} VALUES ({placeholders})'

    # Insert data into Snowflake table
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_conn = snowflake_hook.get_conn()
    snowflake_cursor = snowflake_conn.cursor()
    snowflake_cursor.execute(f'TRUNCATE TABLE {sf_table};')
    snowflake_cursor.executemany(query, pg_data)
    snowflake_conn.commit()

def create_snowflake_tables():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake-new')
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.CATEGORIES (
            CATEGORYID NUMBER(38,0) NOT NULL,
            CATEGORYNAME VARCHAR(256),
            DESCRIPTION VARCHAR(256),
            PICTURE VARCHAR(256),
            primary key (CATEGORYID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.CUSTOMERS (
            CUSTOMERID VARCHAR(5) NOT NULL,
            COMPANYNAME VARCHAR(256),
            CONTACTNAME VARCHAR(256),
            CONTACTTITLE VARCHAR(256),
            ADDRESS VARCHAR(256),
            CITY VARCHAR(256),
            REGION VARCHAR(256),
            POSTALCODE VARCHAR(256),
            COUNTRY VARCHAR(256),
            PHONE VARCHAR(256),
            FAX VARCHAR(256),
            primary key (CUSTOMERID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.EMPLOYEES (
            EMPLOYEEID NUMBER(38,0) NOT NULL,
            LASTNAME VARCHAR(256),
            FIRSTNAME VARCHAR(256),
            TITLE VARCHAR(256),
            TITLEOFCOURTESY VARCHAR(256),
            BIRTHDATE TIMESTAMP_NTZ(9),
            HIREDATE TIMESTAMP_NTZ(9),
            ADDRESS VARCHAR(256),
            CITY VARCHAR(256),
            REGION VARCHAR(256),
            POSTALCODE VARCHAR(256),
            COUNTRY VARCHAR(256),
            HOMEPHONE VARCHAR(256),
            EXTENSION NUMBER(38,0),
            PHOTO VARCHAR(256),
            NOTES VARCHAR(256),
            REPORTSTO NUMBER(38,0),
            PHOTOPATH VARCHAR(256),
            primary key (EMPLOYEEID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.EMPLOYEE_TERRITORIES (
            EMPLOYEEID NUMBER(38,0),
            TERRITORYID NUMBER(38,0)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.ORDERS (
            ORDERID NUMBER(38,0) NOT NULL,
            CUSTOMERID VARCHAR(256),
            EMPLOYEEID NUMBER(38,0),
            ORDERDATE TIMESTAMP_NTZ(9),
            REQUIREDDATE TIMESTAMP_NTZ(9),
            SHIPPEDDATE TIMESTAMP_NTZ(9),
            SHIPVIA NUMBER(38,0),
            FREIGHT NUMBER(10,3),
            SHIPNAME VARCHAR(256),
            SHIPADDRESS VARCHAR(256),
            SHIPCITY VARCHAR(256),
            SHIPREGION VARCHAR(256),
            SHIPPOSTALCODE VARCHAR(256),
            SHIPCOUNTRY VARCHAR(256),
            primary key (ORDERID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.ORDER_DETAILS (
            ORDERID NUMBER(10,3),
            PRODUCTID NUMBER(10,3),
            UNITPRICE NUMBER(10,3),
            QUANTITY NUMBER(10,3),
            DISCOUNT NUMBER(10,3)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.PRODUCTS (
            PRODUCTID NUMBER(38,0) NOT NULL,
            PRODUCTNAME VARCHAR(256),
            SUPPLIERID NUMBER(38,0),
            CATEGORYID NUMBER(38,0),
            QUANTITYPERUNIT VARCHAR(256),
            UNITPRICE NUMBER(10,3),
            UNITSINSTOCK NUMBER(38,0),
            UNITSONORDER NUMBER(38,0),
            REORDERLEVEL NUMBER(38,0),
            DISCONTINUED NUMBER(38,0),
            primary key (PRODUCTID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.REGIONS (
            REGIONID NUMBER(38,0) NOT NULL,
            REGIONDESCRIPTION VARCHAR(256),
            primary key (REGIONID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.SHIPPERS (
            SHIPPERID NUMBER(38,0) NOT NULL,
            COMPANYNAME VARCHAR(256),
            PHONE VARCHAR(256),
            primary key (SHIPPERID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.SUPPLIERS (
            SUPPLIERID NUMBER(38,0) NOT NULL,
            COMPANYNAME VARCHAR(256),
            CONTACTNAME VARCHAR(256),
            CONTACTTITLE VARCHAR(256),
            ADDRESS VARCHAR(256),
            CITY VARCHAR(256),
            REGION VARCHAR(256),
            POSTALCODE VARCHAR(256),
            COUNTRY VARCHAR(256),
            PHONE VARCHAR(256),
            FAX VARCHAR(256),
            HOMEPAGE VARCHAR(256),
            primary key (SUPPLIERID)
        );

        CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC2.TERRITORIES (
            TERRITORYID NUMBER(38,0) NOT NULL,
            TERRITORYDESCRIPTION VARCHAR(256),
            REGIONID NUMBER(38,0),
            primary key (TERRITORYID)
         );

        """,
        # Tambahkan query CREATE TABLE IF NOT EXISTS untuk tabel-tabel lainnya di sini
    ]

    for query in create_table_queries:
        snowflake_hook.run(query)

start_task = PythonOperator(
    task_id='create_snowflake_tables',
    python_callable=create_snowflake_tables,
    dag=dag
)

# Iterate over the table mapping and create tasks to transfer data
for pg_table, sf_table in table_mapping.items():
    pg_task = PostgresOperator(
        task_id=f'extract_{pg_table}_from_postgres',
        postgres_conn_id=postgres_conn_id,
        sql=f'SELECT * FROM public.{pg_table};',
        dag=dag
    )

    sf_task = PythonOperator(
        task_id=f'load_{pg_table}_into_snowflake',
        python_callable=load_data_to_snowflake,
        op_kwargs={'pg_table': pg_table, 'sf_table': sf_table},
        dag=dag
    )

    start_task >> pg_task >> sf_task
