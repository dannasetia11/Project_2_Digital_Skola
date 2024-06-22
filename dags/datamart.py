from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Definisikan default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Definisikan query untuk membuat tabel data mart
create_datamart_best_employee_query = """
CREATE OR REPLACE TABLE PROJECT3.PUBLIC.DATAMART_MONTHLY_BEST_EMPLOYEE AS
SELECT
    e.FIRSTNAME || ' ' || e.LASTNAME AS EMPLOYEE_NAME,
    EXTRACT(MONTH FROM o.ORDERDATE) AS MONTH,
    SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS TOTAL_GROSS_REVENUE
FROM
    PROJECT3.PUBLIC.ORDERS o
JOIN PROJECT3.PUBLIC.ORDER_DETAILS od ON o.ORDERID = od.ORDERID
JOIN PROJECT3.PUBLIC.EMPLOYEES e ON o.EMPLOYEEID = e.EMPLOYEEID
GROUP BY
    e.FIRSTNAME,
    e.LASTNAME,
    EXTRACT(MONTH FROM o.ORDERDATE)
ORDER BY
    EXTRACT(MONTH FROM o.ORDERDATE),
    TOTAL_GROSS_REVENUE DESC;
"""

create_datamart_category_sold_query = """
CREATE OR REPLACE TABLE PROJECT3.PUBLIC.DATAMART_MONTHLY_CATEGORY_SOLD AS
SELECT
    c.CATEGORYNAME AS CATEGORY_NAME,
    EXTRACT(MONTH FROM o.ORDERDATE) AS MONTH,
    SUM(od.QUANTITY) AS TOTAL_SOLD
FROM
    PROJECT3.PUBLIC.ORDERS o
JOIN PROJECT3.PUBLIC.ORDER_DETAILS od ON o.ORDERID = od.ORDERID
JOIN PROJECT3.PUBLIC.PRODUCTS p ON od.PRODUCTID = p.PRODUCTID
JOIN PROJECT3.PUBLIC.CATEGORIES c ON p.CATEGORYID = c.CATEGORYID
GROUP BY
    c.CATEGORYNAME,
    EXTRACT(MONTH FROM o.ORDERDATE)
ORDER BY
    EXTRACT(MONTH FROM o.ORDERDATE),
    TOTAL_SOLD DESC;
"""

create_datamart_supplier_revenue_query = """
CREATE OR REPLACE TABLE PROJECT3.PUBLIC.DATAMART_MONTHLY_SUPPLIER_GROSS_REVENUE AS
SELECT
    s.COMPANYNAME AS SUPPLIER_NAME,
    EXTRACT(MONTH FROM o.ORDERDATE) AS MONTH,
    SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS GROSS_REVENUE
FROM
    PROJECT3.PUBLIC.ORDERS o
JOIN PROJECT3.PUBLIC.ORDER_DETAILS od ON o.ORDERID = od.ORDERID
JOIN PROJECT3.PUBLIC.PRODUCTS p ON od.PRODUCTID = p.PRODUCTID
JOIN PROJECT3.PUBLIC.SUPPLIERS s ON p.SUPPLIERID = s.SUPPLIERID
GROUP BY
    s.COMPANYNAME,
    EXTRACT(MONTH FROM o.ORDERDATE)
ORDER BY
    EXTRACT(MONTH FROM o.ORDERDATE),
    GROSS_REVENUE DESC;
"""

# Definisikan DAG
with DAG('create_datamart_tables',
         default_args=default_args,
         schedule_interval=None,  # Atur schedule sesuai kebutuhan
         catchup=False) as dag:

    # Task untuk membuat tabel best employee
    create_best_employee_table = SnowflakeOperator(
        task_id='create_best_employee_table',
        sql=create_datamart_best_employee_query,
        snowflake_conn_id='snowflake',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel category sold
    create_category_sold_table = SnowflakeOperator(
        task_id='create_category_sold_table',
        sql=create_datamart_category_sold_query,
        snowflake_conn_id='snowflake',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel supplier revenue
    create_supplier_revenue_table = SnowflakeOperator(
        task_id='create_supplier_revenue_table',
        sql=create_datamart_supplier_revenue_query,
        snowflake_conn_id='snowflake',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    create_best_employee_table >> create_category_sold_table >> create_supplier_revenue_table
