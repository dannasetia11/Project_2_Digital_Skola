from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.postgres_to_snowflake import PostgresToSnowflakeOperator
from airflow.utils.dates import days_ago

# Definisikan default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Definisikan query untuk membuat tabel di Snowflake
create_categories_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.categories (
    categoryID INT PRIMARY KEY,
    categoryName VARCHAR(256),
    description VARCHAR(256),
    picture VARCHAR(256)
);
"""

create_customers_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.customers (
    customerID VARCHAR(5) PRIMARY KEY,
    companyName VARCHAR(256),
    contactName VARCHAR(256),
    contactTitle VARCHAR(256),
    address VARCHAR(256),
    city VARCHAR(256),
    region VARCHAR(256),
    postalCode VARCHAR(256),
    country VARCHAR(256),
    phone VARCHAR(256),
    fax VARCHAR(256)
);
"""

create_employee_territories_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.employee_territories (
    employeeID INT,
    territoryID INT
);
"""

create_employees_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.employees (
    employeeID INT PRIMARY KEY,
    lastName VARCHAR(256),
    firstName VARCHAR(256),
    title VARCHAR(256),
    titleOfCourtesy VARCHAR(256),
    birthDate TIMESTAMP,
    hireDate TIMESTAMP,
    address VARCHAR(256),
    city VARCHAR(256),
    region VARCHAR(256),
    postalCode VARCHAR(256),
    country VARCHAR(256),
    homePhone VARCHAR(256),
    extension INT,
    photo VARCHAR(256),
    notes VARCHAR(256),
    reportsTo INT,
    photoPath VARCHAR(256)
);
"""

create_order_details_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.order_details (
    orderID DECIMAL(10, 3),
    productID DECIMAL(10, 3),
    unitPrice DECIMAL(10, 3),
    quantity DECIMAL(10, 3),
    discount DECIMAL(10, 3)
);
"""

create_orders_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.orders (
    orderID INT PRIMARY KEY,
    customerID VARCHAR(256),
    employeeID INT,
    orderDate TIMESTAMP,
    requiredDate TIMESTAMP,
    shippedDate TIMESTAMP,
    shipVia INT,
    freight DECIMAL(10, 3),
    shipName VARCHAR(256),
    shipAddress VARCHAR(256),
    shipCity VARCHAR(256),
    shipRegion VARCHAR(256),
    shipPostalCode VARCHAR(256),
    shipCountry VARCHAR(256)
);
"""

create_products_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.products (
    productID INT PRIMARY KEY,
    productName VARCHAR(256),
    supplierID INT,
    categoryID INT,
    quantityPerUnit VARCHAR(256),
    unitPrice DECIMAL(10, 3),
    unitsInStock INT,
    unitsOnOrder INT,
    reorderLevel INT,
    discontinued INT
);
"""

create_regions_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.regions (
    regionID INT PRIMARY KEY,
    regionDescription VARCHAR(256)
);
"""

create_shippers_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.shippers (
    shipperID INT PRIMARY KEY,
    companyName VARCHAR(256),
    phone VARCHAR(256)
);
"""

create_suppliers_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.suppliers (
    supplierID INT PRIMARY KEY,
    companyName VARCHAR(256),
    contactName VARCHAR(256),
    contactTitle VARCHAR(256),
    address VARCHAR(256),
    city VARCHAR(256),
    region VARCHAR(256),
    postalCode VARCHAR(256),
    country VARCHAR(256),
    phone VARCHAR(256),
    fax VARCHAR(256),
    homePage VARCHAR(256)
);
"""

create_territories_table_query = """
CREATE TABLE IF NOT EXISTS PROJECT3.PUBLIC.territories (
    territoryID INT PRIMARY KEY,
    territoryDescription VARCHAR(256),
    regionID INT
);
"""

# Definisikan DAG
with DAG('create_snowflake_tables',
         default_args=default_args,
         schedule_interval=None,  # Atur schedule sesuai kebutuhan
         catchup=False) as dag:

    # Task untuk membuat tabel categories
    create_categories_table = SnowflakeOperator(
        task_id='create_categories_table',
        sql=create_categories_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel customers
    create_customers_table = SnowflakeOperator(
        task_id='create_customers_table',
        sql=create_customers_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel employee territories
    create_employee_territories_table = SnowflakeOperator(
        task_id='create_employee_territories_table',
        sql=create_employee_territories_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel employees
    create_employees_table = SnowflakeOperator(
        task_id='create_employees_table',
        sql=create_employees_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel order details
    create_order_details_table = SnowflakeOperator(
        task_id='create_order_details_table',
        sql=create_order_details_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel orders
    create_orders_table = SnowflakeOperator(
        task_id='create_orders_table',
        sql=create_orders_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel products
    create_products_table = SnowflakeOperator(
        task_id='create_products_table',
        sql=create_products_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel regions
    create_regions_table = SnowflakeOperator(
        task_id='create_regions_table',
        sql=create_regions_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel shippers
    create_shippers_table = SnowflakeOperator(
        task_id='create_shippers_table',
        sql=create_shippers_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel suppliers
    create_suppliers_table = SnowflakeOperator(
        task_id='create_suppliers_table',
        sql=create_suppliers_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Task untuk membuat tabel territories
    create_territories_table = SnowflakeOperator(
        task_id='create_territories_table',
        sql=create_territories_table_query,
        snowflake_conn_id='snowflake_default',
        warehouse='COMPUTE_WH',
        database='PROJECT3',
        schema='PUBLIC'
    )

    # Set task dependencies
    create_categories_table >> create_customers_table >> create_employee_territories_table >> create_employees_table >> create_order_details_table >> create_orders_table >> create_products_table >> create_regions_table >> create_shippers_table >> create_suppliers_table >> create_territories_table
