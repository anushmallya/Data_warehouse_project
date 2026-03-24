import pyodbc as p
# print(pyodbc.drivers())
import os

SERVER = os.getenv("SQL_SERVER", r"DESKTOP-FI1EJ2S\SQLEXPRESS")
DRIVER = os.getenv("SQL_DRIVER")

if not DRIVER:
    available = p.drivers()
    if "ODBC Driver 18 for SQL Server" in available:
        DRIVER = "ODBC Driver 18 for SQL Server"
    elif "ODBC Driver 17 for SQL Server" in available:
        DRIVER = "ODBC Driver 17 for SQL Server"
    else:
        raise Exception("No suitable SQL Server ODBC driver found")

def connection (DATABASE,autocommit=False): 
    return p.connect(f'DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID=airflow;PWD=airflow;TrustServerCertificate=yes;',autocommit=autocommit)

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql(point, sql_query):
    queries = sql_query.split("GO")
    for q in queries:
        q = q.strip()
        if q:
            point.execute(q)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def setup_database():
    conn1 = connection('master',autocommit=True)
    point = conn1.cursor()

    try:
        database_creation = read_sql_file(os.path.join(BASE_DIR,"scripts/datawarehouse_creation.sql"))
        execute_sql(point, database_creation)
        print("Starting Database Setup...")

        conn2 = connection('DateWarehouse')
        point2 = conn2.cursor()

        try:
            # Schema creation
            schema_creation = read_sql_file(os.path.join(BASE_DIR,"scripts/schema_creation.sql"))
            execute_sql(point2, schema_creation)
            print("Schema created")

            # Bronze DDL
            bronze_ddl = read_sql_file(os.path.join(BASE_DIR,"scripts/bronze/ddl_bronze.sql"))
            execute_sql(point2, bronze_ddl)
            print("Bronze tables created!")

            # Bronze stored procedure
            bronze_data_stored_procedure = read_sql_file(os.path.join(BASE_DIR,"scripts/bronze/data_load_bronze.sql"))
            execute_sql(point2, bronze_data_stored_procedure)
            print("Bronze data in stored procedure")

            # Silver DDL
            silver_ddl = read_sql_file(os.path.join(BASE_DIR,"scripts/silver/ddl_silver.sql"))
            execute_sql(point2, silver_ddl)
            print("Silver tables created!")

            # Silver stored procedure
            silver_data_stored_procedure = read_sql_file(os.path.join(BASE_DIR,"scripts/silver/data_load_silver.sql"))
            execute_sql(point2, silver_data_stored_procedure)
            print("Silver data in stored procedure")

            #Gold
            gold_view = read_sql_file(os.path.join(BASE_DIR,"scripts/gold/ddl_gold.sql"))
            execute_sql(point2,gold_view)
            print("Gold view is created")

            conn2.commit()
            
        except Exception as e:
            print("Error occurred")
            print(e)

        finally:
            point2.close()
            conn2.close()
        print("Database Setup Completed!")

    except Exception as e:
        print("Error occurred")
        print(e)

    finally:
        point.close()
        conn1.close() 

if __name__ == "__main__":
    setup_database()