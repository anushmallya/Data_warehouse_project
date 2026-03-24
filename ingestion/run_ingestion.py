import pyodbc as p
import os
# print(p.drivers())

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

def connection (DATABASE): 
    return p.connect(f'DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID=airflow;PWD=airflow;TrustServerCertificate=yes;')

def load_bronze():
    conn = connection('DateWarehouse')
    point = conn.cursor()

    try:
        print("Starting Bronze load...")
        point.execute("EXEC bronze.load_bronze")
        conn.commit()
        print("Bronze load completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error in Bronze load")
        print(e)
        raise

    finally:
        point.close()
        conn.close()


def load_silver():
    conn = connection('DateWarehouse')
    point = conn.cursor()

    try:
        print("Starting Silver load...")
        point.execute("EXEC silver.load_silver")
        conn.commit()
        print("Silver load completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error in Silver load")
        print(e)
        raise

    finally:
        point.close()
        conn.close()


def validate_gold():
    conn = connection('DateWarehouse')
    point = conn.cursor()

    try:
        print("Validating Gold layer...")

        gold_views = [
            "gold.dim_customer",
            "gold.dim_product",
            "gold.fact_sales"
        ]

        for view_name in gold_views:
            point.execute(f"SELECT COUNT(*) FROM {view_name}")
            row_count = point.fetchone()[0]
            print(f"{view_name} row_count = {row_count}")

        print("Gold validation completed successfully.")

    except Exception as e:
        print("Error in Gold validation")
        print(e)
        raise

    finally:
        point.close()
        conn.close()


def run():
    print("Start Pipeline")
    load_bronze()
    load_silver()
    validate_gold()
    print("Pipeline completed successfully.")
if __name__ == "__main__":
    run()