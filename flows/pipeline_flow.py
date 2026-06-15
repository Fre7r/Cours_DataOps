import os
os.environ["PREFECT_API_URL"] = ""

import subprocess
from pathlib import Path
from io import BytesIO

from prefect import flow, task
import pandas as pd
import duckdb
from sqlalchemy import create_engine
from minio import Minio


# -------------------------
# CONFIG
# -------------------------

# Chemins absolus calculés depuis l'emplacement de ce fichier.
# Le script est dans DataOps/flows/, la racine projet est donc le parent.
BASE_DIR = Path(__file__).resolve().parent.parent          # .../DataOps
DUCKDB_PATH = str(BASE_DIR / "warehouse.duckdb")           # doit matcher profiles.yml
DBT_PROJECT_DIR = BASE_DIR / "dbt_project"                 # dossier du projet dbt
SODA_DIR = BASE_DIR / "soda"                               # dossier des checks Soda

POSTGRES_CONN = "postgresql://dataops:dataops@127.0.0.1:5433/oltp"

MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

BUCKET = "ecommerce-raw"


# -------------------------
# MINIO EXTRACT
# -------------------------

@task
def extract_orders_from_minio():
    obj = MINIO_CLIENT.get_object(BUCKET, "orders.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df


@task
def extract_order_items_from_minio():
    obj = MINIO_CLIENT.get_object(BUCKET, "order_items.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df


# -------------------------
# POSTGRES EXTRACT
# -------------------------

@task
def extract_customers():
    engine = create_engine(POSTGRES_CONN)
    df = pd.read_sql("SELECT * FROM customers", engine)
    return df


@task
def extract_products():
    engine = create_engine(POSTGRES_CONN)
    df = pd.read_sql("SELECT * FROM products", engine)
    return df


@task
def clean_dataframe(df):
    df = df.copy()
    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    # basic cleanup
    df = df.drop_duplicates()
    return df


# -------------------------
# DUCKDB LOAD
# -------------------------

@task
def load_to_duckdb(df, table_name):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")

        df = df.copy()
        df = df.astype("string")  # conversion sûre pour DuckDB

        con.register("tmp", df)
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.{table_name} AS
            SELECT * FROM tmp
        """)

        result = con.execute(
            f"SELECT COUNT(*) FROM staging.{table_name}"
        ).fetchone()
        count = result[0] if result else 0
        print(f"Loaded {table_name}: {count} rows")
    finally:
        con.close()  # on libère le verrou DuckDB quoi qu'il arrive


@task
def validate_load(table_name):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM staging.{table_name}"
        ).fetchone()
        count = result[0] if result else 0
        sample = con.execute(
            f"SELECT * FROM staging.{table_name} LIMIT 5"
        ).df()
    finally:
        con.close()

    print(f"\n[OK] {table_name}: {count} rows")
    print(sample)
    return count


# -------------------------
# HELPER: exécuter une commande externe (dbt, soda)
# -------------------------

def _run_command(cmd, cwd):
    """Lance une commande shell, log la sortie, lève une erreur si échec."""
    print(f"\n$ {' '.join(cmd)}  (cwd={cwd})")
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"La commande a échoué (code {result.returncode}) : {' '.join(cmd)}"
        )
    return result.stdout


# -------------------------
# DBT TASKS
# -------------------------

@task
def dbt_run():
    return _run_command(["dbt", "run"], cwd=DBT_PROJECT_DIR)


@task
def dbt_test():
    return _run_command(["dbt", "test"], cwd=DBT_PROJECT_DIR)


# -------------------------
# SODA TASK
# -------------------------

@task
def soda_scan():
    return _run_command(
        [
            "soda", "scan",
            "-d", "ecommerce",
            "-c", "configuration.yml",
            "checks.yml",
        ],
        cwd=SODA_DIR,
    )


# -------------------------
# FLOW
# -------------------------

@flow(log_prints=True)
def pipeline_flow():
    # ----- 1. EXTRACT -----
    orders = extract_orders_from_minio()
    order_items = extract_order_items_from_minio()
    customers = extract_customers()
    products = extract_products()

    # ----- 2. CLEAN -----
    orders = clean_dataframe(orders)
    order_items = clean_dataframe(order_items)
    customers = clean_dataframe(customers)
    products = clean_dataframe(products)

    # ----- 3. LOAD (staging dans DuckDB) -----
    load_to_duckdb(orders, "orders")
    load_to_duckdb(order_items, "order_items")
    load_to_duckdb(customers, "customers")
    load_to_duckdb(products, "products")

    validate_load("orders")
    validate_load("order_items")
    validate_load("customers")
    validate_load("products")
    print("All sources loaded into DuckDB staging")

    # ----- 4. TRANSFORM (dbt) -----
    # Les connexions DuckDB ci-dessus sont fermées : dbt peut prendre le verrou.
    dbt_run()

    # ----- 5. TESTS dbt -----
    dbt_test()

    # ----- 6. DATA QUALITY (Soda) -----
    soda_scan()

    print("Pipeline terminée : ingestion -> dbt run -> dbt test -> soda scan")


# -------------------------
# ENTRYPOINT
# -------------------------

if __name__ == "__main__":
    pipeline_flow()