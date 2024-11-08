import psycopg2
from google.oauth2 import service_account
from google.cloud import storage
from psycopg2 import sql


import os
from dotenv import load_dotenv
load_dotenv()

cred = service_account.Credentials.from_service_account_file(os.getenv('CRED_PATH'))
db_user = 'postgres'
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')

def list_cs_files(bucket_name):
    storage_client = storage.Client(credentials=cred)
    bucket = storage_client.bucket(bucket_name)
    
    folder_structure = {}

    blobs = bucket.list_blobs()
    for blob in blobs:
        parts = blob.name.split('/')
        if (len(parts) == 5 and
            parts[-1].split('.')[-1] in ["jpeg", "jpg", "png", "gif", "bmp", "tiff", "webp", "svg", "heic"]):  
            key = f'{parts[1]}+{parts[2]}+{parts[3]}'
            if key not in folder_structure:
                folder_structure[key] = []
            folder_structure[key].append((parts[-1], blob.public_url))

    return folder_structure

def database_exists(db_name):
    conn = psycopg2.connect(dbname="postgres", user=db_user, password=db_password, host=db_host)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def schema_exists(conn, schema_name):
    cursor = conn.cursor()
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema_name,))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists

def table_exists(conn, schema_name, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (schema_name, table_name))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists

def setup_database_and_table(db_name, schema_name, table_name, image_data):
    if not database_exists(db_name):
        conn = psycopg2.connect(dbname="postgres", user=db_user, password=db_password, host=db_host)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully.")
        cursor.close()
        conn.close()

    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host)
    
    if not schema_exists(conn, schema_name):
        cursor = conn.cursor()
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        print(f"Schema '{schema_name}' created in database '{db_name}'.")
        cursor.close()

    if not table_exists(conn, schema_name, table_name):
        cursor = conn.cursor()
        cursor.execute(sql.SQL("""
        CREATE TABLE {}.{} (
            id SERIAL PRIMARY KEY,
            file_name TEXT NOT NULL,
            url TEXT NOT NULL
        )
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name)))
        print(f"Table '{table_name}' created in schema '{schema_name}'.")
        cursor.close()

    cursor = conn.cursor()
    for file_name, url in image_data:
        cursor.execute(sql.SQL("SELECT 1 FROM {}.{} WHERE url = %s")
                       .format(sql.Identifier(schema_name), sql.Identifier(table_name)), (url,))
        if cursor.fetchone() is None:
            cursor.execute(sql.SQL("INSERT INTO {}.{} (file_name, url) VALUES (%s, %s)")
                           .format(sql.Identifier(schema_name), sql.Identifier(table_name)), (file_name, url))
            print(f"Inserted image '{file_name}' into table '{table_name}'.")

    conn.commit()
    cursor.close()
    conn.close()

def store_images_in_databases(bucket_name):
    folder_structure = list_cs_files(bucket_name)
    
    for key, image_data in folder_structure.items():
        db_name, schema_name, table_name = key.split('+')
        setup_database_and_table(db_name, schema_name, table_name, image_data)

store_images_in_databases('vivi_app')
