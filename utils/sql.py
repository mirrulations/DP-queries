import psycopg
import os
from queries.utils.secrets_manager import get_secret

conn = None
def connect():
    global conn
    if conn is not None: 
        return conn
    base_secret = get_secret("prod/databases")
    secret_name = base_secret.get("DB_SECRET_NAME")

    if not secret_name:
        raise ValueError("Missing DB_SECRET_NAME in 'prod/databases'")

    secret = get_secret(secret_name)

    conn_params = {
        "dbname": secret['db'],
        "user": secret['username'],
        "password": secret['password'],
        "host": secret['host'],
        "port": secret['port'],
    }

    conn = psycopg.connect(**conn_params)
    return conn