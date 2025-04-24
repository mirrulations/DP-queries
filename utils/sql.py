import psycopg
import os
from queries.utils.secrets_manager import get_secret


def connect():
    """
    Connects to a PostgreSQL database using credentials from environment variables 
    or AWS Secrets Manager, based on the `AWS_SAM_LOCAL` flag.
    """
    if os.getenv("AWS_SAM_LOCAL", ""):
        secret = {
            "db": os.environ.get("POSTGRES_DB"),
            "username": os.environ.get("POSTGRES_USER"),
            "password": os.environ.get("POSTGRES_PASSWORD"),
            "host": os.environ.get("POSTGRES_HOST"),
            "port": os.environ.get("POSTGRES_PORT"),
        }
    else:
        secret_name = os.environ.get("DB_SECRET_NAME")
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
