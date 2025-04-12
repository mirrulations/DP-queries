import os
from dotenv import load_dotenv
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from queries.utils.secrets_manager import get_secret

def connect():

    env = os.getenv("ENVIRONMENT", "").lower()
    print("[DEBUG] ENVIRONMENT from opensearch:", env)

    if env == 'local':
        print("[DEBUG] Using local environment variables for OpenSearch.")
        load_dotenv()
        host = os.getenv('OPENSEARCH_HOST', 'opensearch-node1')
        port = os.getenv('OPENSEARCH_PORT', '9200')
        password = os.getenv('OPENSEARCH_INITIAL_ADMIN_PASSWORD')

        if not password:
            raise ValueError("Missing OPENSEARCH_INITIAL_ADMIN_PASSWORD in local environment.")

        auth = ('admin', password)
        use_ssl = False
        verify_certs = False
        connection_class = RequestsHttpConnection
    else:
        print("[DEBUG] Using AWS Secrets Manager for OpenSearch.")
        region = os.environ.get('AWS_REGION', 'us-east-1')
        base_secret = get_secret("prod/databases")
        resolved_os_secret_name = base_secret.get("OS_SECRET_NAME")

        if not resolved_os_secret_name:
            raise ValueError("Missing OS_SECRET_NAME in 'prod/databases'")

        secret = get_secret(resolved_os_secret_name)

        host = secret.get("host")
        port = secret.get("port")

        if not host or not port:
            raise ValueError("Missing host or port in resolved OpenSearch secret.")

        auth = AWSV4SignerAuth(boto3.Session().get_credentials(), region, 'aoss')
        use_ssl = True
        verify_certs = False
        connection_class = RequestsHttpConnection

    client = OpenSearch(
        hosts=[{'host': host, 'port': int(port)}],
        http_compress=True,
        http_auth=auth,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        connection_class=connection_class,
        pool_maxsize=20,
        timeout=30
    )

    return client
