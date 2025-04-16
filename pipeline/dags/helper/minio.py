from airflow.hooks.base import BaseHook
from minio import Minio

def get_clients():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )

    return client