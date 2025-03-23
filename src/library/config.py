from dotenv import load_dotenv
from typing import TypedDict
from datetime import datetime

import os


class Secrets(TypedDict):
    minio_url: str
    minio_access_key: str
    minio_secret_key: str
    psql_uri: str


class LoopPageConfig(TypedDict):
    query_param_str: str
    article_category: str
    db_category: str
    secrets: Secrets


class Article(TypedDict):
    id: str
    title: str
    url: str
    type: str
    content: str
    source: str
    extracted_date: str
    published_date: str
    extracted_from: str
    article_category: str


def get_secrets(env_path) -> Secrets:

    load_dotenv(env_path)

    return {
        "minio_url": os.environ.get("MINIO_URL"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "psql_uri": os.environ.get("PSQL_URI"),
    }
