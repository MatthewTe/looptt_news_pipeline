import requests
import argparse
import random
import time
import pprint
import io
import json
import uuid
import pandas as pd
import sqlalchemy as sa
from minio import Minio
from sqlalchemy.dialects.postgresql import ARRAY, UUID

from loguru import logger
from datetime import datetime, timezone
from urllib.parse import parse_qs
from library.protobuff_types.trinidad_tobago import looptt_articles_pb2

from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson, MessageToDict

from library.config import LoopPageConfig, Article, Secrets, get_secrets
from library.html_parsing import (
    extract_article_text_content,
    extract_articles_display_page,
    ArticlesDisplayPage,
    ArticleContent,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "env_file", help="The path to the environment file used to load all of the secrets"
)


def get_unique_articles(articles: Message, secrets: Secrets) -> list[dict]:
    PSQL_Engine: sa.engine.Engine = sa.create_engine(secrets["psql_uri"])
    with PSQL_Engine.connect() as conn, conn.begin():

        get_unique_id_query = sa.text(
            """
            SELECT id
            FROM core.source source
            WHERE source.id = ANY(:ids)
            AND type = 'news_article'
            """
        ).bindparams(sa.bindparam("ids", type_=ARRAY(UUID)))

        existing_id_results = conn.execute(
            get_unique_id_query, {"ids": [article.id for article in articles.articles]}
        )
        existing_ids_dict = existing_id_results.mappings().all()
        return existing_ids_dict


def insert_looptt_articles_posts_db(looptt_article: Message, secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])
    with psql_engine.connect() as conn, conn.begin():
        insert_query = sa.text(
            """
            INSERT INTO core.source (id, type, created_date, fields)
            VALUES (:id, :type, :created_date, :fields);
            """
        )

        posts_to_insert = {
            "id": looptt_article.id,
            "type": looptt_article.type,
            "created_date": datetime.fromtimestamp(
                looptt_article.created_date, tz=timezone.utc
            ),
            "fields": json.dumps(MessageToDict(looptt_article.fields)),
        }

        result = conn.execute(insert_query, posts_to_insert)

        logger.info(f"Inserted {result.rowcount} posts to core.source")
        return result.rowcount

def insert_multiple_looptt_articles_posts_db(looptt_articles: list[Message], secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])
    with psql_engine.connect() as conn, conn.begin():
        insert_query = sa.text(
            """
            INSERT INTO core.source (id, type, created_date, fields)
            VALUES (:id, :type, :created_date, :fields);
            """
        )

        posts_to_insert = [
            {
                "id": article.id,
                "type": article.type,
                "created_date": datetime.fromtimestamp(
                    article.created_date, tz=timezone.utc
                ),
                "fields": json.dumps(MessageToDict(article.fields)),
            }
            for article in looptt_articles
        ]

        result = conn.execute(insert_query, posts_to_insert)

        logger.info(f"Inserted {result.rowcount} posts to core.source")
        return result.rowcount


def insert_article_text_content_db(text_content: Message, secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])
    with psql_engine.connect() as conn, conn.begin():
        insert_query = sa.text(
            """
            INSERT INTO core.content (id, source, type, created_date, storage_path, fields)
            VALUES (:id, :source, :type, :created_date, :storage_path, :fields);
            """
        )

        posts_to_insert = {
            "id": text_content.id,
            "source": text_content.source,
            "type": text_content.type,
            "created_date": datetime.fromtimestamp(
                text_content.created_date, tz=timezone.utc
            ),
            "storage_path": text_content.storage_path,
            "fields": json.dumps(MessageToDict(text_content.fields)),
        }

        result = conn.execute(insert_query, posts_to_insert)

        logger.info(f"Inserted {result.rowcount} posts to core.content")
        return result.rowcount

def insert_multiple_article_text_content_db(text_contents: list[Message], secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])
    with psql_engine.connect() as conn, conn.begin():
        insert_query = sa.text(
            """
            INSERT INTO core.content (id, source, type, created_date, storage_path, fields)
            VALUES (:id, :source, :type, :created_date, :storage_path, :fields);
            """
        )

        posts_to_insert = [
            {
                "id": content.id,
                "source": content.source,
                "type": content.type,
                "created_date": datetime.fromtimestamp(
                    content.created_date, tz=timezone.utc
                ),
                "storage_path": content.storage_path,
                "fields": json.dumps(MessageToDict(content.fields)),
            }
            for content in text_contents
        ]

        result = conn.execute(insert_query, posts_to_insert)

        logger.info(f"Inserted {result.rowcount} posts to core.content")
        return result.rowcount

def process_loop_page(config: LoopPageConfig):

    query_param_str = config["query_param_str"]
    article_category = config["article_category"]
    db_category = config["db_category"]

    headers_lst = [
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0"
        },
        {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393"
        },
    ]

    base_url = f"https://tt.loopnews.com/news/{article_category}"
    article_base_url = "https://tt.loopnews.com"
    query_params = parse_qs(query_param_str.lstrip("?"))

    article_extracted_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("Making request to article thumbnail page")
    articles_response: requests.Response = requests.get(
        base_url, params=query_params, headers=random.choice(headers_lst)
    )
    print(base_url)
    print(articles_response.url)

    articles_response.raise_for_status()
    logger.info(f"Article thumbnail http response: {articles_response.status_code}")

    if not articles_response.ok:
        logger.error(f"Unable to grab article for {articles_response.url}")
        logger.error(articles_response.content)
        return

    # Upload the html file to blob storage with an ID and reference to db.
    MINIO_CLIENT = Minio(
        config["secrets"]["minio_url"],
        access_key=config["secrets"]["minio_access_key"],
        secret_key=config["secrets"]["minio_secret_key"],
        secure=False,
    )
    BUCKET_NAME = "news-articles"

    found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
    if not found:
        MINIO_CLIENT.make_bucket(BUCKET_NAME)
        logger.info("Created bucket", BUCKET_NAME)
    else:
        logger.info("Bucket", BUCKET_NAME, "already exists")

    unique_html_page_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL, name=f"{articles_response.url}_{article_extracted_date}"
        )
    )
    html_file_buffer = io.BytesIO(articles_response.content)
    MINIO_CLIENT.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"loop_tt_articles/{unique_html_page_id}.html",
        data=io.BytesIO(articles_response.content),
        length=html_file_buffer.getbuffer().nbytes,
        content_type="text/html",
    )
    s3_filepath = f"s3://{BUCKET_NAME}/loop_tt_articles/{unique_html_page_id}.html"
    logger.info(
        f"Uploaded html file from url {articles_response.url} to blob storage at {s3_filepath}"
    )

    page_content: ArticlesDisplayPage = extract_articles_display_page(
        articles_response.content, s3_filepath
    )
    logger.info(
        f"Extracted {len(page_content['articles'].articles)} page content from {articles_response.url}"
    )

    # Then we extract all of the article content from each of the pages:
    if len(page_content["articles"].articles) < 1:
        logger.error(f"No Articles found for page {articles_response.url}")
        return

    existing_article_ids: list[dict] = get_unique_articles(
        page_content["articles"], config["secrets"]
    )
    duplicate_ids: list[str] = [str(post["id"]) for post in existing_article_ids]
    unique_articles_to_insert: list[Message] = [
        article
        for article in page_content["articles"].articles
        if article.id not in duplicate_ids
    ]
    logger.info(
        f"Found {len(unique_articles_to_insert)} unique articles that are not in the neo4j database. This differs from the total num articles {len(page_content['articles'].articles)}"
    )

    articles_to_ingest: Message = looptt_articles_pb2.LoopTTArticles()
    articles_to_ingest.articles.extend(unique_articles_to_insert)

    if len(articles_to_ingest.articles) == 0:
        logger.error("No unique articles found. Reached end of dataset. Stopping....")
        return

    for article in articles_to_ingest.articles:

        pprint.pprint(article)

        logger.info(f"Inserting Article {article.fields.url} to database")
        inserted_article: int = insert_looptt_articles_posts_db(
            looptt_article=article, secrets=config["secrets"]
        )

        if inserted_article != 1:
            logger.error(
                f"Was unable to ingest article {article.fields.url} to database"
            )
            continue

        logger.info("Making individual article http request")
        single_page_response = requests.get(
            article.fields.url, headers=random.choice(headers_lst)
        )

        logger.info(f"Full article http response: {single_page_response.status_code}")
        if not single_page_response.ok:
            logger.error(
                f"Error in getting page. Exited with status code {single_page_response.status_code}"
            )
            logger.error(single_page_response.content)
            continue

        article_text_content: Message = extract_article_text_content(
            single_page_response.content, article
        )

        inserted_article_text_content: int = insert_article_text_content_db(
            article_text_content, config["secrets"]
        )
        if inserted_article_text_content != 1:
            logger.error(
                f"Error in inserting article content to databsase {article.fields.title}"
            )
        logger.info(
            f"Sucessfully ingested article and text content for {article.fields.title}"
        )

        time.sleep(random.choice(range(1, 5)))

    if not page_content["next_page"]:
        logger.warning("Next page not found for the article thumbnail page")
        return

    new_config: LoopPageConfig = {
        "article_category": article_category,
        "db_category": db_category,
        "query_param_str": page_content["next_page"],
        "secrets": config["secrets"],
    }

    logger.info(f"Recursively parsing next page with new config {config['secrets']}")
    process_loop_page(new_config)


if __name__ == "__main__":

    crime_config: LoopPageConfig = {
        "query_param_str": "?page=0",
        "article_category": "looptt-crime",
        "db_category": "crime",
        "secrets": get_secrets(),
    }

    process_loop_page(crime_config)

    pass
