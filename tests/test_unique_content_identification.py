import pytest
import sqlalchemy as sa

from library.config import Secrets
from library.ingest_articles import get_unique_articles, insert_looptt_articles_posts_db
from library.protobuff_types.trinidad_tobago import looptt_articles_pb2

from google.protobuf.message import Message


def test_identifying_unique_articles(articles: Message, test_secrets: Secrets):

    existing_articles: list[dict] = get_unique_articles(articles, test_secrets)

    # Currently should have zero articles in the database:
    assert len(existing_articles) == 0

    # Inserting two articles into the database:
    first_two_articles = looptt_articles_pb2.LoopTTArticles()
    first_two_articles.articles.extend(articles.articles[0:2])

    inserted_articles = 0
    for article in first_two_articles.articles:
        inserted_articles += insert_looptt_articles_posts_db(article, test_secrets)
    assert inserted_articles == 2

    existing_uploaded_articles = get_unique_articles(articles, test_secrets)
    duplicate_ids: list[str] = [str(post["id"]) for post in existing_uploaded_articles]
    unique_articles_to_insert: list[Message] = [
        article for article in articles.articles if article.id not in duplicate_ids
    ]

    duplicated_articles_removed = looptt_articles_pb2.LoopTTArticles()
    duplicated_articles_removed.articles.extend(unique_articles_to_insert)

    remaining_unique_articles = looptt_articles_pb2.LoopTTArticles()
    remaining_unique_articles.articles.extend(articles.articles[2:])

    assert remaining_unique_articles == duplicated_articles_removed

    # Inserted the rest of the aricles:
    inserted_articles = 0
    for article in duplicated_articles_removed.articles:
        inserted_articles += insert_looptt_articles_posts_db(article, test_secrets)
    assert inserted_articles == 10

    existing_uploaded_articles = get_unique_articles(articles, test_secrets)
    duplicate_ids: list[str] = [str(post["id"]) for post in existing_uploaded_articles]
    unique_articles_to_insert: list[Message] = [
        article for article in articles.articles if article.id not in duplicate_ids
    ]
    assert len(unique_articles_to_insert) == 0

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for article in articles.articles:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": article.id},
            )
