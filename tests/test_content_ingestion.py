import pytest
import sqlalchemy as sa
from google.protobuf.message import Message

from library.ingest_articles import (
    insert_looptt_articles_posts_db,
    insert_article_text_content_db,
)
from library.config import Secrets


def test_inserting_records_to_db(articles: Message, test_secrets: Secrets):

    total_inserted_articles = 0
    for article in articles.articles:
        total_inserted_articles += insert_looptt_articles_posts_db(
            article, test_secrets
        )

    assert total_inserted_articles == 12

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for article in articles.articles:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": article.id},
            )


def test_inserting_article_content_to_db(
    example_accompanying_text_content_article: Message,
    extracted_article_text: Message,
    test_secrets: Secrets,
):

    assert (
        insert_looptt_articles_posts_db(
            example_accompanying_text_content_article, test_secrets
        )
        == 1
    )
    assert insert_article_text_content_db(extracted_article_text, test_secrets) == 1

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        conn.execute(
            sa.text("DELETE FROM core.content WHERE core.content.id = :id"),
            {"id": extracted_article_text.id},
        )
        conn.execute(
            sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
            {"id": example_accompanying_text_content_article.id},
        )
