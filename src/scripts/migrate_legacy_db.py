import argparse
import pandas as pd
import uuid
from loguru import logger

from library.config import Secrets, get_secrets
from datetime import datetime, timezone
from google.protobuf.message import Message

from library.ingest_articles import get_unique_articles, insert_looptt_articles_posts_db, insert_article_text_content_db
from library.protobuff_types.trinidad_tobago import looptt_articles_pb2
from library.protobuff_types import core_content_pb2

parser = argparse.ArgumentParser()
parser.add_argument("--csv", "-f", help="Path to the csv containing legacy data")
parser.add_argument(
    "--env_file", help="The path to the environment file used to load all of the secrets"
)

args = parser.parse_args()

if __name__ == "__main__":

    # /Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/configs/data/all_tt_articles.csv
    legacy_df = pd.read_csv(args.csv)
    secrets: Secrets = get_secrets(args.env_file)

    loop_tt_news_articles: list[dict[str, Message]] = []

    for index, row in legacy_df.iterrows():

        old_formatted_extracted_date = datetime.strptime(row["extracted_date"], "%B %d, %Y %I:%M %p")
        old_formatted_published_date = datetime.strptime(row["published_date"], "%B %d, %Y %I:%M %p")

        if row.isnull().any():
            row["content"] = ""

        loop_tt_news_article = looptt_articles_pb2.LoopTTNewsArticle(
            id=row['id'],
            type="news_article",
            created_date=old_formatted_published_date.replace(tzinfo=timezone.utc).timestamp(),
            fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                title=row['title'],
                url=f"{row['source']}{row['url']}",
                extracted_date=old_formatted_extracted_date.replace(tzinfo=timezone.utc).timestamp(),
                published_date=old_formatted_published_date.replace(tzinfo=timezone.utc).timestamp(),
                source_file_path="NULL-Migration"
            )
        )

        loop_tt_text_content = looptt_articles_pb2.ArticlesTextContent(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{row['title']}{old_formatted_published_date.replace(tzinfo=timezone.utc).timestamp()}")),
            source=row['id'],
            type=core_content_pb2.CoreContentTypes.TEXT_CONTENT,
            created_date=old_formatted_published_date.replace(tzinfo=timezone.utc).timestamp(),
            storage_path="NULL-Migration",
            fields={
                "title": row["title"], 
                "text": row["content"]
            },
        )

        loop_tt_news_articles.append({
            "article": loop_tt_news_article,
            "text_content": loop_tt_text_content
        })

    logger.info(f"Extracted {len(loop_tt_news_articles)} from csv")
    

    articles = looptt_articles_pb2.LoopTTArticles(articles=[article['article'] for article in loop_tt_news_articles])

    existing_article_ids: list[dict] = get_unique_articles(articles, secrets)
    duplicate_ids: list[str] = [str(post["id"]) for post in existing_article_ids]
    unique_articles_to_insert: list[dict[str, Message]] = [
        {
            "article": content_dict['article'],
            "text_content":content_dict['text_content'] 
        }
        for content_dict in loop_tt_news_articles
        if content_dict['article'].id not in duplicate_ids
    ]
    logger.info(f"Found {len(unique_articles_to_insert)} unique articles that are not in the postgres db")

    for article_to_insert in unique_articles_to_insert:
        assert insert_looptt_articles_posts_db(
            looptt_article=article_to_insert['article'],
            secrets=secrets
        ) == 1, logger.error(f"Error in uploading article {article_to_insert['article'].title}")
        
        assert insert_article_text_content_db(
            text_content=article_to_insert['text_content'],
            secrets=secrets
        ) == 1, logger.error(f"Error in inserting raw text content for {article_to_insert['text_content'].fields['title']}")