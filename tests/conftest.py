import pytest
import os
from google.protobuf.message import Message

from library.protobuff_types.trinidad_tobago import looptt_articles_pb2
from library.protobuff_types import core_content_pb2

from library.html_parsing import ArticlesDisplayPage
from library.config import Secrets


@pytest.fixture(scope="session", autouse=True)
def test_secrets() -> Secrets:
    os.environ["MINIO_URL"] = "127.0.0.1:9000"
    os.environ["MINIO_ACCESS_KEY"] = "test_access_key"
    os.environ["MINIO_SECRET_KEY"] = "test_secret_key"
    os.environ[
        "PSQL_URI"
    ] = "postgresql://reddit_content_dev:test@127.0.0.1:5432/content_dev"

    return {
        "minio_url": os.environ.get("MINIO_URL"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "psql_uri": os.environ.get("PSQL_URI"),
    }


@pytest.fixture(scope="session")
def example_html_string_content() -> str:
    with open("./data/example_articles_content.html", "r") as f:
        content = f.read()

    return content


@pytest.fixture(scope="session")
def example_article_text_content() -> str:
    with open("./data/example_article_text_content.html", "r") as f:
        content = f.read()

    return content


@pytest.fixture()
def example_accompanying_text_content_article() -> Message:
    return looptt_articles_pb2.LoopTTNewsArticle(
        id="d2291bc6-d148-5a3c-8ebe-b6627662a9f4",
        type="news_article",
        created_date=1.74230016e09,
        fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
            title="Son saves elderly dad from attempted home invasion ",
            url="https://tt.loopnews.com/content/son-saves-elderly-dad-attempted-home-invasion",
            extracted_date=1.74230016e09,
            published_date=1.74230016e09,
            source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
        ),
    )


@pytest.fixture()
def extracted_article_text() -> Message:
    return looptt_articles_pb2.ArticlesTextContent(
        id="3a7e6383-5bbd-5dbc-9814-dc8aac31e538",
        source="d2291bc6-d148-5a3c-8ebe-b6627662a9f4",
        type=core_content_pb2.CoreContentTypes.TEXT_CONTENT,
        created_date=1.74230016e09,
        storage_path="s3://bucket_name/folder1/folder2/file1.hmtl",
        fields={
            "title": "Son saves elderly dad from attempted home invasion ",
            "text": "A 40-year-old man narrowly escaped with his life after gunmen shot at him during an attemped home invasion on Monday morning.The victim told police that around 4am, he was at his home in Carapichaima when he got a call from his 75-year-old father who said that he had been awakened by a noise.Upon checking his CCTV cameras, he observed a group of men had jumped the wall to his home located along Ramjass Trace, and were trying to enter. The victim ran out of his home and drove to his father's residence which was a short distance away.Upon getting closer, he began blaring the car horn in an attempt to raise an alarm. He then observed five men jump the wall of his father's home.The men, on seeing the car, began approaching. The victim immediately drove off, however, he heard several  explosions. He then saw the group run from the scene.The police were notified and are continuing enquiries.",
        },
    )


@pytest.fixture()
def example_articles_from_html(articles: Message) -> ArticlesDisplayPage:

    return {"articles": articles, "next_page": "?page=1"}


@pytest.fixture(scope="session")
def articles() -> Message:
    loop_tt_articles = looptt_articles_pb2.LoopTTArticles()
    loop_tt_articles.articles.extend(
        [
            looptt_articles_pb2.LoopTTNewsArticle(
                id="fbac86ea-ec06-5941-a37e-cd0002cf0335",
                type="news_article",
                created_date=1.74230195e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="$10M in Colombian cannabis seized by cops during search exercise ",
                    url="https://tt.loopnews.com/content/10m-colombian-cannabis-seized-cops-during-search-exercise",
                    extracted_date=1.74230195e09,
                    published_date=1.74230195e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="d2291bc6-d148-5a3c-8ebe-b6627662a9f4",
                type="news_article",
                created_date=1.74230016e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Son saves elderly dad from attempted home invasion ",
                    url="https://tt.loopnews.com/content/son-saves-elderly-dad-attempted-home-invasion",
                    extracted_date=1.74230016e09,
                    published_date=1.74230016e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="21129943-0021-5121-9c4b-0104536c7383",
                type="news_article",
                created_date=1.7422999e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Pensioner assaulted, robbed as bandits attack his home ",
                    url="https://tt.loopnews.com/content/pensioner-assaulted-robbed-bandits-attack-his-home",
                    extracted_date=1.7422999e09,
                    published_date=1.7422999e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="2a03731d-130d-5718-b8a9-a9345a898a90",
                type="news_article",
                created_date=1.74229965e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Man beaten with cutlass and robbed during home invasion",
                    url="https://tt.loopnews.com/content/man-beaten-cutlass-and-robbed-during-home-invasion",
                    extracted_date=1.74229965e09,
                    published_date=1.74229965e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="1850ced2-9deb-5e07-be72-325ed726f34e",
                type="news_article",
                created_date=1.74229888e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Known offender killed in shootout with police ",
                    url="https://tt.loopnews.com/content/known-offender-killed-shootout-police",
                    extracted_date=1.74229888e09,
                    published_date=1.74229888e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="c3b93d71-3f2e-5d06-85d2-35a7a2f70011",
                type="news_article",
                created_date=1.74229862e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Man scammed after attempt to purchase car seen on Facebook ",
                    url="https://tt.loopnews.com/content/man-scammed-after-attempt-purchase-car-seen-facebook",
                    extracted_date=1.74229862e09,
                    published_date=1.74229862e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="650d8f46-776a-519d-aa64-3dfd873374d9",
                type="news_article",
                created_date=1.74229837e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Former SRP ambushed and killed in Curepe bar",
                    url="https://tt.loopnews.com/content/former-srp-ambushed-and-killed-curepe-bar",
                    extracted_date=1.74229837e09,
                    published_date=1.74229837e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="d9cebf81-d4fd-5a0e-9650-c009c38ebf76",
                type="news_article",
                created_date=1.74221875e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Venezuelan man viciously stabbed after trying to stop bar fight",
                    url="https://tt.loopnews.com/content/venezuelan-man-viciously-stabbed-after-trying-stop-bar-fight",
                    extracted_date=1.74221875e09,
                    published_date=1.74221875e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="669536fe-3431-5f31-b05b-c2dabd557360",
                type="news_article",
                created_date=1.74221632e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Three people shot at wake in Arima",
                    url="https://tt.loopnews.com/content/three-people-shot-wake-arima",
                    extracted_date=1.74221632e09,
                    published_date=1.74221632e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="a66dcfe8-6d63-5920-b2ba-bc5e646d5d56",
                type="news_article",
                created_date=1.7422153e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Soldier charged with Pleasantville mother's murder",
                    url="https://tt.loopnews.com/content/soldier-charged-pleasantville-mothers-murder",
                    extracted_date=1.7422153e09,
                    published_date=1.7422153e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="4110f6be-c359-5ed3-9461-e3e3af402644",
                type="news_article",
                created_date=1.74221402e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Man charged for selling iguanas during closed Hunting Season",
                    url="https://tt.loopnews.com/content/man-charged-selling-iguanas-during-closed-hunting-season",
                    extracted_date=1.74221402e09,
                    published_date=1.74221402e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
            looptt_articles_pb2.LoopTTNewsArticle(
                id="c5b847ff-f26d-5873-a459-a519249ff96d",
                type="news_article",
                created_date=1.7422135e09,
                fields=looptt_articles_pb2.LoopTTNewsArticle.NewsArticleFields(
                    title="Carenage gas station attendant assaulted, robbed",
                    url="https://tt.loopnews.com/content/carenage-gas-station-attendant-assaulted-robbed",
                    extracted_date=1.7422135e09,
                    published_date=1.7422135e09,
                    source_file_path="s3://bucket_name/folder1/folder2/file1.hmtl",
                ),
            ),
        ]
    )
    return loop_tt_articles
