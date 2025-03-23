import pytest
import requests
from google.protobuf.message import Message

from library.html_parsing import (
    extract_articles_display_page,
    extract_article_text_content,
    ArticlesDisplayPage,
)


def test_article_extraction_from_html(
    example_html_string_content: str, example_articles_from_html: ArticlesDisplayPage
):
    articles: ArticlesDisplayPage = extract_articles_display_page(
        html_str=example_html_string_content,
        html_file_s3_path="s3://bucket_name/folder1/folder2/file1.hmtl",
    )

    assert articles == example_articles_from_html


@pytest.mark.skip()
def test_article_text_ingestion(
    example_article_text_content: str,
    example_accompanying_text_content_article: Message,
    extracted_article_text: Message,
):

    article_text_content: Message = extract_article_text_content(
        example_article_text_content, example_accompanying_text_content_article
    )

    assert article_text_content == extracted_article_text
