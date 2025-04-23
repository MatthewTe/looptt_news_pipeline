import requests
import sqlalchemy as sa
import pandas as pd
import pprint
from pathlib import Path
from loguru import logger

import traceback
import numpy as np
import json
import sys
import csv
import argparse

from library.config import get_secrets
from library.article_labelers import perform_ner_on_entity

parser = argparse.ArgumentParser()
parser.add_argument(
    "--env_file",
    "-e",
    help="The path to the environment file used to load all of the secrets",
)

parser.add_argument(
    "--output_file",
    "-f",
    help="The name of the output csv file where the labelling will be stored",
)

args = parser.parse_args()

if __name__ == "__main__":

    csv_path: Path = Path(args.output_file)
    secrets = get_secrets(args.env_file)

    if csv_path.is_file():
        logger.info(f"{csv_path} metadata already exists - loading all ids from csv")
        existing_metadata_df = pd.read_csv(csv_path)
        already_labelled_ids: list[str] = list(existing_metadata_df["id"].unique())

        logger.info(
            f"{len(already_labelled_ids)} existing ids in the csv. These ids will be skipped from the sql query"
        )
    else:
        logger.warning(
            f"A csv does not exist at location: {csv_path}. Creating an empty csv"
        )
        already_labelled_ids = None
        with open(csv_path, "w", newline="") as csvfile:
            columns_names = ["id", "location", "label"]
            writer = csv.DictWriter(csvfile, fieldnames=columns_names)
            writer.writeheader()

    # /Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/configs/data/all_tt_articles.csv
    db = False
    if db:
        PSQL_ENGINE = sa.create_engine(secrets["psql_uri"])
        with PSQL_ENGINE.connect() as conn, conn.begin():
            if already_labelled_ids or len(already_labelled_ids) != 0:
                placeholders = ", ".join(
                    [f":id_{i}" for i in range(len(already_labelled_ids))]
                )
                articles_query = sa.text(
                    f"""
                    SELECT
                        source.id as id,
                        content.fields->>'title' AS article_title,
                        content.fields->>'text' AS text
                    FROM core.source AS source
                    JOIN core.content AS content
                    ON source.id = content.source
                    WHERE source.type = 'news_article'
                    AND and source.id NOT IN ({placeholders})
                    LIMIT 500
                    """
                )

                all_articles_df = pd.read_sql(
                    articles_query,
                    con=conn,
                    params={
                        f"id_{i}": id_ for i, id_ in enumerate(already_labelled_ids)
                    },
                )
            else:
                articles_query = sa.text(
                    f"""
                    SELECT
                        source.id as id,
                        content.fields->>'title' AS article_title,
                        content.fields->>'text' AS text
                    FROM core.source AS source
                    JOIN core.content AS content
                    ON source.id = content.source
                    WHERE source.type = 'news_article'
                    LIMIT 500
                    """
                )

                all_articles_df = pd.read_sql(articles_query, con=conn)

        logger.info(
            f"Queried {len(all_articles_df)} articles from the database - stating to process"
        )

    else:
        all_articles_df = pd.read_csv(
            "/Volumes/T7/Repos/java_webpage_content_extractor_POC/configs/data/all_tt_articles.csv"
        )

        if already_labelled_ids and len(already_labelled_ids) != 0:
            logger.info(f"Filtering all articles ids for the already labelled data:")
            all_articles_df = all_articles_df[
                ~all_articles_df["id"].isin(already_labelled_ids)
            ]

    for i, row in all_articles_df.iterrows():
        # Example output:
        # {
        #    "Metadata": {
        #        "Crime_Type":"shooting",
        #        "Location": ["Port of Spain", "Tunapuna", "Example Sheet"]
        #    }
        # }
        labeled_response: dict | None = perform_ner_on_entity(row["content"])
        print(labeled_response)

        if labeled_response is None:
            continue

        try:
            logger.info(f"Adding location labels to the csv")
            with open(csv_path, "a", newline="") as csvfile:
                columns_names = ["id", "location", "label"]
                writer = csv.DictWriter(csvfile, fieldnames=columns_names)

                extracted_locations = labeled_response["labels"]["Metadata"]["Location"]
                crime_type = labeled_response["labels"]["Metadata"]["Crime_Type"]

                if isinstance(crime_type, list):
                    for crime in crime_type:

                        for location in extracted_locations:
                            new_row = {
                                "id": row["id"],
                                "location": location,
                                "label": crime,
                            }
                            writer.writerow(new_row)
                            logger.info(f"Appended label for {row['id']}")
                            pprint.pprint(new_row)
                else:
                    for location in extracted_locations:
                        new_row = {
                            "id": row["id"],
                            "location": location,
                            "label": crime_type,
                        }
                        writer.writerow(new_row)
                        logger.info(f"Appended label for {row['id']}")
                        pprint.pprint(new_row)

        except Exception as e:
            logger.error(f"Error in adding label for {row['id']}")
            logger.error(traceback.format_exc())
            continue
