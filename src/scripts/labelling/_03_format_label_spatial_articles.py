import requests
import sqlalchemy as sa
import pandas as pd
import pprint
import geopandas as gpd
import shapely
import time
from pathlib import Path
from loguru import logger
import networkx as nx
import osmnx as ox
import matplotlib.pyplot as plt
import contextily as cx

import traceback
from datetime import datetime
import numpy as np
import json
import sys
import csv
import argparse


# logger.add(f"./{datetime.now()}_spatial_label_error.log", level="ERROR", colorize=False, backtrace=True, diagnose=True)

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input_file", "-f", help="Filepath for the source raw article data"
)

parser.add_argument(
    "--label_file",
    "-lf",
    help="The filepath to the csv containing the crime type and location name labels",
)

parser.add_argument(
    "--geocoded_parquet",
    "-p",
    help="The path to the spatial parquet file containing spatial labels to be applied to the dataset",
)

parser.add_argument("--output_file", "-o", help="The labelled outputs")

if __name__ == "__main__":

    args = parser.parse_args()

    raw_article_data_df: pd.DataFrame = pd.read_csv(args.input_file)
    logger.info(f"Loaded raw articles data from {args.input_file}")
    print(raw_article_data_df)

    article_location_label_df: pd.DataFrame = pd.read_csv(args.label_file)
    logger.info(
        f"Loaded the article crime type and location label from {args.label_file}"
    )
    print(article_location_label_df)

    geocoded_parquet_gdf: gpd.GeoDataFrame = gpd.read_parquet(args.geocoded_parquet)
    logger.info(
        f"Loaded the geodataframe for the geocoded point from {args.geocoded_parquet}"
    )
    print(geocoded_parquet_gdf)

    # Joining all data together:
    articles_w_labels_df: pd.DataFrame = pd.merge(
        raw_article_data_df, article_location_label_df, left_on="id", right_on="id"
    )

    logger.info(f"Merged dataframes with labels")
    print(articles_w_labels_df)

    articles_w_geocoded_df: gpd.GeoDataFrame = pd.merge(
        articles_w_labels_df,
        geocoded_parquet_gdf,
        left_on="location",
        right_on="location",
    )

    logger.info(f"Merged the geocoded data to the articles dataset")
    print(articles_w_geocoded_df)

    articles_w_geocoded_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
        articles_w_geocoded_df, geometry=articles_w_geocoded_df["geometry"]
    )

    tt_bbox = [-61.95, 10.0, -60.895, 10.89]

    articles_codes_within_country_gdf: gpd.GeoDataFrame = articles_w_geocoded_gdf.cx[
        tt_bbox[0] : tt_bbox[1], tt_bbox[2] : tt_bbox[3]
    ]
    logger.warning(
        f"Detected {len(articles_w_geocoded_gdf) - len(articles_codes_within_country_gdf)} points outside TT"
    )

    logger.info(f"Writing all joined geocoded data to {args.output_file}")

    articles_codes_within_country_gdf.to_parquet(args.output_file)
    articles_codes_within_country_gdf.to_csv(
        args.output_file.replace(".parquet", ".csv")
    )

    with open(args.output_file.replace(".parquet", ".json"), "w") as f:
        f.write(articles_codes_within_country_gdf.to_json())

    print(articles_codes_within_country_gdf)
