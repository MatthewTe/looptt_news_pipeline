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


logger.add(
    f"./{datetime.now()}_location_label.log",
    level="ERROR",
    colorize=False,
    backtrace=True,
    diagnose=True,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_file", "-f", help="The name of the input csv contaning the labelled data"
)

parser.add_argument("--output_file", "-o", help="The path to the output parquet file")

args = parser.parse_args()

if __name__ == "__main__":

    csv_path: Path = Path(args.input_file)
    output_path: Path = Path(args.output_file)

    labelled_article_df = pd.read_csv(csv_path)

    print(labelled_article_df)

    print(f"Unique locations to label: {len(labelled_article_df['location'].unique())}")
    unique_locations: list[str] = labelled_article_df["location"].unique()

    # labelled_locations: dict[str, tuple[float, float]] = {}
    labelled_locations = []
    for location in unique_locations:
        try:
            geocoded_point: tuple[float, float] = ox.geocoder.geocode(location)
            # labelled_locations[location] = geocoded_point
            labelled_locations.append(
                {
                    "location": location,
                    "lat": geocoded_point[0],
                    "lon": geocoded_point[1],
                }
            )
            logger.info(f"Added label for location {location} - {geocoded_point}")
            time.sleep(3)

        except Exception as e:
            logger.error(f"Error in geocoding and logging item: {e}")

    locations_df = pd.DataFrame.from_records(labelled_locations)
    location_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
        locations_df,
        geometry=gpd.points_from_xy(x=locations_df["lon"], y=locations_df["lat"]),
    )

    logger.info(f"Writing {location_gdf} to {output_path}")

    ax = location_gdf.plot(figsize=(10, 10), alpha=0.5, edgecolor="k")
    # cx.add_basemap(ax)
    # plt.show()

    location_gdf.to_parquet(output_path)
