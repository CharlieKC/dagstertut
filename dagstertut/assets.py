import base64
from io import BytesIO
import json
import os
from pathlib import Path
from typing import List
from dagster_duckdb import DuckDBResource
from matplotlib import pyplot as plt

import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset, get_dagster_logger

DATA_DIR = Path(__file__).parent.parent / "data"
TOPSTORY_ID_FILE = DATA_DIR / "topstory_ids.json"

@asset
def topstory_ids() -> List:
    """This is an asset!
    
        Its a software defined asset. 
    
    """
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]

    return ( top_new_story_ids )

@asset(
    group_name="hackernews",
    io_manager_key="database_io_manager",
)
def topstories(context: AssetExecutionContext,
               topstory_ids: list) -> pd.DataFrame:
    logger = get_dagster_logger()
    results = []

    for item_id in topstory_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

        if len(results) % 20 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    # context is a special variable injected in by dagster
    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    
    return df

@asset(
        group_name="hackernews",
        io_manager_key="duckdb"
)
def topstories_db(context: AssetExecutionContext,
        ) -> pd.DataFrame:
    
    df = pd.DataFrame({"a": ["dog", "cat"], "wonky": [50, 1000]})

    return df

    # logger = get_dagster_logger()
    
    # with duckdb.get_connection() as conn:
    #     conn.execute("CREATE TABLE news.top AS SELECT * FROM topstories")

    


    # logger.info(f"Stored {len(topstories)} rows in ")
    

@asset
def most_frequent_words(context: AssetExecutionContext,
                        topstories: pd.DataFrame) -> dict:
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    # loop through the titles and count the frequency of each word
    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    # Make a bar chart of the top 25 words
    plt.figure(figsize=(10, 6))
    plt.bar(top_words.keys(), top_words.values())
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    context.add_output_metadata(metadata={"plot": MetadataValue.md(md_content)})
    
    return top_words