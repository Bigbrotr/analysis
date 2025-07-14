import os
import sys
import psycopg2
import numpy as np
import pandas as pd
import polars as pl
from dotenv import load_dotenv


def generate_relay_synchronization_csv(data_folder, bigbrotr):
    """Generate relay_synchronization.csv if it does not exist."""
    if 'relay_synchronization.csv' not in os.listdir(data_folder):
        query = """
        SELECT
            latest.relay_url AS relay_url,
            e.created_at AS timestamp,
            latest.seen_at AS seen_at
        FROM (
            SELECT DISTINCT ON (relay_url)
                relay_url,
                seen_at,
                event_id
            FROM
                events_relays
            ORDER BY
                relay_url,
                seen_at DESC
        ) AS latest
        JOIN events e ON e.id = latest.event_id;
        """
        with bigbrotr.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=['relay_url', 'timestamp', 'seen_at'])
        df['timestamp_month'] = pd.to_datetime(
            df['timestamp'], unit='s').dt.to_period('M')
        df['seen_at_day'] = pd.to_datetime(
            df['seen_at'], unit='s').dt.to_period('D')
        df = df.sort_values(
            by=['seen_at_day', 'timestamp_month'], ascending=True)
        df.to_csv(os.path.join(
            data_folder, 'relay_synchronization.csv'), index=False)
        print("relay_synchronization.csv generated.")
    else:
        print("relay_synchronization.csv already exists.")


def generate_events_csv(data_folder, bigbrotr):
    """Generate events.csv if it does not exist."""
    if "events.csv" not in os.listdir(data_folder):
        with bigbrotr.cursor() as cur:
            with open(os.path.join(data_folder, 'events.csv'), 'w') as f:
                cur.copy_expert(
                    "COPY (SELECT id, pubkey, created_at, kind FROM events) TO STDOUT WITH CSV HEADER", f)
        print("events.csv generated.")
    else:
        print("events.csv already exists.")


def generate_events_relays_csv(data_folder, bigbrotr):
    """Generate events_relays.csv if it does not exist."""
    if "events_relays.csv" not in os.listdir(data_folder):
        with bigbrotr.cursor() as cur:
            with open(os.path.join(data_folder, 'events_relays.csv'), 'w') as f:
                cur.copy_expert(
                    "COPY (SELECT event_id, relay_url FROM events_relays) TO STDOUT WITH CSV HEADER", f)
        print("events_relays.csv generated.")
    else:
        print("events_relays.csv already exists.")


def generate_pubkey_follow_pubkey_csv(data_folder, bigbrotr):
    """Generate pubkey_follow_pubkey.csv if it does not exist."""
    def process_tags_3(tags):
        result = set()
        for tag in tags:
            if len(tag) >= 2 and tag[0] == 'p' and len(tag[1]) == 64:
                result.add(tag[1])
        return list(result)
    if 'pubkey_follow_pubkey.csv' not in os.listdir(data_folder):
        query = """
        SELECT DISTINCT ON (pubkey) pubkey, tags
        FROM events
        WHERE kind = 3
        ORDER BY pubkey, created_at DESC;
        """
        with bigbrotr.cursor() as cursor:
            cursor.execute(query)
            events = cursor.fetchall()
        events_df = pd.DataFrame(events, columns=['pubkey', 'tags'])
        events_df['following'] = events_df['tags'].apply(process_tags_3)
        events_df = events_df.drop(columns=['tags'])
        events_df = events_df.explode('following')
        events_df = events_df.rename(
            columns={'pubkey': 'pubkey_src', 'following': 'pubkey_dst'})
        events_df = events_df.drop_duplicates()
        events_df.to_csv(os.path.join(
            data_folder, 'pubkey_follow_pubkey.csv'), index=False)
        print("pubkey_follow_pubkey.csv generated.")
    else:
        print("pubkey_follow_pubkey.csv already exists.")


def generate_pubkey_rw_relay_csv(data_folder, bigbrotr):
    """Generate pubkey_rw_relay.csv if it does not exist."""
    def process_10002_tags(tags):
        result = []
        for tag in tags:
            try:
                taglen = len(tag)
                if taglen in [2, 3] and tag[0] == 'r':
                    relay_url = Relay(tag[1]).url
                    if taglen == 2:
                        result.append([relay_url, True, True])
                    elif taglen == 3 and (tag[2] == 'read' or tag[2] == 'write'):
                        result.append(
                            [relay_url, tag[2] == 'read', tag[2] == 'write'])
            except (ValueError, TypeError):
                continue
        return result
    if 'pubkey_rw_relay.csv' not in os.listdir(data_folder):
        query = """
        SELECT DISTINCT ON (pubkey) pubkey, tags
        FROM events
        WHERE kind = 10002
        ORDER BY pubkey, created_at DESC;
        """
        with bigbrotr.cursor() as cursor:
            cursor.execute(query)
            events = cursor.fetchall()
        events_df = pd.DataFrame(events, columns=['pubkey', 'tags'])
        events_df['tags'] = events_df['tags'].apply(process_10002_tags)
        events_df = events_df.explode('tags')
        events_df['relay_url'] = events_df['tags'].apply(
            lambda x: x[0] if isinstance(x, list) else np.nan)
        events_df['read'] = events_df['tags'].apply(
            lambda x: x[1] if isinstance(x, list) else np.nan)
        events_df['write'] = events_df['tags'].apply(
            lambda x: x[2] if isinstance(x, list) else np.nan)
        events_df = events_df.drop(columns=['tags'])
        events_df = events_df.dropna().drop_duplicates()
        events_df = events_df.groupby(['pubkey', 'relay_url']).aggregate(
            {'read': 'any', 'write': 'any'}).reset_index()
        events_df.to_csv(os.path.join(
            data_folder, 'pubkey_rw_relay.csv'), index=False)
        print("pubkey_rw_relay.csv generated.")
    else:
        print("pubkey_rw_relay.csv already exists.")


def generate_relay_stats_csv(data_folder, bigbrotr):
    # TODO: add all relay_metadata information to relay_stats.csv
    """Generate relay_stats.csv if it does not exist."""
    if 'relay_stats.csv' not in os.listdir(data_folder):
        events_relays = pl.read_csv(
            os.path.join(DATA_FOLDER, 'events_relays.csv'))
        events = pl.read_csv(os.path.join(
            DATA_FOLDER, 'events.csv')).rename({'id': 'event_id'})
        events_relays = events_relays.join(events, on='event_id', how='left')
        relay_stats = events_relays.group_by("relay_url").agg([
            pl.col("event_id").n_unique().alias("num_events"),
            pl.col("pubkey").n_unique().alias("num_pubkeys"),
            pl.col("created_at").min().alias("first_eventdate"),
            pl.col("created_at").max().alias("last_eventdate")
        ])
        nunique_pubkeys = events_relays.select(
            pl.col("pubkey").n_unique()).to_numpy()[0][0]
        nunique_events = events_relays.select(
            pl.col("event_id").n_unique()).to_numpy()[0][0]
        relay_stats = relay_stats.with_columns([
            (pl.col("num_events") / nunique_events * 100).alias("pct_events"),
            (pl.col("num_pubkeys") / nunique_pubkeys * 100).alias("pct_pubkeys"),
        ])
        query = """
        SELECT
            url AS relay_url,
            network
        FROM relays
        """
        with bigbrotr.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        relays = pl.DataFrame(
            rows, schema=['relay_url', 'network'], orient='row')
        relay_stats = relay_stats.join(relays, on='relay_url', how='left')
        relay_stats.write_csv(os.path.join(DATA_FOLDER, 'relay_stats.csv'))
        print("relay_stats.csv generated.")
    else:
        print("relay_stats.csv already exists.")


def generate_pubkey_stats_csv(data_folder):
    # TODO: add for example n_relay_coverage and other stats to pubkey_stats.csv
    """Generate pubkey_stats.csv if it does not exist."""
    if 'pubkey_stats.csv' not in os.listdir(data_folder):
        events = pl.read_csv(os.path.join(data_folder, 'events.csv'))
        pubkey_follow_pubkey = pl.read_csv(
            os.path.join(data_folder, 'pubkey_follow_pubkey.csv'))
        pubkey_rw_relay = pl.read_csv(
            os.path.join(data_folder, 'pubkey_rw_relay.csv'))
        pubkey_stats = (
            events
            .sort(["pubkey", "created_at"])
            .group_by("pubkey")
            .agg([
                pl.len().alias("event_count"),
                pl.min("created_at").alias("first_eventdate"),
                pl.max("created_at").alias("last_eventdate"),
                pl.col("created_at").diff().alias("intervals")
            ])
            .explode("intervals")
            .group_by("pubkey")
            .agg([
                pl.first("event_count"),
                pl.first("first_eventdate"),
                pl.first("last_eventdate"),
                (pl.first("last_eventdate") -
                 pl.first("first_eventdate")).alias("lifespan"),
                pl.mean("intervals").alias("mean_interval"),
                pl.median("intervals").alias("median_interval"),
                pl.std("intervals").alias("std_interval")
            ])
        )
        pubkey_stats = pubkey_stats.join(
            pubkey_rw_relay.group_by("pubkey").agg([
                (pl.when(pl.col("read")).then(1).otherwise(
                    0).sum().alias("read_relay_count")),
                (pl.when(pl.col("write")).then(1).otherwise(
                    0).sum().alias("write_relay_count"))
            ]),
            on="pubkey",
            how="left"
        )
        pubkey_stats = pubkey_stats.join(
            pubkey_follow_pubkey.group_by("pubkey_src").agg([
                pl.count("pubkey_dst").alias("following_count")
            ]).rename({"pubkey_src": "pubkey"}),
            on="pubkey",
            how="left"
        )
        pubkey_stats = pubkey_stats.join(
            pubkey_follow_pubkey.group_by("pubkey_dst").agg([
                pl.count("pubkey_src").alias("followers_count")
            ]).rename({"pubkey_dst": "pubkey"}),
            on="pubkey",
            how="left"
        )
        pubkey_stats = pubkey_stats.with_columns(
            pl.col('followers_count').fill_null(0),
            pl.col('following_count').fill_null(0)
        )
        pubkey_stats.write_csv(os.path.join(data_folder, 'pubkey_stats.csv'))
        print("pubkey_stats.csv generated.")
    else:
        print("pubkey_stats.csv already exists.")


if __name__ == "__main__":
    load_dotenv()
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    LIB_FOLDER = os.getenv("LIB_FOLDER")
    sys.path.append(LIB_FOLDER)
    from relay import Relay
    bigbrotr = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )
    generate_relay_synchronization_csv(DATA_FOLDER, bigbrotr)
    generate_events_csv(DATA_FOLDER, bigbrotr)
    generate_events_relays_csv(DATA_FOLDER, bigbrotr)
    generate_pubkey_follow_pubkey_csv(DATA_FOLDER, bigbrotr)
    generate_pubkey_rw_relay_csv(DATA_FOLDER, bigbrotr)
    generate_relay_stats_csv(DATA_FOLDER, bigbrotr)
    generate_pubkey_stats_csv(DATA_FOLDER)
    print("All data files generated successfully.")
    bigbrotr.close()
