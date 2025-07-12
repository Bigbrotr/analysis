import os
import psycopg2
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


def generate_relays_events_csv(data_folder, bigbrotr):
    """Generate relays_events.csv if it does not exist."""
    if "events_relays.csv" not in os.listdir(data_folder):
        with bigbrotr.cursor() as cur:
            with open(os.path.join(data_folder, 'events_relays.csv'), 'w') as f:
                cur.copy_expert(
                    "COPY (SELECT event_id, relay_url FROM events_relays) TO STDOUT WITH CSV HEADER", f)
        print("events_relays.csv generated.")
    else:
        print("events_relays.csv already exists.")


def generate_relay_event_count_csv(data_folder, bigbrotr):
    """Generate relay_event_count.csv if it does not exist."""
    if 'relay_event_count.csv' not in os.listdir(data_folder):
        relays_events = pl.read_csv(
            os.path.join(data_folder, 'events_relays.csv'))
        relay_event_count = relays_events.group_by('relay_url').agg(
            pl.count('event_id').alias('event_count')).sort('event_count', descending=True)
        query = """
        SELECT relay_url, network
        FROM relays
        """
        with bigbrotr.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        relays = pl.DataFrame(rows, schema=['relay_url', 'network'])
        relay_event_count = relay_event_count.join(
            relays, on='relay_url', how='left')
        relay_event_count.write_csv(os.path.join(
            data_folder, 'relay_event_count.csv'))
        print("relay_event_count.csv generated.")
    else:
        print("relay_event_count.csv already exists.")


if __name__ == "__main__":
    load_dotenv()
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    bigbrotr = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )
    generate_relay_synchronization_csv(DATA_FOLDER, bigbrotr)
    generate_events_csv(DATA_FOLDER, bigbrotr)
    generate_relays_events_csv(DATA_FOLDER, bigbrotr)
    generate_relay_event_count_csv(DATA_FOLDER, bigbrotr)
    bigbrotr.close()
