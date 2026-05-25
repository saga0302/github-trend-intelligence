from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import gzip
import json

default_args = {
    'owner': 'saga',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def download_gh_archive(**context):
    """
    Download one hour of GitHub Archive data.
    Runs at :05 past the hour, pulls the previous hour's file.
    data_interval_start - 1 hour = freshest complete file guaranteed.
    """
    target = context['data_interval_start'] - timedelta(hours=1)

    year  = target.year
    month = target.month
    day   = target.day
    hour  = target.hour

    filename = f"{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    url = f"https://data.gharchive.org/{filename}"
    local_path = f"/tmp/{filename}"

    print(f"=== Downloading GitHub Archive file ===")
    print(f"DAG interval:  {context['data_interval_start']}")
    print(f"Fetching:      {target} (1 hour behind = freshest complete file)")
    print(f"URL:           {url}")
    print(f"Saving to:     {local_path}")

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total_bytes = 0
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            total_bytes += len(chunk)

    size_mb = total_bytes / (1024 * 1024)
    print(f"Downloaded:    {size_mb:.1f} MB")
    print(f"File saved:    {local_path}")

    context['ti'].xcom_push(key='gh_filename', value=local_path)
    context['ti'].xcom_push(key='target_hour', value=str(target))
    return local_path


def count_events(**context):
    """
    Read the downloaded file and count events by type.
    Pulls filename from XCom — set by download_gh_archive.
    """
    local_path = context['ti'].xcom_pull(
        task_ids='download_gh_archive',
        key='gh_filename'
    )

    print(f"=== Counting events in: {local_path} ===")

    event_counts = {}
    total_events = 0

    with gzip.open(local_path, 'rt', encoding='utf-8') as f:
        for line in f:
            event = json.loads(line)
            etype = event.get('type', 'Unknown')
            event_counts[etype] = event_counts.get(etype, 0) + 1
            total_events += 1

    print(f"\nTotal events: {total_events:,}")
    print(f"\nEvent breakdown:")
    for etype, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        pct = count / total_events * 100
        print(f"  {etype:<30} {count:>6,}  ({pct:.1f}%)")

    # Find top starred repo this hour
    star_counts = {}
    with gzip.open(local_path, 'rt', encoding='utf-8') as f:
        for line in f:
            event = json.loads(line)
            if event.get('type') == 'WatchEvent':
                repo = event['repo']['name']
                star_counts[repo] = star_counts.get(repo, 0) + 1

    if star_counts:
        top_repo = max(star_counts, key=star_counts.get)
        print(f"\nTop starred repo this hour: {top_repo} ({star_counts[top_repo]} stars)")

    context['ti'].xcom_push(key='total_events', value=total_events)
    context['ti'].xcom_push(key='event_counts', value=event_counts)


def summarize(**context):
    """Final task — clean summary, ready for Bronze ingestion."""
    total  = context['ti'].xcom_pull(task_ids='count_events', key='total_events')
    counts = context['ti'].xcom_pull(task_ids='count_events', key='event_counts')
    target = context['ti'].xcom_pull(task_ids='download_gh_archive', key='target_hour')

    watch = counts.get('WatchEvent', 0)
    push  = counts.get('PushEvent', 0)
    fork  = counts.get('ForkEvent', 0)
    pr    = counts.get('PullRequestEvent', 0)

    print(f"=== PIPELINE SUMMARY ===")
    print(f"Hour processed: {target}")
    print(f"Total events:   {total:,}")
    print(f"Stars:          {watch:,}")
    print(f"Pushes:         {push:,}")
    print(f"Forks:          {fork:,}")
    print(f"Pull requests:  {pr:,}")
    print(f"Star rate:      {watch/total*100:.1f}% of all events")
    print(f"========================")
    print(f"Week 1 complete. Ready for Bronze layer ingestion in Week 2.")


with DAG(
    dag_id='github_archive_download',
    default_args=default_args,
    description='Download and explore GitHub Archive hourly files',
    start_date=datetime(2025, 4, 19, 3, 0, 0),
    schedule_interval='5 * * * *',
    catchup=False,
    tags=['github', 'week1', 'pipeline'],
) as dag:

    t1 = PythonOperator(
        task_id='download_gh_archive',
        python_callable=download_gh_archive,
    )

    t2 = PythonOperator(
        task_id='count_events',
        python_callable=count_events,
    )

    t3 = PythonOperator(
        task_id='summarize',
        python_callable=summarize,
    )

    t1 >> t2 >> t3