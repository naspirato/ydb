#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import posixpath
import threading
import time
import ydb
from collections import Counter

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

# Maximum number of threads to use at any given time
MAX_THREADS = 10

def create_tables(pool, table_path):
    print(f"> create table: {table_path}")

    def callee(session):
        session.execute_scheme(f"""
            CREATE table IF NOT EXISTS`{table_path}` (
                branch Utf8 NOT NULL,
                build_type Utf8 NOT NULL,
                commit Utf8 NOT NULL,
                duration Double,
                job_id Uint64,
                job_name Utf8,
                log Utf8,
                logsdir Utf8,
                owners Utf8,
                pull Utf8,
                run_timestamp Timestamp NOT NULL,
                status_description Utf8,
                status Utf8 NOT NULL,
                stderr Utf8,
                stdout Utf8,
                suite_folder Utf8 NOT NULL,
                test_id Utf8 NOT NULL,
                test_name Utf8 NOT NULL,
                PRIMARY KEY (`test_name`, `suite_folder`,build_type, status, run_timestamp)
            )
                PARTITION BY HASH(`test_name`, `suite_folder`, branch, build_type )
                WITH (STORE = COLUMN)
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
        .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("log", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("logsdir", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stderr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stdout", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def process_timestamp(driver, table_client, ts):
    query_get_runs = f"""
    select * from `test_results/test_runs_column`
    where run_timestamp = cast({ts['run_timestamp']} as Timestamp)
    """
    query = ydb.ScanQuery(query_get_runs, {})

    start_time = time.time()
    it = driver.table_client.scan_query(query)

    results = []
    prepared_for_update_rows = []
    while True:
        try:
            result = next(it)
            results = results + result.result_set.rows
        except StopIteration:
            break

    end_time = time.time()
    data_timestamp = datetime.datetime.utcfromtimestamp(ts["run_timestamp"]/1000000 ).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f'Transaction duration for timestamp {data_timestamp}: {end_time - start_time}')

    print(f'Runs data captured, {len(results)} rows')
    for row in results:
        prepared_for_update_rows.append({
            'branch': row['branch'],
            'build_type': row['build_type'],
            'commit': row['commit'],
            'duration': row['duration'],
            'job_id': row['job_id'],
            'job_name': row['job_name'],
            'log': row['log'],
            'logsdir': row['logsdir'],
            'owners': row['owners'],
            'pull': row['pull'],
            'run_timestamp': row['run_timestamp'],
            'status_description': row['status_description'],
            'status': row['status'],
            'stderr': row['stderr'],
            'stdout': row['stdout'],
            'suite_folder': row['suite_folder'],
            'test_id': row['test_id'],
            'test_name': row['test_name'],
            'full_name': row['suite_folder'] + '/' + row['test_name']
        })

    print('Upserting runs for timestamp:', ts["run_timestamp"])
    with ydb.SessionPool(driver) as pool:
        full_path = posixpath.join(DATABASE_PATH, 'test_results/test_runs')
        bulk_upsert(driver.table_client, full_path, prepared_for_update_rows)

    print('History updated for timestamp:', ts["run_timestamp"])


def main():
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return 1
    else:
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )

        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)

        with ydb.SessionPool(driver) as pool:
            create_tables(pool, 'test_results/test_runs')

        # Get last timestamp from runs column
        default_start_date = datetime.datetime(2024, 8, 20)
        last_datetime = default_start_date
            
        last_date = default_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f'Last run_datetime in table: {last_date}')

        # Get timestamp list from runs
        last_date_query = f"""select distinct run_timestamp from `test_results/test_runs_column`
        where run_timestamp >=Timestamp('{last_date}')"""
        query = ydb.ScanQuery(last_date_query, {})
        it = table_client.scan_query(query)
        timestamps = []

        start_time = time.time()
        while True:
            try:
                result = next(it)
                timestamps = timestamps + result.result_set.rows
            except StopIteration:
                break

        end_time = time.time()
        print(f"Transaction 'geting timestamp list from runs' duration: {end_time - start_time}")
        print(f'Count of timestamps: {len(timestamps)}')

        # Create and start threads
        threads = []
        active_threads = 0
        for ts in timestamps:
            while active_threads >= MAX_THREADS:
                # Wait for a thread to finish
                for thread in threads:
                    if not thread.is_alive():
                        threads.remove(thread)
                        active_threads -= 1
                        break
                time.sleep(0.1)

            thread = threading.Thread(target=process_timestamp, args=(driver, table_client, ts))
            threads.append(thread)
            thread.start()
            active_threads += 1

        # Wait for threads to complete
        for thread in threads:
            thread.join()

        print("All threads completed.")


if __name__ == "__main__":
    main()