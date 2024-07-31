
#!/usr/bin/env python3

import configparser
import os
import ydb
import datetime


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_history(test_names_array, last_n_runs_of_test_amount, build_type):
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        #os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]="/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json"
       return {}
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    query = f"""
        $date_format = DateTime::Format("%Y-%m-%d");
        $test= (
        SELECT 
                test_name,suite_folder,build_type ,run_timestamp,status,
                $date_format(run_timestamp) as date
        
                    
                    FROM 
                        `test_results/test_runs_results`
                    where (job_name ='Nightly-run' or job_name like 'Postcommit%') --and
        
                    GROUP by  test_name,suite_folder,build_type,status,run_timestamp
        );
        select test_name,suite_folder,build_type,status ,date, max(run_timestamp)

        from $test
        GROUP by  test_name,suite_folder,status,build_type,date
            
    """

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )

        with session.transaction() as transaction:
            prepared_query = session.prepare(query)

            result_set = session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query, commit_tx=True
            )

            results = {}
            for row in result_set[0].rows:
                if not row["full_name"].decode("utf-8") in results:
                    results[row["full_name"].decode("utf-8")] = {}

                results[row["full_name"].decode("utf-8")][row["run_timestamp"]] = {
                    "status": row["status"],
                    "commit": row["commit"],
                    "datetime": datetime.datetime.fromtimestamp(int(row["run_timestamp"] / 1000000)).strftime("%H:%m %B %d %Y"),
                    "count_of_passed": row["count_of_passed"],
                }
            return results


if __name__ == "__main__":
    get_history()
