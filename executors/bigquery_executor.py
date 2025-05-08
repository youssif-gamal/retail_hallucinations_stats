import subprocess
import json
import os
import pandas as pd


class BigQueryExecutor:
    def execute_query(self, query_object, cache=True):
        INF = 1000000
        csv_filename = f"./queries_cache/{query_object.query_hash}.csv"
        if os.path.exists(csv_filename) and cache:
            return pd.read_csv(csv_filename)

        cmd = [
            "bq",
            "query",
            f"--project_id={query_object.project_id}",
            "--format=json",
            "--nouse_legacy_sql",
            f"--max_rows={INF}",  # Added this line to return all rows
            f"{query_object.query}",
        ]

        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=None,
            )
            query_results = json.loads(result.stdout)
            print(query_results)
            df = pd.DataFrame(query_results)

            if cache and not df.empty:
                df.to_csv(csv_filename, index=False)
                print(f"Query results cached to {csv_filename}")
            return df

        except subprocess.CalledProcessError as e:
            print("Query failed:", e)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"Raw output: {result.stdout}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
