from concurrent import futures
import csv
import queue

import sqlactions


MULTI_ROW_INSERT_LIMIT = 1000
WORKERS = 6


def read_csv(csv_file):
    with open(csv_file, encoding="utf-8", newline="") as in_file:
        reader = csv.reader(in_file, delimiter="|")
        next(reader)  # Header row

        for row in reader:
            yield row


def process_row(row, batch, table_name, conn_params):
    batch.put(row)

    if batch.full():
        sqlactions.multi_row_insert(batch, table_name, conn_params)

    return batch


def load_csv(csv_file, table_def, conn_params):
    # Optional, drops table if it exists before creating
    sqlactions.make_table(table_def, conn_params)

    batch = queue.Queue(MULTI_ROW_INSERT_LIMIT)

    with futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        todo = []

        for row in read_csv(csv_file):
            future = executor.submit(
                process_row, row, batch, table_def["name"], conn_params
            )
            todo.append(future)

        for future in futures.as_completed(todo):
            result = future.result()

    # Handle left overs
    if not result.empty():
        sqlactions.multi_row_insert(result, table_def["name"], conn_params)


if __name__ == "__main__":
    table_def = {
        "name": "dummy_data",
        "columns": {
            "id": "INTEGER",
            "job": "VARCHAR(100)",
            "company": "VARCHAR(100)",
            "name": "VARCHAR(100)",
            "sex": "CHAR",
            "mail": "VARCHAR(100)",
            "birthdate": "DATE",
        },
    }

    conn_params = {
        "server": "localhost",
        "database": "TutorialDB",
        "user": "yourUserName",
        "tds_version": "7.4",
        "password": "yourStrong(!)Password",
        "port": 1433,
        "driver": "FreeTDS",
    }

    load_csv("dummy_data.csv", table_def, conn_params)
