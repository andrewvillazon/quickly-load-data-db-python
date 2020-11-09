import pyodbc
from pypika import Column, Query, Table


def execute_query(q, conn_params):
    connection = pyodbc.connect(**conn_params)
    cursor = connection.cursor()

    cursor.execute(q)
    connection.commit()

    connection.close()


def make_table(table_def, conn_params):
    table = Table(table_def["name"])
    cols = [Column(k, v) for k, v, in table_def["columns"].items()]

    drop = Query.drop_table(table).if_exists()
    create = Query.create_table(table).columns(*cols)

    execute_query(str(drop) + "\n" + str(create), conn_params)


def multi_row_insert(batch, table_name, conn_params):
    row_expressions = []

    for _ in range(batch.qsize()):
        row_data = tuple(batch.get())
        row_expressions.append(row_data)

    table = Table(table_name)
    insert_into = Query.into(table).insert(*row_expressions)

    execute_query(str(insert_into), conn_params)
