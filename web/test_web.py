import eel
import psycopg2
from psycopg2 import sql


def query():
    conn = psycopg2.connect(host='127.0.0.1', port=5432,
                            user='postgres', password='Erfnhd123890', dbname="finance_project")
    raw = sql.SQL("SELECT {} FROM {}").format(sql.Identifier(
        "table", "field"), sql.Identifier("public", "table"))
    print(raw)


def connect():
    conn = psycopg2.connect(host='127.0.0.1', port=5432,
                            user='postgres', password='Erfnhd123890', dbname="finance_project")
    cursor = conn.cursor(scrollable=True, name="ecl_query")
    cursor.execute(
        "select * from public.bv_ecl_process_det13_20231031 where account_no = '1471000023';")

    # cursor.scrollable = True
    print(cursor.scroll(100*100))

    lobject = conn.lobject(mode='r')

    lobject.read()


query()

eel.init('web/public')

eel.start('base.html')
