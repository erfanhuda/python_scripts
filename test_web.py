import eel
import psycopg2
import psycopg2.sql as sql
import random
import json
from decimal import Decimal as D


class BaseEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, D):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


def connect_pg():
    conn = psycopg2.connect(
        host="localhost",
        database="finance_project",
        user="postgres",
        password="Erfnhd123890")
    conn.autocommit = True

    return conn


def get_all_data(sql):
    conn = connect_pg()
    cur = conn.cursor()
    cur.execute(sql)

    return cur.fetchall()


def convert_to_json(data):
    js_data = json.dumps(data, use_decimal=True)
    return js_data


pd_mapping = sql.SQL("select * from public.bv_ecl_mapping_pd")
lgd_mapping = sql.SQL("select * from public.bv_ecl_mapping_lgd")

get_json = BaseEncoder()

eel.init('web')                     # Give folder containing web files


@eel.expose
def py_random():
    return random.random()


@eel.expose                         # Expose this function to Javascript
def say_hello_py(x):
    print('Hello from %s' % x)


@eel.expose
def get_json_data(data):
    data = get_json.encode(data)
    return data


say_hello_py('Python World!')
eel.say_hello_js('Python World!')   # Call a Javascript function

eel.start('templates/base.html', size=(300, 200))    # Start
