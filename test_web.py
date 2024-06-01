import eel
import psycopg2
import psycopg2.sql as sql
import random
import json
from decimal import Decimal as D
import urllib3
from io import BytesIO
import http.client


class ByteIOSocket:
    def __init__(self, data):
        self.handle = BytesIO(data)

    def makefile(self, mode):
        return self.handle

def response_from_byte(data):
    sock = ByteIOSocket(data)
    response = http.client.HTTPResponse(sock)

    return response

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

import socket

if __name__ == "__main__":
    import json

    conn = http.client.HTTPConnection('10.23.168.74', 8080)

    conn.request('GET', '/')

    response = conn.getresponse()
    print(response)