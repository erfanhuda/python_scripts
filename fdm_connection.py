import sys
from typing import Protocol
from dataclasses import dataclass
import requests
import mysql.connector

# MySQL server configuration
dev_config = {
    'user': 'dev_erfan',
    'password': 'G#e6HtXEJKM',
    'host': '10.162.34.96',
    'port': '6608',
    'database': 'sbid_fin_dm_rep'
}

production_config = {
    'user': 'dev_erfan',
    'password': 'G#e6HtXEJKM',
    'host': '10.162.40.159',
    'port': '10001',
    'database': ''
}

test_config = {
    'user': 'dev_erfan',
    'password': 'G#e6HtXEJKM',
    'host': '10.162.36.231',
    'port': '6608',
    'database': ''
}


def test_connection(query):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(**dev_config)

        if connection.is_connected():
            print('Connected to FDM Dev server')
            cursor = connection.cursor()
            cursor.execute(
                f"{query}")

            return cursor.fetchall()
            # Perform further operations if needed

    except mysql.connector.Error as error:
        print(f'Failed to connect to MySQL server: {error}')

    finally:
        # Close the connection
        if 'connection' in locals():
            connection.close()
            print('Connection closed')


def upload_file(filename: str, tablename: str) -> None:
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(**dev_config)

        if connection.is_connected():
            print('Connected to FDM Dev server')
            cursor = connection.cursor()
            column = cursor.execute(
                f"""SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = '{tablename.split(".")[0]}' AND TABLE_NAME = '{tablename.split(".")[1]}';""")
            columns = ",".join([x[0] for x in column])
            cursor.execute(
                f"""LOAD DATA LOCAL INFILE '{filename}' INTO TABLE {tablename} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES ({columns});""")

            return cursor.fetchall()
            # Perform further operations if needed

    except mysql.connector.Error as error:
        print(f'Failed to connect to MySQL server: {error}')

    finally:
        # Close the connection
        if 'connection' in locals():
            connection.close()
            print('Connection closed')


if __name__ == '__main__':
    upload_file(
        filename="", tablename="sbid_fin_dm_stg.rep_fin_reg_com_master_kredit_ss_d")

    FILE_NAME = ''
    TABLE_NAME = ''
    if (FILE_NAME == '') or (TABLE_NAME == ''):
        upload_file(filename=sys.argv[1], tablename=sys.argv[2])
    else:
        upload_file(FILE_NAME, TABLE_NAME)
