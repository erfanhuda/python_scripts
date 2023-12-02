import psycopg2


class Connection:
    def __init__(self):
        self.status = None
        self.cursor = None
        self.open_connection()

    def open_connection(self):
        conn = psycopg2.connect(
            host="localhost",
            database="finance_project",
            user="postgres",
            password="erfan123")
        conn.autocommit = True

        if conn.autocommit:
            self.status = True
            self.cursor = conn.cursor()


conn = Connection()
cur = conn.cursor
cur.execute("SELECT * FROM public.bv_update_ecl_bucket_cif_lfam;")
result = cur.fetchone()
print(result)
