import eel
import psycopg2
from jinja2 import Environment, FileSystemLoader


class Connection:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="finance_project",
            user="postgres",
            password="Erfnhd123890")
        self.conn.autocommit = True


files = FileSystemLoader('templates')
environment = Environment(loader=files)
template = environment.from_string("Hello, {{name}}")
template.render(name="World")


def main():
    eel.init("public")
    eel.start("template.html")


if __name__ == "__main__":
    main()
