import psycopg2
from decouple import config


class DB:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=config('DB_HOST', default='localhost'),
            database=config('DB_NAME', default='localhost'),
            user=config('DB_USER', default='localhost'),
            password=config('DB_PASS', default='localhost'),
            port=config('DB_PORT', default=5432)
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def fetch_one(self, query, params=None):
        self.cursor.execute(query, params or [])
        return self.cursor.fetchone()

    def fetch_one_dict(self, query, params=None):
        self.cursor.execute(query, params or [])
        row = self.cursor.fetchone()
        if row is None:
            return None
        colnames = [desc[0] for desc in self.cursor.description]
        return dict(zip(colnames, row))

    def save(self, query, params=None):
        self.cursor.execute(query, params or [])
        return self.cursor.rowcount

    def close(self):
        self.cursor.close()
        self.conn.close()
