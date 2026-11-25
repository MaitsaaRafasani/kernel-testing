"""Postgres async client, same connection setup as kernel v2 (service/db/async_db.py).

psycopg3 + AsyncConnectionPool + dict rows + search_path schema.
One pool on the primary host (no read replica): E2E reads right after kernel writes,
a replica could lag and give false failures.
"""
from typing import Optional

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def build_conninfo(db_config: dict) -> str:
    """Same conninfo as kernel v2 _build_conninfo."""
    base = (
        f"host={db_config['host']} "
        f"port={db_config['port']} "
        f"user={db_config['user']} "
        f"password={db_config['password']} "
        f"dbname={db_config['name']} "
        f"sslmode=disable "
        f"keepalives=1 "
        f"keepalives_idle=500 "
        f"keepalives_interval=10 "
        f"keepalives_count=50"
    )
    if not db_config.get("use_rds_proxy"):
        base += f" options='-c search_path={db_config['schema']}'"
    return base


class Database:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.pool: Optional[AsyncConnectionPool] = None

    async def connect(self) -> None:
        schema = self.db_config["schema"]
        use_rds_proxy = self.db_config.get("use_rds_proxy")

        async def configure(conn):
            # RDS proxy does not accept `options`, set search_path per connection (like kernel v2)
            if use_rds_proxy:
                await conn.execute(f"SET search_path TO {schema}")

        self.pool = AsyncConnectionPool(
            conninfo=build_conninfo(self.db_config),
            min_size=1,
            max_size=int(self.db_config.get("max_conn", 5)),
            open=False,
            configure=configure,
            kwargs={"row_factory": dict_row, "autocommit": True},
        )
        await self.pool.open()
        await self.pool.wait(timeout=5.0)

    async def get_one(self, query: str, params: tuple | None = None) -> Optional[dict]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchone()

    async def get_all(self, query: str, params: tuple | None = None) -> list[dict]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchall()

    async def execute(self, query: str, params: tuple | None = None) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(query, params)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
