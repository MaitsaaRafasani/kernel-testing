"""Check connections before run: NATS (+ stream) and Postgres."""
from harness.report import console
from service.db import Database
from service.nats import NatsClient


async def ping(config: dict) -> int:
    """Return exit code (0 = all OK)."""
    failed = 0

    client = NatsClient(config["nats"])
    try:
        await client.connect()
        stream = config["nats"]["stream"]
        if await client.stream_exists(stream):
            console.info(f"NATS OK {client.server_url}, stream '{stream}' ada")
        else:
            failed += 1
            console.fail(f"NATS {client.server_url}: stream '{stream}' tidak ada")
    except Exception as error:
        failed += 1
        console.fail(f"NATS {client.server_url}: {error}")
    finally:
        await client.close()

    db_config = config["db"]
    target = f"{db_config['host']}:{db_config['port']}/{db_config['name']} schema {db_config['schema']}"
    database = Database(db_config)
    try:
        await database.connect()
        row = await database.get_one("SELECT current_schema() AS schema")
        console.info(f"DB OK {target}, current_schema={row['schema']}")
    except Exception as error:
        failed += 1
        console.fail(f"DB {target}: {error}")
    finally:
        await database.close()

    return 1 if failed else 0
