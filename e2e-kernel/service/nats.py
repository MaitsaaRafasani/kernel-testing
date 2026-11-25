"""NATS JetStream client, same connection setup as kernel v2 (service/nats.py). Publish only."""
import nats


class NatsClient:
    def __init__(self, nats_config: dict):
        self.server_url = f"nats://{nats_config['host']}:{nats_config['port']}"
        self.nc = None
        self.js = None

    async def connect(self) -> None:
        # same options as kernel v2 NatsService.connect
        self.nc = await nats.connect(
            servers=self.server_url,
            max_reconnect_attempts=-1,
            name="e2e-kernel",
            reconnect_time_wait=2,
            ping_interval=20,
            max_outstanding_pings=2,
        )
        self.js = self.nc.jetstream()

    async def publish(self, subject: str, payload: str, stream: str) -> None:
        """JetStream publish, waits for the stream ack (data stored)."""
        await self.js.publish(subject, payload.encode(), stream=stream, timeout=10)

    async def stream_exists(self, stream: str) -> bool:
        try:
            await self.js.stream_info(stream)
            return True
        except nats.js.errors.NotFoundError:
            return False

    async def close(self) -> None:
        if self.nc is not None:
            await self.nc.drain()
            self.nc = None
