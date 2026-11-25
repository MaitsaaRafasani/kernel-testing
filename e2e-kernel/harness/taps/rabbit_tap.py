"""Capture RabbitMQ messages using an exclusive queue bound to the exchange."""


class RabbitTap:
    async def start(self, bindings: list[dict]) -> None:
        raise NotImplementedError

    def messages_since(self, exchange: str, since) -> list[dict]:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError
