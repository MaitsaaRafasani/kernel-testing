"""Capture NATS messages without taking them from kernel (core subscribe / ephemeral consumer)."""


class NatsTap:
    async def start(self, subjects: list[str]) -> None:
        raise NotImplementedError

    def messages_since(self, subject: str, since) -> list[dict]:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError
