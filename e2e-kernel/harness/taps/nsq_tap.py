"""Capture NSQ messages using an ephemeral channel (<name>#ephemeral)."""


class NsqTap:
    async def start(self, topics: list[str]) -> None:
        raise NotImplementedError

    def messages_since(self, topic: str, since) -> list[dict]:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError
