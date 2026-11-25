"""Render versions/<ver>/gate_format.yaml and publish to NATS JetStream, like the IoT gateway.

Gate messages: data (device packet), connect, disconnect, xmit.
"""
import json
from datetime import datetime, timezone
from string import Template

from service.nats import NatsClient


def gate_time(at: datetime | None = None) -> str:
    """JS Date ISO string, like the gateway: 2026-11-18T03:00:00.123Z."""
    at = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return at.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def render_value(template, values: dict):
    """Render one template value (see gate_format.yaml header)."""
    if isinstance(template, dict):
        if set(template) == {"json"}:
            return json.dumps(render_value(template["json"], values), separators=(",", ":"))
        return {key: render_value(value, values) for key, value in template.items()}
    if isinstance(template, str):
        name = template[1:]
        if template.startswith("$") and name.isidentifier():
            if name not in values:
                raise KeyError(f"gate format needs value '{name}'")
            return values[name]
        return Template(template).substitute(values)
    return template


class Injector:
    def __init__(self, nats_config: dict, gate_format: dict, client: NatsClient | None = None):
        """client None = dry run (render only)."""
        self.nats_config = nats_config
        self.gate_format = gate_format
        self.client = client

    def render(self, message: str, values: dict) -> tuple[str, str, str]:
        """Return (stream, subject, json body)."""
        spec = self.gate_format["messages"][message]
        base = {"tube": self.nats_config["tube"], "stream": self.nats_config["stream"]}
        if message == "data":
            base.update((self.gate_format.get("by_family") or {}).get(values.get("family"), {}))
        values = {**base, **values}
        stream = render_value(spec["stream"], values)
        subject = render_value(spec["subject"], values)
        body = render_value(spec["body"], values)
        return stream, subject, json.dumps(body, separators=(",", ":"))     # compact, like JSON.stringify

    async def send(self, message: str, values: dict) -> tuple[str, str]:
        """Publish one gate message (render only on dry run). Return (subject, body)."""
        stream, subject, body = self.render(message, values)
        if self.client is not None:
            await self.client.publish(subject, body, stream)
        return subject, body
