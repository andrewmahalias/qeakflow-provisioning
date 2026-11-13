"""Infrastructure side of provisioning, behind a small interface.

Every operation is idempotent: calling it twice has the same effect as once.
That property is what makes retries and compensation safe.
"""
import asyncio
import time
from typing import Protocol

import docker
from docker.errors import NotFound


def container_name(tenant_id: str) -> str:
    return f"tenant-{tenant_id}"


class Provisioner(Protocol):
    async def ensure_running(self, tenant_id: str, port: int) -> str: ...

    async def wait_healthy(self, tenant_id: str) -> None: ...

    async def stop(self, tenant_id: str) -> None: ...

    async def remove(self, tenant_id: str) -> None: ...


class DockerProvisioner:
    def __init__(self, image: str, network: str | None, memory_limit: str, cpu_limit: float,
                 health_timeout: float = 60.0):
        self._client = docker.from_env()
        self._image = image
        self._network = network
        self._memory_limit = memory_limit
        self._cpu_limit = cpu_limit
        self._health_timeout = health_timeout

    # Docker SDK calls are blocking: run them in a thread so the event loop stays free.
    async def ensure_running(self, tenant_id: str, port: int) -> str:
        return await asyncio.to_thread(self._ensure_running, tenant_id, port)

    async def wait_healthy(self, tenant_id: str) -> None:
        await asyncio.to_thread(self._wait_healthy, tenant_id)

    async def stop(self, tenant_id: str) -> None:
        await asyncio.to_thread(self._stop, tenant_id)

    async def remove(self, tenant_id: str) -> None:
        await asyncio.to_thread(self._remove, tenant_id)

    def _ensure_running(self, tenant_id: str, port: int) -> str:
        name = container_name(tenant_id)
        try:
            existing = self._client.containers.get(name)
            if existing.status != "running":
                existing.start()  # e.g. unfreezing after payment
            return name
        except NotFound:
            pass

        self._client.containers.run(
            self._image,
            name=name,
            detach=True,
            labels={"app": "provisioning-demo", "tenant_id": tenant_id},
            network=self._network,
            ports={"80/tcp": port},
            mem_limit=self._memory_limit,  # enforced through cgroups
            nano_cpus=int(self._cpu_limit * 1_000_000_000),
            restart_policy={"Name": "unless-stopped"},
        )
        return name

    def _wait_healthy(self, tenant_id: str) -> None:
        """Poll until the container runs (and passes its HEALTHCHECK, if the image defines one).
        A real service would call its own health endpoint here."""
        container = self._client.containers.get(container_name(tenant_id))
        deadline = time.monotonic() + self._health_timeout
        while time.monotonic() < deadline:
            container.reload()
            health = container.attrs["State"].get("Health", {}).get("Status")
            if container.status == "running" and health in (None, "healthy"):
                return
            time.sleep(2)
        raise TimeoutError(f"{container_name(tenant_id)} did not become healthy")

    def _stop(self, tenant_id: str) -> None:
        try:
            self._client.containers.get(container_name(tenant_id)).stop()
        except NotFound:
            return

    def _remove(self, tenant_id: str) -> None:
        try:
            self._client.containers.get(container_name(tenant_id)).remove(v=True, force=True)
        except NotFound:
            return  # already gone


class FakeProvisioner:
    """In-memory stand-in for tests and for running the demo without Docker."""

    def __init__(self, fail_times: int = 0, healthy: bool = True):
        self.containers: dict[str, int] = {}  # tenant id -> port (includes stopped ones)
        self.stopped: set[str] = set()
        self.create_calls = 0
        self.healthy = healthy
        self._fail_times = fail_times

    async def ensure_running(self, tenant_id: str, port: int) -> str:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("simulated infrastructure failure")
        if tenant_id not in self.containers:
            self.create_calls += 1
            self.containers[tenant_id] = port
        self.stopped.discard(tenant_id)
        return container_name(tenant_id)

    async def wait_healthy(self, tenant_id: str) -> None:
        if not self.healthy:
            raise TimeoutError("instance did not become healthy")

    async def stop(self, tenant_id: str) -> None:
        if tenant_id in self.containers:
            self.stopped.add(tenant_id)

    async def remove(self, tenant_id: str) -> None:
        self.containers.pop(tenant_id, None)
        self.stopped.discard(tenant_id)
