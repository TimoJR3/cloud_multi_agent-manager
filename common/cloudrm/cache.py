from __future__ import annotations

from redis.asyncio import Redis


def create_redis(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


async def ping_redis(client: Redis) -> bool:
    return bool(await client.ping())
