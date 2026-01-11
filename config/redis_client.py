from typing import Optional
from loguru import logger

from .settings import settings

try:
    import redis  # type: ignore
except Exception:
    redis = None

_pool = None
_client = None


def get_redis_client() -> Optional["redis.Redis"]:
    global _pool, _client
    if not settings.enable_redis_cache:
        return None
    if redis is None:
        return None
    url = settings.redis_url
    if not url:
        return None
    if _client is not None:
        return _client
    try:
        _pool = redis.ConnectionPool.from_url(url, decode_responses=True, max_connections=20)
        _client = redis.Redis(connection_pool=_pool)
        _client.ping()
        logger.info("Redis connected")
        return _client
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        _pool = None
        _client = None
        return None


def redis_get(key: str) -> Optional[str]:
    c = get_redis_client()
    if c is None:
        return None
    try:
        return c.get(key)
    except Exception as e:
        logger.warning(f"Redis GET failed: {e}")
        return None


def redis_set(key: str, value: str, ex: Optional[int] = None) -> bool:
    c = get_redis_client()
    if c is None:
        return False
    try:
        c.set(name=key, value=value, ex=ex)
        return True
    except Exception as e:
        logger.warning(f"Redis SET failed: {e}")
        return False


def redis_delete(key: str) -> int:
    c = get_redis_client()
    if c is None:
        return 0
    try:
        return int(c.delete(key))
    except Exception as e:
        logger.warning(f"Redis DEL failed: {e}")
        return 0


def redis_scan_delete(pattern: str) -> int:
    c = get_redis_client()
    if c is None:
        return 0
    deleted = 0
    try:
        for k in c.scan_iter(match=pattern, count=1000):
            try:
                c.delete(k)
                deleted += 1
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Redis SCAN/DEL failed: {e}")
    return deleted
