import hashlib
from ..config.custom_logging import CustomLogger
from functools import wraps

logger = CustomLogger("CacheManager")
_cache = {}


def clear():
    logger.info("Clearing the cache")
    keys = len(_cache)
    _cache.clear()
    logger.info("Cleared {} items".format(keys))


def cache(fun):
    @wraps(fun)
    def wrapper(*args, **kwargs):
        key_source = fun.__name__

        if args:
            key_source += str(args)

        if kwargs:
            key_source += str(kwargs)

        key = hashlib.md5(key_source.encode("utf-8")).hexdigest()

        if not key in _cache:
            _cache[key] = fun(*args, **kwargs)
            logger.debug("{} result saved in cache".format(key_source))
        else:
            logger.debug("{} result read from cache".format(key_source))
        return _cache[key]

    return wrapper
