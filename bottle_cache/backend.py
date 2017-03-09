# -*- coding: utf-8 -*-
"""`bottle_cache.backend` module.

Provides package caching backend implementations.
"""

__author__ = 'Papavassiliou Vassilis'
__date__ = '23-1-2016'

try:
    import cPickle as pickle
except:
    import pickle
import zlib

from bottle_cache.bases import (BaseCacheBackend, CacheError)
from redis import StrictRedis
from redis.exceptions import RedisError


class RedisCacheBackend(BaseCacheBackend):
    """Redis backend Implementation
    """

    def __init__(self, backend_client=StrictRedis, key_tpl='{}', **conn_data):
        """Overriding 'BaseCache.__init__' method.
        """

        redis_backend = backend_client(**conn_data)

        super(RedisCacheBackend, self).__init__(backend_client=redis_backend, key_tpl=key_tpl)

    def get(self, key):
        """Implementing `BaseCache.get` method.
        """
        try:
            return self.backend.get(key)
        except RedisError as error:  # pragma: no cover
            raise CacheError(error.args)

    def set(self, key, value, ttl=None):
        """Implementing `BaseCache.set` method.
        """
        try:
            if ttl:
                self.backend.setex(key, ttl, value)
            else:
                self.backend.set(key, value)

            return self

        except RedisError as error:  # pragma: no cover
            raise CacheError(error.args)

    def remove(self, key):
        """Implementing `BaseCache.remove` method.
        """
        try:
            self.backend.delete(key)
            return self

        except RedisError as error:  # pragma: no cover
            raise CacheError(error.args)

    def clear(self):
        self.backend.flushall()
        return self


class CompressedRedisCacheBackend(RedisCacheBackend):
    """Redis backend with compression enabled
    """
    def __init__(self, **kwargs):
        self.compression_method = kwargs.pop("compression_method", zlib)
        super(CompressedRedisCacheBackend, self).__init__(**kwargs)

    def get(self, key):
        try:
            return str(
                pickle.loads(
                    self.compression_method.decompress(
                        super(CompressedRedisCacheBackend, self).get(key))))
        except TypeError as e:
            print(e)
            return None

    def set(self, key, value, ttl=None):
        return super(CompressedRedisCacheBackend, self).set(
            key, self.compression_method.compress(pickle.dumps(value)), ttl=ttl)
