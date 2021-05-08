# -*- coding: utf-8 -*-
import logging
import time
import types
import warnings
from functools import wraps, partial, update_wrapper

import rqdatac
from rqdatac.share.errors import OverwriteWarning

log = logging.getLogger("rqdata")


def deprecated(func=None, msg=None):
    if func is None:
        return partial(deprecated, msg=msg)

    if msg is None:
        msg = func.__name__ + " is deprecated, and will be removed in future."

    @wraps(func)
    def wrap(*args, **kwargs):
        warnings.warn(msg, category=FutureWarning, stacklevel=0)
        return func(*args, **kwargs)

    return wrap


def export_as_api(f=None, name=None, namespace=None, priority=0):
    if f is None:
        return partial(export_as_api, name=name, namespace=namespace)
    name = name if name else f.__name__
    if namespace:
        if hasattr(rqdatac, namespace):
            namespace = getattr(rqdatac, namespace)
        else:
            namespace_name = namespace
            namespace = types.ModuleType(namespace_name)
            namespace.__file__ = 'rqdatac plugin'
            namespace.__module__ = "rqdatac"
            setattr(rqdatac, namespace_name, namespace)
            rqdatac.__all__.append(namespace_name)
    else:
        namespace = rqdatac
        rqdatac.__all__.append(name)

    old_f = getattr(namespace, name, None)
    if old_f is not None:
        if old_f.__priority > priority:
            warnings.warn("!!!! CAN'T OVERWRITE API {} WITH {} BECAUSE OLD PRIPORITY {} > {} !!!!".format(name, f, old_f.__priority, priority), category=OverwriteWarning)
            return f
        warnings.warn("!!!! OVERWRITE API {} WITH {} !!!!".format(name, f), category=OverwriteWarning)

    f.__priority = priority
    setattr(namespace, name, f)

    return f


def retry(count, suppress_exceptions, delay=1.0):
    def decorate(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            c = count
            while c > 0:
                try:
                    return func(*args, **kwargs)
                except suppress_exceptions as e:
                    c -= 1
                    if c == 0:
                        raise e
                    log.error("Retry because %r", e)
                    if delay:
                        time.sleep(delay)

        return wrap

    return decorate


def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen

    return primer


def ttl_cache(ttl):
    if not isinstance(ttl, int) or not ttl > 0:
        raise TypeError("Expected ttl to be a positive integer")

    def decorating_function(user_function):
        wrapper = _ttl_cache_wrapper(user_function, ttl)
        return update_wrapper(wrapper, user_function)

    return decorating_function


def _ttl_cache_wrapper(user_function, ttl):
    sentinel = object()
    cache = {}
    cache_get = cache.get  # bound method to lookup a key or return None

    def wrapper(*args, **kwargs):
        if kwargs:
            key = args + (repr(sorted(kwargs.items())),)
        else:
            key = args

        # in cpython, dict.get is thread-safe
        result = cache_get(key, sentinel)
        if result is not sentinel:
            expire_at, value = result
            if expire_at > time.time():
                return value
        value = user_function(*args, **kwargs)
        cache[key] = (time.time() + ttl, value)
        return value

    return wrapper


def compatible_with_parm(func=None, name=None, value=None, replace=None):
    if func is None:
        return partial(compatible_with_parm, name=name, value=value, replace=replace)

    @wraps(func)
    def wrap(*args, **kwargs):
        if name:
            if name in kwargs:
                msg = "'{}' is no longer used, please use '{}' instead ".format(name, replace)
                warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
                item = kwargs.pop(name)
                if item != value:
                    raise ValueError("'{}': except '{}', got '{}'".format(name, value, item))
        return func(*args, **kwargs)

    return wrap
