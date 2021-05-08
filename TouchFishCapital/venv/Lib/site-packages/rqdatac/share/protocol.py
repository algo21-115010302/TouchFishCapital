# -*- coding: utf-8 -*-

"""
    2 bytes    |1 byte |1 byte |            4 bytes            |
----------------------------------------------------------------
|   Msg Type   |   ST  |   CM  |           Body Length         |
----------------------------------------------------------------

Msg Type: 消息类型
ST: Serialization Type, 序列化方式
CM: Compression Method, 压缩方式
Body Length: 包体长度，不包含头

little-endian
"""


def with_metaclass(mcls):
    def decorator(cls):
        body = vars(cls).copy()
        # clean out class body
        body.pop("__dict__", None)
        body.pop("__weakref__", None)
        return mcls(cls.__name__, cls.__bases__, body)

    return decorator


class SimpleEnum(type):
    def __new__(mcs, name, bases, attrs):
        mapping = {v: k for k, v in attrs.items() if not k.startswith("_")}
        attrs["__map"] = mapping
        attrs["__key"] = frozenset(mapping.keys())
        return type.__new__(mcs, name, bases, attrs)

    def __getitem__(self, item):
        _map = self.__dict__["__map"]
        if item not in self.__dict__["__key"]:
            return "unknown %s(%s)" % (self.__name__, item)
        return _map[item]

    def __contains__(self, item):
        return item in self.__dict__["__key"]


@with_metaclass(SimpleEnum)
class MSG_TYPE:
    HANDSHAKE = 0
    REQUEST = 1
    RESPONSE = 2
    ERROR = 3
    STREAM_START = 4
    STREAM_END = 5
    FEED = 6
    TABLE = 7
    TABLE2 = 8


@with_metaclass(SimpleEnum)
class SERIALIZATION_TYPE:
    RAW = 0
    JSON = 1
    MSGPACK = 2


@with_metaclass(SimpleEnum)
class COMPRESSION_METHOD:
    NONE = 0
    SNAPPY = 1
    ZLIB = 2
    BROTLI = 3


HEADER_FORMAT = "<HBBI"
HEADER_LENGTH = 8
