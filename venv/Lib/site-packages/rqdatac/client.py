# -*- coding: utf-8 -*-
import os
import re
import socket
import warnings
import logging
from functools import partial, wraps

from six.moves.urllib.parse import urlparse
from six import string_types
from sys import platform
import rqdatac

URI_PATTERN = r"^(?P<schema>tcp|rqdatac?)://(?P<username>\S+?):(?P<password>\S+)@(?P<hostname>\S+):(?P<port>\d+$)"
URI_PATTERN = re.compile(URI_PATTERN)

if getattr(os, "register_at_fork", None):
    def _close_after_fork():
        global _CLIENT
        if _CLIENT is not _DUMMY and _CLIENT.PID != os.getpid():
            reset()


    os.register_at_fork(after_in_child=_close_after_fork)


class DummyClient:
    PID = -1

    def execute(self, *args, **kwargs):
        raise RuntimeError("rqdatac is not initialized")

    def reset(self):
        pass

    def info(self):
        print('rqdatac is not initialized')

    def close(self):
        pass

    execute_with_timeout = execute


_DUMMY = DummyClient()

_CLIENT = _DUMMY

_PLUGINS_IMPORTED = False


def reset():
    """reset all connections. this function is helpful when you fork from an connected client."""
    _CLIENT.reset()
    _CLIENT.PID = os.getpid()


def initialized():
    """
    is rqdatac already initialized ?

    :return: Bool
    """
    global _CLIENT, _DUMMY
    return _CLIENT is not _DUMMY


def set_sock_opts(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        s = func(*args, **kwargs)
        s.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        try:
            if platform.startswith('linux'):
                # (unit:s)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5 * 60)
            elif platform.startswith('darwin'):  # macos
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
            elif platform.startswith('win'):  # windows
                # (unit:ms) since windows2000
                s.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 5 * 60 * 1000, 60 * 1000))
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 20)
        except:
            pass
        return s

    return wrap


def _parse_proxy_url(proxy_url):
    if not proxy_url:
        return

    proxy = urlparse(proxy_url)
    return proxy.scheme, proxy.hostname or proxy.path, proxy.port, proxy.username, proxy.password


def _set_sock_factory(proxy_info):
    from rqdatac.connection import Connection
    if proxy_info is None:
        conn = socket.create_connection
    else:
        try:
            import socks
        except ImportError:
            raise RuntimeError("PySocks is required when use proxy. You can install it using `pip install PySocks` or "
                               "`pip install rqdatac[proxy]`")

        if not isinstance(proxy_info, tuple) or len(proxy_info) != 5:
            raise ValueError("expected a tuple like (proxy_type, host, port, user, password)")

        proxy_type, host, port, user, password = proxy_info
        if proxy_type.upper() == "HTTP":
            proxy_type = socks.PROXY_TYPE_HTTP
        elif proxy_type.upper() == "SOCKS4":
            proxy_type = socks.PROXY_TYPE_SOCKS4
        elif proxy_type.upper() == "SOCKS5":
            proxy_type = socks.PROXY_TYPE_SOCKS5
        else:
            raise ValueError("proxy type {} not supported yet. http, socks4 and socks5 proxy are supported".format(proxy_type))
        conn = partial(socks.create_connection, proxy_type=proxy_type, proxy_addr=host, proxy_port=port,
                       proxy_username=user, proxy_password=password)

    Connection.set_sock_factory(set_sock_opts(conn))


def init(username=None, password=None, addr=("rqdatad-pro.ricequant.com", 16011), *_, **kwargs):
    """initialize rqdatac.

    rqdatac connection is thread safe but not fork safe. Every thread have their own connection by
    default. you can set param 'use_pool' to True to use a connection pool instead.

    NOTE: if you are using rqdatac with python < 3.7 in a multi-process program, remember to call
    reset in child process.

    Environment Variable:
    RQDATAC_CONF / RQDATAC2_CONF: When init called with no argument, the value in RQDATAC2_CONF is used as uri, then RQDATAC_CONF.
    RQDATAC_PROXY: proxy info, e.g. http://username:password@host:port

    :param username: string
    :param password: string
    :param addr: ('127.0.0.1', 80) or '127.0.0.1:80'

    :keyword uri: keyword only. a uri like rqdata://username:password@host:port or tcp://username:password@host:port
    :keyword connect_timeout: socket connect connect timeout, default is 5 sec.
    :keyword timeout: socket time out, default is 60 sec.
    :keyword lazy: True by default, means "do not connect to server immediately".
    :keyword use_pool: use connection pool. default is False
    :keyword max_pool_size: max pool size, default is 8
    :keyword proxy_info: a tuple like (proxy_type, host, port, user, password) if use proxy, default is None
    :keyword auto_load_plugins: boolean, enable or disable auto load plugin, default to True.
    """
    extra_args = {k: kwargs.pop(k) for k in ('timeout', 'connect_timeout') if k in kwargs}
    proxy_info = kwargs.pop('proxy_info', None) or _parse_proxy_url(os.environ.get('RQDATAC_PROXY'))
    _set_sock_factory(proxy_info)

    logging.getLogger("rqdata").disabled = not kwargs.pop('debug', False)

    uri = kwargs.pop("uri", None)
    if not (username or password or uri):
        uri = os.environ.get("RQDATAC2_CONF") or os.environ.get("RQDATAC_CONF")
    if username and password and addr:
        addr = parse_address(addr)
    elif uri:
        r = URI_PATTERN.match(uri)
        if r is not None:
            username = username or r.group('username')
            password = password or r.group('password')
            addr = parse_address((r.group("hostname"), r.group("port")))
        else:
            raise ValueError("uri invalid")
    else:
        err = ValueError("username/password/addr or uri expected")
        if not username:
            raise err
        r = URI_PATTERN.match(username)
        # pass uri as first argument.
        if not r:
            raise err
        username = r.group('username')
        password = r.group('password')
        addr = parse_address((r.group("hostname"), r.group("port")))
        if not (username and password):
            raise err

    global _CLIENT
    if _CLIENT is not _DUMMY:
        warnings.warn("rqdatac is already inited. Settings will be changed.", stacklevel=0)
        reset()

    global _PLUGINS_IMPORTED
    if not _PLUGINS_IMPORTED:
        if kwargs.get("auto_load_plugins", True):
            _auto_import_plugin()
            _PLUGINS_IMPORTED = True

    auth_info = {'username': username, 'password': password, 'ver': rqdatac.__version__}
    if kwargs.pop("use_pool", False):
        from .connection_pool import ConnectionPool
        max_pool_size = kwargs.pop("max_pool_size", 8)
        _CLIENT = ConnectionPool(addr, auth=auth_info, max_pool_size=max_pool_size, **extra_args)
    else:
        from .thread_local import ThreadLocalConnection
        _CLIENT = ThreadLocalConnection(addr, auth=auth_info, **extra_args)

    _CLIENT.PID = os.getpid()

    def show_info():
        print('rqdatac version:', rqdatac.__version__)
        print('server address: {}:{}'.format(addr[0], addr[1]))
        if username == 'license':
            print('You are using license:\r\n{}'.format(password))
        elif username.startswith('rqpro.'):
            print('You are using a RQPro account: {}'.format(username.split('.', 1)[1]))
        elif username == 'sid':
            print('You are using your RQPro account')
        else:
            print('You are using account: {}'.format(username))

    _CLIENT.info = show_info

    if username == "license":
        quota = get_client().execute("user.get_quota")
        remaining_days = quota["remaining_days"]
        is_trial = quota["license_type"] == "TRIAL"
        if is_trial or 0 <= remaining_days <= 14:
            warnings.warn("Your account will be expired after  {} days. "
                          "Please call us at 0755-22676337 to upgrade or purchase or "
                          "renew your contract.".format(remaining_days))
    elif not kwargs.get("lazy", True):
        get_client().execute("get_all_trading_dates")


def get_client():
    if _CLIENT.PID != os.getpid():
        reset()
    return _CLIENT


def parse_address(address):
    if isinstance(address, tuple):
        host, port = address
        return host, int(port)
    elif isinstance(address, string_types):
        if ":" not in address:
            return address
        host, port = address.rsplit(":", 1)
        return host, int(port)
    else:
        raise ValueError("address must be a str or tuple")


def _auto_import_plugin():
    from pkgutil import iter_modules
    from importlib import import_module
    plugin_module_names = []
    for m in iter_modules():
        _, name, is_pkg = m
        if not is_pkg:
            continue
        if name.startswith('rqdatac_'):
            plugin_module_names.append(name)

    plugin_module_names.sort()
    for name in plugin_module_names:
        logging.info('loading plugin {}'.format(name))
        import_module(name)
