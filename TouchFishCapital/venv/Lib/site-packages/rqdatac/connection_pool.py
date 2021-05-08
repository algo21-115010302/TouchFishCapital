# -*- coding: utf-8 -*-
import socket
from collections import deque
from threading import Lock, Semaphore
from contextlib import contextmanager

from rqdatac.share.errors import ErrorFromBackend, GatewayError
from rqdatac.decorators import retry
from rqdatac.connection import Connection
from rqdatac.utils import connection_error, timeout_error


class ConnectionPool:
    def __init__(self, addr, auth=None, max_pool_size=8, connect_timeout=3.0, timeout=5 * 60):
        self._addr = addr
        self._auth = auth if auth is not None else {}
        self._connect_timeout = connect_timeout
        self._timeout = timeout
        self._lock = Lock()
        self._free_connections = deque()
        self._semaphore = Semaphore(max_pool_size)

    @retry(10, suppress_exceptions=connection_error)
    def execute(self, method, *args, **kwargs):
        with self._semaphore:
            with self._get_connection() as conn:
                return conn.execute(method, *args, **kwargs)

    def reset(self):
        with self._lock:
            for conn in self._free_connections:
                conn.close()
            self._free_connections.clear()

    @contextmanager
    def _get_connection(self):
        conn = self._ensure_connection()
        try:
            yield conn
        except (KeyboardInterrupt, Exception) as e:
            if not isinstance(e, (ErrorFromBackend, GatewayError)):
                conn.close()
            raise e
        else:
            with self._lock:
                self._free_connections.append(conn)

    def _ensure_connection(self):
        with self._lock:
            while self._free_connections:
                # round-robin to distribute load
                conn = self._free_connections.popleft()
                if conn.is_normal():
                    return conn
                else:
                    conn.close()

        return self._new_connection()

    @retry(3, suppress_exceptions=timeout_error)
    def _new_connection(self):
        s = Connection.sock_factory(self._addr, timeout=self._connect_timeout)
        s.settimeout(self._timeout)
        return Connection(s, self._auth)

    close = reset
