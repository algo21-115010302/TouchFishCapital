# -*- coding: utf-8 -*-
import threading
import socket

from rqdatac.share.errors import ErrorFromBackend, GatewayError
from rqdatac.connection import Connection
from rqdatac.decorators import retry
from rqdatac.utils import connection_error, timeout_error


class ThreadLocalConnection:
    def __init__(self, addr, auth=None, connect_timeout=5, timeout=60):
        self._local = threading.local()
        self._addr = addr
        self._auth = auth
        self._connect_timeout = connect_timeout
        self._timeout = timeout

    def _get_connection(self):
        conn = getattr(self._local, "connection", None)
        if conn is not None:
            if conn.is_normal():
                return conn
            else:
                self.close()
        s = Connection.sock_factory(self._addr, timeout=self._connect_timeout)
        s.settimeout(self._timeout)
        self._local.connection = Connection(s, self._auth)
        return self._local.connection

    def _execute(self, conn, method, args, kwargs):
        try:
            return conn.execute(method, *args, **kwargs)
        except (KeyboardInterrupt, Exception) as e:
            if not isinstance(e, (ErrorFromBackend, GatewayError)):
                self.reset()
                conn.close()
            raise e

    @retry(3, suppress_exceptions=(connection_error, GatewayError, timeout_error, socket.timeout))
    def execute(self, method, *args, **kwargs):
        return self._execute(self._get_connection(), method, args, kwargs)

    @retry(3, suppress_exceptions=(connection_error, GatewayError))
    def execute_with_timeout(self, timeout, method, *args, **kwargs):
        assert isinstance(timeout, int) and timeout > 0
        conn = self._get_connection()
        conn.set_timeout(timeout)
        r = self._execute(conn, method, args, kwargs)
        conn.set_timeout(self._timeout)
        return r

    def close(self):
        if getattr(self._local, "connection", None) is not None:
            self._local.connection.close()
            self._local.connection = None

    reset = close
