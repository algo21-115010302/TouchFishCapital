# -*- coding: utf-8 -*-
#
# Copyright 2018 Ricequant, Inc
import os
import logging

from . import client


class _RQDataClient:
    def __init__(self):
        import rqdatad.core.app
        import rqdatad.__main__
        import rqdatad.database

        app = rqdatad.core.app.app
        app.config = rqdatad.__main__.default_config_file()
        app.init()
        rqdatad.database.set_database_config(app.config)

        self.app = app
        self.PID = os.getpid()

    def execute(self, api_name, *args, **kwargs):
        return self.app.debug_api(api_name, *args, **kwargs)

    def close(self):
        import asyncio

        loop = self.app.get_event_loop()
        loop.stop()
        for task in asyncio.Task.all_tasks():
            task.cancel()
        loop.close()


def as_rqdata():
    logging.info('RQDATAC: behave like RQDATA now')
    client._CLIENT = _RQDataClient()
