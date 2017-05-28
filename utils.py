# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import RotateEvent,QueryEvent
from pymysqlreplication import BinLogStreamReader

from config import mysql_settings

def create_stream(log_file=None,log_pos=None):
    return BinLogStreamReader(
        connection_settings = mysql_settings,
        server_id=1,
        blocking=True,
        log_file=log_file,
        # only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent,RotateEvent,QueryEvent],
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent,QueryEvent],
        log_pos=log_pos,
        resume_stream=True
    )


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
    # if isinstance(obj,datetime.datetime.date):
        # serial = obj.strftime('%Y-%m-%d')
    # if isinstance(obj,datetime.datetime.day):
    #     serial = obj.strftime('%Y-%m-%d')
    if isinstance(obj,Decimal):
        serial = str(obj)
    return serial
