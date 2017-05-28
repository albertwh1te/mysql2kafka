# -*- coding: utf-8 -*-
import json
from bson import json_util
from utils import json_serial

import socket

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import QueryEvent,RotateEvent

from config import sqlite_settings,kafka_settings
from sqlite import Sqlite
from kafkaapi import Kafka


ip_value = socket.gethostbyname(socket.gethostname())

class CUDHandler(object):

    def __init__(self,binlogevent,verbose=1):
        self.binlogevent = binlogevent
        self.event = {
            "metadata":
            {
                "alias": "database:"+binlogevent.schema+" python:{}".format(ip_value),
                "table": binlogevent.table
            },
            "rows":""
        }
        self.verbose = verbose
        # self.log_file = binlogevent.

    def __addtype(self):
        #TODO 1->insert into, 2->delete, 3->update 4->alter
        # import ipdb; ipdb.set_trace()

        if isinstance(self.binlogevent, DeleteRowsEvent):
            self.event["metadata"]["event"] = "delete"

        elif isinstance(self.binlogevent, WriteRowsEvent):
            self.event["metadata"]["event"] = "insert"

        elif isinstance(self.binlogevent, UpdateRowsEvent):
            self.event["metadata"]["event"] = "update"
            if self.verbose == 1:
                self.event['rows'] = self.binlogevent.rows

    def tojson(self):
        self.event['rows'] = self.binlogevent.rows
        self.__addtype()
        return json.dumps(self.event,default=json_serial)


class RotateeventHandler(object):

    def __init__(self,rotateevent):
        self.log_file = rotateevent.next_binlog
        self.position = rotateevent.position

    def show(self):
        print('log file and position changed to {log_file}:{position}').format(
                log_file=self.log_file,
                position=self.position
            )

    def todb(self):
        db = Sqlite(sqlite_settings)
        # print(db.getPositionorCreate(self.log_file,self.position))
        db.updatePosition(self.log_file,self.position)


class QueryeventHandler(object):

    def __init__(self,queryevent):
        self.queryevent = queryevent
        self.event = {
            "metadata":
            {
                "alias": "database:"+queryevent.schema+" python:{}".format(ip_value),
                "event":"alter"
            },
            "sql":queryevent.query
        }


    def tojson(self):
        return json.dumps(self.event,default=json_serial)


class GeneralHandler(object):

    def __init__(self,binlogevent):
        if isinstance(binlogevent,QueryEvent):
            self.handler = QueryeventHandler(binlogevent)
        # elif isinstance(binlogevent,RotateEvent):
        #     self.handler = RotateeventHandler(binlogevent)
        else:
            self.handler = CUDHandler(binlogevent)

    def toKafka(self):
        if self.handler.event.get('sql',None) != 'BEGIN' and self.handler.event.get('sql',None) != 'COMMIT':
          kafka = Kafka(
              kafka_settings.get("bootstrap_servers",None),
              kafka_settings.get("zookeeper",None)
          )
          if self.handler.tojson():
              results = kafka.send(
                  kafka_settings.get('topic'),
                  self.handler.tojson()
              )
              return results


