# -*- coding: utf-8 -*- from bson import json_util
from utils import create_stream
from handler import GeneralHandler
from config import sqlite_settings
from sqlite import Sqlite

def main():
    # try:
    sqlite = Sqlite(sqlite_settings)
    log_file,log_pos = sqlite.getPositionorCreate()
    print("from sqlite info: {},{}".format(log_file,log_file))
    stream = create_stream(log_file,log_pos)
    stream = create_stream()
    for binlogevent in stream:
        print(binlogevent.event_type)
        log_file,log_pos = sqlite.getPositionorCreate()
        print("from sqlite info: {},{}".format(log_file,log_pos))

        handler = GeneralHandler(binlogevent)
        results = handler.toKafka()
        if results:
            sqlite.updatePosition(stream.log_file,stream.log_pos)
        print('stream info {},{}'.format(
            stream.log_file,stream.log_pos
        ))
        print('success one')

    stream.close()
    # except Exception as e:
        # print(e)



if __name__ == '__main__':
    main()

