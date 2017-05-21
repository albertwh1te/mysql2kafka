# -*- coding: utf-8 -*-

import sqlite3


class Sqlite(object):

    def __init__(self,sqlite_config):
        self.config = sqlite_config
        self.table_name = sqlite_config.get("table_name")
        self.id_field = sqlite_config.get("id_field")

    def getPositionorCreate(self):
        conn = sqlite3.connect(self.config.get("sqlite_file"))
        c = conn.cursor()
        try:
            sql =  "SELECT file,pos from {table_name}".format(
                table_name = self.table_name
            )
            c.execute(sql)
            conn.commit()
            return c.fetchone()
        except Exception as e:
            c.execute(
                """
                CREATE TABLE {table_name} ({id_field} INTEGER PRIMARY KEY,file TEXT, pos INTEGER);
                """.format(
                    table_name = self.table_name,
                    id_field = self.id_field,
                )
                )
            return "created"
        finally:
            conn.close()

    def updatePosition(self,file_name,pos):
        conn = sqlite3.connect(self.config.get("sqlite_file"))
        c = conn.cursor()

        try:
            sql = "UPDATE {table_name} SET file = '{file_name}',pos = '{pos}'".format(
                table_name = self.table_name,
                    file_name = file_name,
                    pos = pos
            )
            c.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
