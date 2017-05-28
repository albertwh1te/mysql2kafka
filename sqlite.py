# -*- coding: utf-8 -*-

import sqlite3


class Sqlite(object):

    def __init__(self,sqlite_config):
        self.config = sqlite_config
        self.table_name = sqlite_config.get("table_name")
        self.id_field = sqlite_config.get("id_field")

    def getPositionorCreate(self):
        # import ipdb; ipdb.set_trace()
        conn = sqlite3.connect(self.config.get("sqlite_file"))
        c = conn.cursor()

        # import ipdb; ipdb.set_trace()

        try:
            sql =  "SELECT file,pos FROM {table_name};".format(

                table_name = self.table_name
            )
            c.execute(sql)
            results = c.fetchone()
            if results:
                return results
            else:
                sql = "INSERT INTO {table_name} (id,file,pos) VALUES (1,1,1);".format(
                        table_name = self.table_name,
                        id_field = self.id_field,
                    )
                c.execute(sql)
            return None,None

        except Exception as e:
            sql = """
                INSERT INTO {table_name} VALUES (1,1,1);
                """.format(
                    table_name = self.table_name,
                    id_field = self.id_field)
            c.execute(
                """
                CREATE TABLE {table_name} ({id_field} INTEGER PRIMARY KEY,file TEXT, pos INTEGER);
                """.format(
                    table_name = self.table_name,
                    id_field = self.id_field,
                )
                )
            c.execute(sql)
            sql2 = "UPDATE {table_name} SET file = '{file_name}',pos = '{pos}'".format(
                table_name = self.table_name,
                pos=1,
                file_name='22'
                )
            c.execute(sql2)
            return "created",'ff'
        finally:
            conn.commit()
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
