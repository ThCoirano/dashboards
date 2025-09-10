#!/usr/bin/python
# coding: utf-8

import psycopg2
from utils.functions import config_log
from utils.functions import config_log 
from utils import constants
from utils import constants as  cs

log = config_log()


keepalive_kwargs = {
  "keepalives": 1,
  "keepalives_idle": 60,
  "keepalives_interval": 10,
  "keepalives_count": 5
}

def connectdatabase():
    #log.info('Connecting in database')
    con = psycopg2.connect(
        host = constants.HOST_POSTGRES,
        dbname = constants.DB_POSTGRES,
        user = constants.USER_POSTGRES,
        password = constants.PWD_POSTGRES,
        port = constants.PORT_POSTGRES,
        **keepalive_kwargs
    )
    return con

def connectaplicacao():
    #log.info('Connecting in database')
    con = psycopg2.connect(
        host = constants.HOST_APLICACAO,
        dbname = constants.DB_APLICACAO,
        user = constants.USER_APLICACAO,
        password = constants.PWD_APLICACAO,
        port = constants.PORT_APLICACAO,
        **keepalive_kwargs
    )
    return con



# Functions for reading scripts
class ScriptReader(object):

    @staticmethod
    def get_script(path):
        return open(path, 'r').read()

# Utils for messages
class Messages(object):

    @staticmethod
    def print_message(msg):
        log.info(msg)

#  functions to send and retrieve data
class DataManager(object):

    @staticmethod
    def execute_update(con, cur, script):
        message = None

        try:
            cur.execute(script)
            con.commit()
            message = "Update successful"
            result = True
        except Exception as e:
            Messages.print_message(e)
            con.rollback()
            message = e
            result = False
        finally:
            con.close()

        return (result, message)

    @staticmethod
    def execute_query(con, cur, script):
        try:
            cur.execute(script)
            con.commit()
            result = 'success'
            msg_error = 'success'
        except Exception as e:
            Messages.print_message(e)
            con.rollback()
            result = 'error'
            msg_error = e
        finally:
            con.close()
        return dict({'result':[result], 'msg_error':[msg_error] }) 
    
    @staticmethod
    def execute_query_fetch(con, cur, script):
        try:
            cur.execute(script)
            con.commit()
            result = cur.fetchall()
            msg_error = 'success'
        except Exception as e:
            Messages.print_message(e)
            con.rollback()
            msg_error = e
            result = 'error'
        finally:
            con.close()
        return dict({'result':[result], 'msg_error':[msg_error] }) 


    @staticmethod
    def get_conn_string(db_conn):
        return "dbname='{}' port='{}' user='{}' password='{}' host='{}'".format(
            db_conn['db_name'], db_conn['db_port'], db_conn['db_username'], db_conn['db_password'], db_conn['db_host'])

    @staticmethod
    def create_conn(conn_string):
        return psycopg2.connect(conn_string)
 
    @staticmethod
    def get_conn(db_connection):
        return DataManager.create_conn(
            DataManager.get_conn_string(db_connection))

    @staticmethod
    def run_update(script, db_connection):
        con = DataManager.get_conn(db_connection)
        return DataManager.execute_update(con, con.cursor(), script)

    @staticmethod
    def run_query(script, db_connection):
        con = DataManager.get_conn(db_connection)
        return DataManager.execute_query(con, con.cursor(), script)
    
    @staticmethod
    def run_query_fetch(script, db_connection):
        con = DataManager.get_conn(db_connection)
        return DataManager.execute_query_fetch(con, con.cursor(), script)

