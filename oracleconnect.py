# -*- coding: utf-8 -*-
"""
Created on Wed May  8 09:47:55 2019

@author: o.orujov
"""

import os
import collections
import cx_Oracle
import datetime
from getpass import getpass


class ConnectToOracle:
    
    def __init__(self, delimiter=':'):
        self.__delimiter = delimiter
        self.__file_conn = False
        self.__check_temp = ''
        if self.__set_connection_str():
            self.__check_temp = self.__check_connection()
        while self.__check_temp == "Invalid username/password" and self.__file_conn == False:
            print(self.__check_temp + '\n')
            if self.__set_connection_str():
                self.__check_temp = self.__check_connection()
            else:
                break
        print(self.__check_temp + '\n')
    
    @property
    def conn_str(self):
        return self.__oracle_con
    
    @property
    def conn_check(self):
        return True if self.__check_temp == 'Connection established' else False

    @property
    def conn(self):
        try:
            return self.__conn
        except:
            return None

    def __set_connection_str(self):
        try:
            self.__oracle_con = '{user}/{password}@{host}/{db}'
            conn_dict = collections.defaultdict(str)
            fh = None
            try:
                fh = open('oracle_connection.txt', 'r')
                for conn_str in fh.readlines():
                    pos = conn_str.find(self.__delimeter)
                    if pos != -1:
                        conn_dict[str.lower(conn_str[:pos]).strip()] = conn_str[pos+1::].strip()
            except FileNotFoundError:
                print("'Oracle_connection.txt' file not found, please be sure to put that file to current directory({0})".format(os.getcwd()))
                raise
            except Exception:
                raise
            finally:
                if fh is not None:
                    fh.close()
            if 'password' not in conn_dict and len(conn_dict) > 0:
                while True:
                    password = getpass("Enter password for "+str.upper(conn_dict['user'])+" user: ")
                    if len(Password) == 0:
                        print("--- No password was given ---")
                    else:
                        conn_dict['password'] = password
                        break
            else: 
                self.__file_conn = True
            self.__oracle_con = self.__oracle_con.format(user=conn_dict['user'], password=conn_dict['password'],
                                                         host=conn_dict['host'],  db=conn_dict['db'])
            return True
        except Exception as err:
            self.__log_error(err)
            return False

    def __check_connection(self):
        v_return = None
        con = None
        try:
            con = cx_Oracle.connect(self.__oracle_con)
            self.__conn = con
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 12154:
                v_return = error.message + '\n\t See "Oracle_connection.txt" file'
            elif error.code == 1017:
                if self.__file_conn:
                    v_return = 'Invalid username/password, please check "Oracle_connection.txt" file'
                else:
                    v_return = 'Invalid username/password'
            else:
                v_return = '----------Exception----------\n\n' + error.message + '\n\n-----------------------------'
        finally:
            if con is not None:
                pass
        return 'Connection established' if v_return is None else v_return

    @staticmethod
    def __log_error(self, error):
        print(error)
        error_time = datetime.datetime.strftime(datetime.datetime.now(), '%d.%m.%Y %H:%M:%S')
        row_delimiter = '\n' + '-' * 15 + __name__ + '-' * 15
        error_record = '[' + error_time + '] ' + str(error) + row_delimiter
        with open('error_log.txt', 'a', encoding='utf-8') as log_file:
            log_file.write(error_record + '\n')


if __name__ == '__main__':
    conn = ConnectToOracle()
