#!/usr/bin/env python
# -*- coding: utf-8

import sys, os, numpy, argparse
import psycopg2 as ppg



class pgwrapper:
        def __init__(self, dbname, host='', user='', passwd=''):
                self.dbname = dbname                    # Database name which connect to.
                self.host = host                        # Host name (default is "localhost")
                self.user = user                        # User name for login to the database.
                self.password = passwd                  # Password for login to the database. 
                self.connection = self.setConnect()     # Set a connection to the database
                self.cursor = self.setCursor()          # Generate cursor.
                
        def setConnect(self):
                conn_string = "dbname='%s' user='%s'" % (self.dbname, self.user)
                if self.host:
                        conn_string += " host='%s'" % self.host
                if self.password:
                        conn_string += " passwd='%s'" % self.password
                
                conn = ppg.connect(conn_string)
                return conn
        
        def setCursor(self):
                return self.connection.cursor()
                

        
        def executeSql(self,sql,results=True,commit=False):
                # Excute the SQL statement.
                #print sql
                try:
                        self.cursor.execute(sql)
                except Exception, e:
                        self.connection.rollback()
                        print e.pgerror
                        pass
                if commit:
                        self.connection.commit()
                        print 'commited'

                if results :
                        # Get the results.
                        results = self.cursor.fetchall()
                        # Return the results.1
                        return results
        '''
        def executeSqlP(self,sql,data):
                # Excute the SQL statement.
                try:
                        self.cursor.mogrify(sql,data)
                except Exception, e:
                        self.connection.rollback()
                        print e.pgerror
                        pass
                self.connection.commit()
        '''
        
        def count(self, table):
                """!Count the number of rows.
                @param table         : Name of the table to count row"""
                sql_count='SELECT COUNT(*) FROM "' + table + '"'
                self.cursor.execute(sql_count)
                n=self.cursor.fetchall()[0][0]
                return n
        
        def updatecol(self, table, columns, where=''):
                """!Update the values of columns.
                @param table            : Name of the table to parse.
                @param columns          : Keys values pair of column names and values to update.
                @param where            : Advanced search option for 'where' statement.
                """
                # Make a SQL statement.
                parse = '' 
                for i in range(len(columns)):
                        parse = parse + '"' + str(dict.keys(columns)[i]) + '"=' + str(dict.values(columns)[i]) + ','
                parse = parse.rstrip(',')

                if where=='':
                        sql_update_col = 'UPDATE "' + table + '" SET ' + parse
                else:
                        sql_update_col = 'UPDATE "' + table + '" SET ' + parse + ' WHERE ' + where
                print "upcol %s"%sql_update_col      
                # Excute the SQL statement.
                self.cursor.execute(sql_update_col)
                
