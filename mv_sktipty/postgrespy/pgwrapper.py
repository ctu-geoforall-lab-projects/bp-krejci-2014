#!/usr/bin/env python
# -*- coding: utf-8

import sys, os, numpy, argparse
import psycopg2 as ppg



class pgwrapper:
        def __init__(self, dbname, host='localhost', user='', passwd=''):
                self.dbname = dbname                    # Database name which connect to.
                self.host = host                        # Host name (default is "localhost")
                self.user = user                        # User name for login to the database.
                self.password = passwd                  # Password for login to the database. 
                self.connection = self.setConnect()     # Set a connection to the database
                self.cursor = self.setCursor()          # Generate cursor.
                
        def setConnect(self):
                conn = ppg.connect("dbname='"+self.dbname+"' user='"+self.user+"' host='"+self.host+"' password='"+self.password+"'")
                return conn
        
        def setCursor(self):
                return self.connection.cursor()
        

        
        def executeSql(self,sql,results=True):
                # Excute the SQL statement.
                try:
                        self.cursor.execute(sql)
                except Exception, e:
                        self.connection.rollback()
                        print e.pgerror
                        pass
             
                
                if results :
                        # Get the results.
                        results = self.cursor.fetchall()
                        # Return the results.1
                        return results
        
        def count(self, table):
                """!Count the number of rows.
                @param table         : Name of the table to count row"""
                sql_count='SELECT COUNT(*) FROM "' + table + '"'
                self.cursor.execute(sql_count)
                n=self.cursor.fetchall()[0][0]
                return n
        

                