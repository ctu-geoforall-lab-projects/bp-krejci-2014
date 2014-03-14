#!/usr/bin/env python
# -*- coding: utf-8

import sys, os, numpy, argparse
import psycopg2 as ppg


from rpy2 import robjects as ro
from rpy2.robjects.packages import importr as rin
from progressbar import * 

r_base = rin('base')


class pgwrapper:
        def __init__(self, dbname, srid, host='localhost', user='', passwd=''):
                self.dbname = dbname                    # Database name which connect to.
                self.srid = str(srid)                   # Identifier of spatial reference system.
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
        
        
        def executeSql(self,sql):
                # Excute the SQL statement.
                self.cursor.execute(sql)
                # Get the results.
                #results = self.cursor.fetchall()
                results = self.cursor.fetchone()
                # Return the results.
                return results
        
        def count(self, table):
                """!Count the number of rows.
                @param table         : Name of the table to count row"""
                sql_count='SELECT COUNT(*) FROM "' + table + '"'
                self.cursor.execute(sql_count)
                n=self.cursor.fetchall()[0][0]
                return n
        
        def progresBarStart():
                label="aaa"
                widgets = [label+":", Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]
                pbar = ProgressBar(widgets=widgets, maxval=n+1).start()
                
        def progresBarStop():
                pbar.finish()
                