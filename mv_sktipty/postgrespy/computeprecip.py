#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random
sys.path.append('/home/matt/Dropbox/BC/python/komodo/postgrespy/pgwrapper.py') 
# Import our wrapper.
from pgwrapper import pgwrapper as pg
-
def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def computePrecip(db,baseline_decibel,Aw):
#------------------------------------------------------------------functions-------------------------------------------    
#nuber of link in table link   
    link_num=db.count("link")
    
#nuber of records in table record  
    record_num=db.count("record")
    
#compute A...(substraction "rxpower-txpower")
    sql="ALTER TABLE record ADD COLUMN A double precision; "
    print db.executeSql(sql)
    sql="UPDATE  record SET  B=(rxpower-txpower); "
    print db.executeSql(sql)
    
#create view of record sorting by time asc!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!limit 10
    table=randomword(5)
    sql="CREATE VIEW %s AS SELECT * from record ORDER BY time::date asc ,time::time asc limit 10 "%table
    print db.executeSql(sql)

    
    for record in record_num:
#select precip from first row from view
        sql="SELECT A FROM %s LIMIT 1 "%table
        A=db.executeSql(sql)
        print A
        
#select linkid from first row from view
        sql="SELECT linkid FROM %s LIMIT 1 "%table
        linkid=db.executeSql(sql)
        print linkid
        
#get length [meters]of link
        sql="SELECT record.linkid,ST_Length(link.geom,false)\
        FROM link join record on link.linkid=record.linkid \
        WHERE link.linkid = record.linkid AND link.linkid=%s\
        limit 1"%linkid
        length=db.executeSql(sql)
        
#delete first rows in view        
        sql="DELETE FROM %s LIMIT 1" %table  
        print db.executeSql(sql)
  
#------------------------------------------------------------------main-------------------------------------------    
def main():
    print "heloo "
    db_schema="public"
    db_name="mvdb"
    db_host="localhost"
    db_port="5432"
    db_user='grass'
    db_password= None
    srid= 4326
    
    timestep=60
    baseline_decibel=1
    Aw=1.5
    

    
#if required password and user    
    if db_password:        
        db = pg(dbname=db_name,srid=srid, host=db_host,
                user=db_user, passwd = db_password)   
#if required only user
    elif db_user and not db_password:         
        db = pg(dbname=db_name,srid=srid, host=db_host,
                user=db_user)
#if not required user adn passwd   
    else:
        db = pg(dbname=db_name,srid=srid, host=db_host,
                user=db_user)    
#compute precipitation    
    computePrecip(db,baseline_decibel,Aw)
 
    
    
main()