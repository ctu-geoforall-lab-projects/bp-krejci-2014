#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string
sys.path.append('/home/matt/Dropbox/BC/python/komodo/postgrespy/pgwrapper.py') 
# Import our wrapper.
from pgwrapper import pgwrapper as pg



def computePrecip(db):
    
    link_num=db.count("link")
    
    sql="create view test as select* from record order by time::date asc ,time::time asc limit 36"
    print db.executeSql(sql)

    
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
    
    computePrecip(db)
 
    
    
main()