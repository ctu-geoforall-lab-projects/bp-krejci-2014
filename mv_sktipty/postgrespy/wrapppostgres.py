#!/usr/bin/env python
# -*- coding: utf-8

import os
import sys
import argparse
import string

# Append python path to the custom wrappers.
sys.path.append('/home/matt/Dropbox/BC/python/komodo/postgrespy/pgwrapper.py') 
# Import our wrapper.
from pgwrapper import pgwrapper as pg

# import GRASS python lib
gisbase = os.environ['GISBASE'] = "/home/matt/Documents/grass_trunk/dist.x86_64-unknown-linux-gnu"

# settings path to data,location and mapset 
gisdbase = os.path.join(os.environ['HOME'], "grassdata")
location = "cr-wgs84"
mapset = "PERMANENT"

# import GRASS python lib
sys.path.append(os.path.join(os.environ["GISBASE"], "etc", "python"))
try:
    from grass.script import core as g
except ImportError:
    import grass.script.setup as gsetup


def main():

    #parser = argparse.ArgumentParser ()
    #group1 = parser.add_argument_group('group1', 'group database')
    #group1.add_argument("phost", help = 'database host')
    #group1.add_argument("pdb_name", help = 'database name')
    #group1.add_argument("pdb_schema", help = 'database schema')
    #group1.add_argument("pport", help = 'port')
    #group1.add_argument("puser", help = 'database user')
    #group1.add_argument("ppassword", help = 'database password')
    
    #group2 = parser.add_argument_group('group2', 'data config')
    #group2.add_argument("int",ptimestep", help = 'time step in sec')
    #args = parser.parse_args()`
    #db_schema=(args.accumulate(args.pdb_schema))
    
#jen docesne misto parser
    db_schema="public"
    db_name="mvdb"
    db_host="localhost"
    db_port="5432"
    db_user='grass'
    db_password= ''
    srid= 4326
    
    timestep=60
    
    
##set up and connect to postgres database in grass and by python driver##

#make string for connect database- example(database="host=myserver.itc.it,dbname=mydb")
    dbconn="dbname=" + db_name
    if db_host: dbconn += ",host = " + db_host
    if db_port: dbconn += ",port = " + db_port
    
#if required password and user    
    if db_password:  
        g.run_command('db.login',
                      user = db_user,
                      driver = 'pg',
                      database = dbconn,
                      password = db_password)
#for python driver        
        #db = pg(dbname=db_name,srid=srid, host=db_host, user=db_user, passwd = db_password)   
#if required only user
    #elif db_user and not db_password: 

#for python driver         
        #db = pg(dbname=db_name,srid=srid, host=db_host, user=db_user)
        
#if not required user adn passwd   
    #else:
        #db = pg(dbname=db_name,srid=srid, host=db_host, user=db_user)    
 
    g.run_command('db.connect',
                  driver = 'pg',
                  database = dbconn,
                  schema = db_schema)
    
##import data to grass from db##    
    g.run_command('v.in.ogr',
                dsn ="./",
                layer = link,
                output = link1,
                quiet = True,
                overwrite = True,)
    
    g.run_command('v.category',
                input=link1,
                output=link_nat,
                op="transfer",
                layer="1,2")

    g.run_command('v.db.connect',
                link_nat,
                layer=1,
                table=link,
                key=linkid)
    
    g.run_command('v.db.connect',
                link_nat,
                layer=2,
                table=record_test,
                key=linkid)

##compute nuber of links(need for db.record procesing)
    num_links=g.parse_command('db.select',
                                driver='pg',
                                database=db_name,
                                sql='select count(linkid) from link',
                                flags='c')
  
  
  
main() 
    

    
    


