#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random
import math
import timeit 
import time
import psycopg2
import atexit
from pgwrapper import pgwrapper as pg


try:
    from grass.script import message,core as grass  
except ImportError:
    import grass
except:
    raise Exception ("Cannot find 'grass' Python module. Python is supported by GRASS from version >= 6.4" )



#%module
#% description: Module for working with microwave links
#%end
##########################################################
################### Interpolation ########################
##########################################################



##########################################################
############## guisection: database work ################
##########################################################

#%option
#% key:host
#% type: string
#% label: Host
#% description: Host name of the machine on which the server is running.
#% guisection: Database
#% required : no
#%end

#%option
#% key: port
#% type: integer
#% label: Port
#% description: TCP port on which the server is listening, usually 5432.
#% guisection: Database
#% required : no
#%end

#%option
#% key: database
#% type: string
#% key_desc : name
#% gisprompt: old_dbname,dbname,dbname
#% label: Database
#% description: Database name
#% guisection: Database
#% required : yes
#%end

#%option
#% key: schema
#% type: string
#% label: Schema
#% description: Database schema.
#% guisection: Database
#% required : no
#%end

#%option
#% key: user
#% type: string
#% label: User
#% description: Connect to the database as the user username instead of the  default.
#% guisection: Database
#% required : no
#%end

#%option
#% key: password
#% type: string
#% label: Password
#% description: Password will be stored in flat file!
#% guisection: Database
#% required : no
#%end
##########################################################
############## guisection: Preprocesing #################
##########################################################

#%option 
#% key: fromtime
#% label: First timestamp "YYYY-MM-DD H:M:S"
#% description: Set first timestamp in format "YYYY-MM-DD H:M:S"
#% type: string
#% guisection: Preprocesing
#%end

#%option 
#% key: totime
#% label: Last timestamp "YYYY-MM-DD H:M:S"
#% description: Set last timestamp in format "YYYY-MM-DD H:M:S"
#% type: string
#% guisection: Preprocesing
#%end

#%option 
#% key: baseline
#% label: Baseline value
#% description: This options set baseline A0[dB]. note (Ar=A-A0-Aw)
#% type: double
#% guisection: Preprocesing
#%end

#%option 
#% key: aw
#% label: Aw value
#% description: This options set Aw[dB] value for safety. note (Ar=A-A0-Aw)
#% type: double
#% guisection: Preprocesing
#%end

#%flag
#% key:f
#% description: First run (prepare columns, make geometry etc)
#% required : no
#% guisection: Preprocesing
#%end

#%flag
#% key:c
#% description: Compute precip in db (not necessary if you computed precip in the past)
#% required : no
#% guisection: Preprocesing
#%end

#%flag
#% key:m
#% description: Create time windows in db (not necessary if created in the past)
#% required : no
#% guisection: Preprocesing
#%end





############################################
######################## info ##############
############################################

#%flag
#% key:t
#% description: Get info about timestamp(first,last) in db 
#% required : no
#% guisection: Info
#%end




view="view"
mesuretime=0
restime=0
view_statement = "TABLE"
schema_name = "temp3"  #new scheme, no source
fromtime= "2013-09-09 19:59:00"
totime="2013-09-09 23:59:00"
record_tb_name= "record"
temp_windows_names=[]

def print_message(msg):
    print '-' * 80
    print msg
    print '-' * 80
    print 
    sys.stdout.flush()
    
def dbConnGrass(host,port,database,schema,user,password):
    print_message("Connecting to db-GRASS...")
    host = options['host']
    port = options['port']
    database = options['database']
    schema = options['schema']
    user = options['user']
    password = options['password']

    # Test connection
    conn = "dbname=" + database
    if host: conn += ",host=" + host
    if port: conn += ",port=" + port

    # Unfortunately we cannot test untill user/password is set
    if user or password:
        print_message("Setting login (db.login) ... ")
        if grass.run_command('db.login', driver = "pg", database = conn, user = user, password = password) != 0:
            grass.fatal("Cannot login")

    # Try to connect
    print "Testing connection ..."
    sys.stdout.flush()
    if grass.run_command('db.select', quiet = True, flags='c', driver= "pg", database=conn, sql="select version()" ) != 0:
        if user or password:
            pprint_message( "Deleting login (db.login) ...")
            if grass.run_command('db.login', quiet = True, driver = "pg", database = conn, user = "", password = "") != 0:
                print_message("Cannot delete login.")
        grass.fatal("Cannot connect to database.")

    if grass.run_command('db.connect', driver = "pg", database = conn, schema = schema) != 0:
        grass.fatal("Cannot connect to database.")

def dbConnPy():
    print_message("Conecting to db by psycopg")
    db_host = options['host']
    db_name = options['database']
    db_user = options['user']
    db_password = options['password']
    
    try:
        #if require password and user    
        if db_password:        
            db = pg(dbname=db_name, host=db_host,
                    user=db_user, passwd = db_password)
            print " #required password and user "
        #if required only user
        elif db_user and not db_password:         
            db = pg(dbname=db_name, host=db_host,
                    user=db_user)
            print "required only user"
        #if not required user and passwd   
        else:
            db = pg(dbname=db_name, host=db_host)
            print "not required user and passwd"
    except psycopg2.OperationalError, e:
        sys.exit("I am unable to connect to the database (db=%s, user=%s). %s" % (db_name, db_user, e))
    return db
    
def firstRun(db):
        print_message("Preparing database...")
    
        sql="CREATE EXTENSION postgis;"
        db.executeSql(sql,False,True)
        #sql="CREATE EXTENSION postgis_topology;"
        #db.executeSql(sql,False,True)
        sql="SELECT AddGeometryColumn   ('public','node','geom',4326,'POINT',2); "
        db.executeSql(sql,False,True)
        sql="SELECT AddGeometryColumn  ('public','link','geom',4326,'LINESTRING',2); "
        db.executeSql(sql,False,True)
        sql="UPDATE node SET geom = ST_SetSRID(ST_MakePoint(long, lat), 4326); "
        db.executeSql(sql,False,True)
        sql="UPDATE link SET geom = st_makeline(n1.geom,n2.geom) \
        FROM node AS n1 JOIN link AS l ON n1.nodeid = fromnodeid JOIN node AS n2 ON n2.nodeid = tonodeid WHERE link.linkid = l.linkid; "
        db.executeSql(sql,False,True)
        sql="alter table record add column polarization char(1); "
        db.executeSql(sql,False,True)
        sql="alter table record add column lenght real; "
        db.executeSql(sql,False,True)
        sql="alter table record add column precipitation real; "
        db.executeSql(sql,False,True)
        sql="alter table record add column a real; "
        db.executeSql(sql,False,True)
        sql="update record set a= rxpower-txpower;  "
        db.executeSql(sql,False,True)
        sql="update record  set polarization=link.polarization from link where record.linkid=link.linkid;"
        db.executeSql(sql,False,True)
        sql="update record  set lenght=ST_Length(link.geom,false) from link where record.linkid=link.linkid;"
        db.executeSql(sql,False,True)
        sql="CREATE SEQUENCE serial START 1; "
        db.executeSql(sql,False,True)
        sql="alter table record add column recordid integer default nextval('serial'); "
        db.executeSql(sql,False,True)
        sql="CREATE INDEX idindex ON record USING btree(recordid); "
        db.executeSql(sql,False,True)
        sql="CREATE INDEX timeindex ON record USING btree (time); "
        db.executeSql(sql,False,True)

def randomWord(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def computeAlphaK(freq,polarization):
    """@RECOMMENDATION ITU-R P.838-3
    Specific attenuation model for rain for use in prediction methods
    γR = kR^α
    return kv and αv (vertical polarization)
    return kh and αh (horizontal polarization)   
    """
    if polarization =="h":
    #Coefficients for kH    1
        aj_kh=(-5.33980,-0.35351,-0.23789,-0.94158)
        bj_kh=(-0.10008,1.26970,0.86036,0.64552)
        cj_kh=(1.13098,0.45400,0.15354,0.16817)
        mk_kh=-0.18961
        ck_kh=0.71147
    
    #Coefficients for αH    3
        aj_ah=(-0.14318, 0.29591, 0.32177,-5.37610,16.1721)
        bj_ah=(1.82442, 0.77564, 0.63773,-0.96230,-3.29980)
        cj_ah=(-0.55187, 0.19822, 0.13164,1.47828,3.43990)
        ma_ah=0.67849
        ca_ah=-1.95537
        kh=0
        ah=0
    #kh.. coefficient k of horizontal polarization
        for j in range(0,len(aj_kh)):
            frac_kh=-math.pow(((math.log10(freq) - bj_kh[j]) / cj_kh[j]),2)
            kh+=aj_kh[j]* math.exp(frac_kh)
            
        kh=10**(kh+ mk_kh * math.log10(freq) + ck_kh)
        
    #ah.. coefficient α of horizontal polarization
        for j in range(0,len(aj_ah)):
            frac_ah=-math.pow(((math.log10(freq) - bj_ah[j]) / cj_ah[j]),2)
            ah+=aj_ah[j]* math.exp(frac_ah)
            
        ah=ah + ma_ah * math.log10(freq) + ca_ah

        return (ah,kh)
    else:    
        #Coefficients for kV    2
        aj_kv=[-3.80595,-3.44965,-0.39902,0.50167]
        bj_kv=[0.56934,-0.22911,0.73042,1.07319]
        cj_kv=[0.81061,0.51059,0.11899,0.27195]
        mk_kv=-0.16398
        ck_kv=0.63297
        
        #Coefficients for αV   4
        aj_av=[-0.07771, 0.56727, -0.20238,-48.2991, 48.5833]
        bj_av=[2.33840, 0.95545, 1.14520,0.791669,0.791459]
        cj_av=[-0.76284, 0.54039, 0.26809,0.116226,0.116479]
        ma_av=-0.053739
        ca_av=0.83433
        kv=0
        av=0
    #kv.. coefficient k of vertical polarization       
        for j in range(0,len(aj_kv)):
            frac_kv=-math.pow(((math.log10(freq) - bj_kv[j]) / cj_kv[j]),2)
            kv+=aj_kv[j]* math.exp(frac_kv)
            
        kv=10**(kv + mk_kv * math.log10(freq) + ck_kv)
    
    #av.. coefficient α of vertical polarization       
        for j in range(0,len(aj_av)):
            frac_av=-math.pow(((math.log10(freq) - bj_av[j]) / cj_av[j]),2)
            av+=aj_av[j]* math.exp(frac_av)             
    
        av=(av + ma_av * math.log10(freq) + ca_av)
        
        return (av,kv)   

def st(mes=True):
    if mes:
        global mesuretime
        global restime
        mesuretime= time.time()
    else:
        restime=time.time() - mesuretime
        print "time is: ", restime

def computePrecip(db):
    print_message("Prepare db for computing precipitation...")
    
    baseline_decibel= options['baseline']
    baseline_decibel=float(baseline_decibel)
    Aw=options['aw']
    Aw=float(Aw)
    
    #nuber of link and record in table link
    link_num=db.count("link")
    record_num=db.count("record")
    print_message("Numer of records %s"%record_num)
    
    #select values for computing
    xx=record_num
    sql=" select time,a,lenght,polarization,frequency,linkid from %s order by recordid limit %d ; "%(record_tb_name, xx)
    resu=db.executeSql(sql,True,True)
    
    #crate table for results
    global temptb
    temptb="result_" + randomWord(4)
    sql="create table %s.%s ( linkid integer,time timestamp, precipitation real);"%(schema_name,temptb)
    db.executeSql(sql,False,True)
    
    #save name of result table for next run without compute precip
    r= open("last_res","wr")
    r.write(temptb)
    r.close()
    
    
    #optimalization of commits
    db.setIsoLvl(0)
    #mesasure computing time
    st()
    
    #open blankfile
    io= open("precip","wr")
    
    recordid = 1
    temp= []
    print_message("Computing precipitation...")
    
    for record in resu:
    #coef_a_k[alpha, k]
        coef_a_k= computeAlphaK(record[4],record[3])
        
    #final precipiatation is R1    
        Ar=(-1)*record[1]-baseline_decibel-Aw
        yr=Ar/record[2]  
        alfa=1/coef_a_k[1]
        beta=1/coef_a_k[0]
        R1=(yr/beta)**(1/alfa)
        
    #string for output flatfile
        out=str(record[5])+"|"+str(record[0])+"|"+str(R1)+"\n"
        temp.append(out)
        recordid += 1
        
    #write values to flat file
    io.writelines(temp)
    io.close()
    print_message("Write precipitation to database...")
    io=open("precip","r")
    db.copyfrom(io,"%s.%s"%(schema_name,temptb))
    io.close()
    
    #sql="drop table %s.%s"%(schema_name,temptb)
    #db.executeSql(sql,False,True) 
    st(False)
    print 'AMD Phenom X3 ocek cas minut %s'%((record_num * (restime / xx)) / 60)    

def sumPrecip(db,sumprecip,from_time,to_time):
    print_message("Creating time windows...")
    #@function sumPrecip make db views for all timestamps
    io=open("last_res",'r')
    temptb=io.read()
    io.close()
    print temptb
    
    #summing values per (->user)timestep interval
    view_db="sum_"+randomWord(3)
    sql="CREATE %s %s.%s as select\
        linkid,avg(precipitation)as precip_mm_h, date_trunc('%s',time)\
        as timestamp FROM %s.%s GROUP BY linkid, date_trunc('%s',time)\
        ORDER BY timestamp"%(view_statement, schema_name, view_db ,sumprecip ,schema_name,temptb ,sumprecip)    
    data=db.executeSql(sql,False,True)
   
    #num of rows of materialized view
    record_num=db.count("%s.%s"%(schema_name,view_db))
    
    #the user's choice or all records 
    if from_time and to_time:
        first_timestamp=from_time
        last_timestamp=to_time
    else:    
        #get first timestamp
        sql="select timestamp from %s.%s limit 1"%(schema_name,view_db)
        first_timestamp=db.executeSql(sql)[0][0]
        
        #get last timestep
        sql="select timestamp from  %s.%s offset %s"%(schema_name,view_db,record_num-1)
        last_timestamp=db.executeSql(sql)[0][0]
    
    #make view per(->user) interval for all timestamp and for all links
    time_const=0
    if sumprecip=="minute":
        tc=60
    elif sumprecip=='second':
        tc=1
    elif sumPrecip=="hour" :
        tc=3600
    else:
        tc=216000
        
        
    cur_timestamp=first_timestamp
    i=0
    global temp_windows_names
    io2=open("timewindow","wr")
    temp=[]
    while str(cur_timestamp)!=str(last_timestamp):
        #crate name of view
        a=time.strftime("%Y_%m_%d_%H_%M", time.strptime(str(cur_timestamp), "%Y-%m-%d %H:%M:%S"))
        view_name="%s%s"%(view,a)
        print "(timestamp'%s'+ %s * interval '1 second')" % (first_timestamp,time_const)
        temp.append(view_name)
        
        i+=1
        #create view
        sql="CREATE table %s.%s as select * from %s.%s where timestamp=(timestamp'%s'+ %s * interval '1 second')"%(schema_name,view_name,schema_name,view_db,first_timestamp,time_const)
        data=db.executeSql(sql,False,True)
        
        #go to next time interval
        time_const+=tc
        
        #compute cur_timestamp (need for loop)
        sql="select (timestamp'%s')+ %s* interval '1 second'"%(cur_timestamp,tc)
        cur_timestamp=db.executeSql(sql)[0][0]
        print cur_timestamp
        print last_timestamp
    
        
    #write values to flat file
    io2.writelines(temp)
    io2.close()
    
    
    sql="drop table %s.%s"%(schema_name,view_db)
    db.executeSql(sql,False,True) 

def grassWork():

    dbConnGrass(host,port,database,schema,user,password)
    
    io=open('timewindow','r')
    timewindow=io.read()
    r=0
    for window in timewindow:
        temp_windows_names[r]=window
        r+=1
        
    
    
    grass.run_command('v.in.ogr',
                layer = 'link',
                output = 'link',
                quiet = True,
                overwrite = True,)
    
    grass.run_command('v.category',
                input='link',
                output='link_nat',
                op="transfer",
                layer="1,2")
    
    grass.run_command('g.region',
                      vect='link_nat',
                      res=10 )
    
    grass.run_command('v.db.connect',
                    map='link_nat',
                    layer=1,
                    table=link,
                    key=linkid)
    
    
    for window in temp_windows_names:

        grass.run_command('v.db.connect',
                    map='link_nat',
                    layer=2,
                    table=window,
                    key=linkid)
        
        grass.run_command('v.to.rast',
                    input='link_nat',
                    layer=2,
                    output=window)
        

#------------------------------------------------------------------main-------------------------------------------    
def main():
    print_message("Script is running...")
    
    '''
    print "hello "
    db_schema="public"
    if len(sys.argv) > 1:
        db_name = sys.argv[1]
    else:
        db_name="letnany"
    if len(sys.argv) == 3: # hack for geo102 (TODO: fix it)
        db_host = '' 
    else:
        db_host="localhost"
    db_port="5432"
    if len(sys.argv) > 2:
        db_user = sys.argv[2]
    else:
        db_user='matt'
    if len(sys.argv) > 3:
        db_password = sys.argv[3]
    else:
        db_password= None
    '''


 
    
    
    
    #connect to database psycopg
    db=dbConnPy()
    if flags['t']:
        sql="create view tt as select time from %s order by time"%record_tb_name
        db.executeSql(sql,False,True)
        #get first timestamp
        sql="select time from tt limit 1"
        first_timestamp=db.executeSql(sql,True,True)[0][0]
        print_message('First timestamp is %s'%first_timestamp)
        record_num=db.count(record_tb_name)
        
        #get last timestep
        sql="select time from  tt offset %s"%(record_num-1)
        last_timestamp=db.executeSql(sql,True,True)[0][0]
        print_message('Last timestamp is %s'%last_timestamp)
        sql="drop view tt"
        db.executeSql(sql,False,True)
    else:    
        #first run- prepare db
        if flags['f']:
            firstRun(db)
       
    
        #compute precipitation
        if flags['c']:
            data=db.executeSql("CREATE SCHEMA %s" % schema_name,False,True)
            computePrecip(db)
           
        #make time wiindows
        if flags['m']:
            sumprecip=('second','minute', 'hour', 'day')
            sprc=sumprecip[1] 
            sumPrecip(db,sprc,fromtime,totime)
            
            
        grassWork()
    
if __name__ == "__main__":
    options, flags = grass.parser()

main()
