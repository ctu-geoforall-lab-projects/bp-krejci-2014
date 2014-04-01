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
from math import sin, cos, atan2,degrees,radians, tan,sqrt

try:
    from grass.script import message,core as grass  
except ImportError:
    import grass
except:
    raise Exception ("Cannot find 'grass' Python module. Python is supported by GRASS from version >= 6.4" )


##########################################################
################## guisection: required ##################
##########################################################
#%module
#% description: Module for working with microwave links
#%end

#%option G_OPT_M_MAPSET
#% label: Name of working mapset
#% required: yes
#%end
##########################################################
################# guisection: Interpolation ##############
##########################################################

#%flag
#% key:i
#% description: Run GRASS analysis
#% guisection: Interpolation
#%end


##########################################################
############## guisection: database work #################
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
#% description: Connect to the database as the user username instead of the default.
#% guisection: Database
#% required : no
#%end

#%option
#% key: password
#% type: string
#% label: Password
#% description: Password will be stored in file!
#% guisection: Database
#% required : no
#%end

##########################################################
############## guisection: Preprocessing #################
##########################################################

#%option 
#% key: fromtime
#% label: First timestamp "YYYY-MM-DD H:M:S"
#% description: Set first timestamp in format "YYYY-MM-DD H:M:S"
#% type: string
#% guisection: Preprocessing
#%end

#%option 
#% key: totime
#% label: Last timestamp "YYYY-MM-DD H:M:S"
#% description: Set last timestamp in format "YYYY-MM-DD H:M:S"
#% type: string
#% guisection: Preprocessing
#%end

#%option 
#% key: baseline
#% label: Baseline value
#% description: This options set baseline A0[dB]. note (Ar=A-A0-Aw) i.e. 2
#% type: double
#% guisection: Preprocessing
#%end

#%option 
#% key: aw
#% label: Aw value
#% description: This options set Aw[dB] value for safety. note (Ar=A-A0-Aw) i.e. 1.5
#% type: double
#% guisection: Preprocessing
#%end

#%flag
#% key:f
#% description: First run (prepare columns, make geometry etc)
#% guisection: Preprocessing
#%end

#%flag
#% key:c
#% description: Compute precip in db (not necessary if you computed precip in the past)
#% guisection: Preprocessing
#%end

#%flag
#% key:m
#% description: Create time windows in db (not necessary if created in the past)
#% guisection: Preprocessing
#%end

#%option 
#% key: step
#% label: Interpolation step per meter
#% description: Interpolate points along links per meter (not necessary if created in the past)
#% type: integer
#% guisection: Preprocessing
#%end

#%flag
#% key:p
#% description: Interpolate points along links per meter (not necessary if created in the past)
#% guisection: Preprocessing
#%end


##########################################################
################## guisection: info ######################
##########################################################
#%flag
#% key:t
#% description: Print info about timestamp(first,last) in db 
#% guisection: Print
#%end


##########################################################
############### guisection: optional #####################
##########################################################

#%flag
#% key:r
#% description: Remove temp working schema
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
R=6371
path= os.path.dirname(os.path.realpath(__file__))+"/tmpdata"

def intrpolatePoints(db):
    print_message("Interpolating points along lines...")
    step=options['step']
    step=float(step)
    sql="select ST_AsText(link.geom),ST_Length(link.geom,false), linkid from link"
    resu=db.executeSql(sql,True,True)
    
    nametable="linkpoints"+str(step).replace('.0','')
    sql="DROP TABLE IF EXISTS %s.%s"%(schema_name,nametable)
    db.executeSql(sql,False,True)
    try:
        io= open(path+"/linkpointsname","wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    io.write(nametable)
    io.close
    
    sql="create table %s.%s (linkid integer,long real,lat real,point_id serial PRIMARY KEY) "%(schema_name,nametable)
    db.executeSql(sql,False,True)

    latlong=[]
    dist=[]
    linkid=[]
    points=[]
    a=0
    x=0
    
    
    try:
        io= open(path+"/linknode","wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
        
        
    temp=[]
    for record in resu:
        tmp=record[0]
        tmp = tmp.replace("LINESTRING(", "")
        tmp = tmp.replace(" ", ",")
        tmp = tmp.replace(")", "")
        tmp = tmp.split(",")
        
        latlong.append(tmp)
        
        lon1=latlong[a][0]
        lat1=latlong[a][1]
        lon2=latlong[a][2]
        lat2=latlong[a][3]
        
        dist=record[1]
        linkid=record[2]
    
        az=bearing(lat1,lon1,lat2,lon2)
        distt=dist
        
        a+=1
        while abs(distt) > step:
            lat1 ,lon1,finalBrg,backBrg=destinationPointWGS(lat1,lon1,az,step)
            az=finalBrg

            distt-=step
            out=str(linkid)+"|"+str(lon1)+"|"+str(lat1)+'|'+str(x)+"\n"

            temp.append(out)  
            x+=1

    io.writelines(temp)
    io.flush()
    io.close()
    
    print_message("Writing interpolated points to database...")
    io1=open(path+"/linknode","r")
    db.copyfrom(io1,"%s.%s"%(schema_name,nametable))
    io1.close()
            
    sql="SELECT AddGeometryColumn  ('%s','%s','geom',4326,'POINT',2); "%(schema_name,nametable)
    db.executeSql(sql,False,True)        
            
    sql="UPDATE %s.%s SET geom = \
    (ST_SetSRID(ST_MakePoint(long, lat),4326)); "%(schema_name,nametable)
    db.executeSql(sql,False,True)
    
    sql="alter table %s.%s drop column lat"%(schema_name,nametable)
    db.executeSql(sql,False,True)
    sql="alter table %s.%s drop column long"%(schema_name,nametable)
    db.executeSql(sql,False,True)
    
def destinationPointWGS(lat1,lon1,brng,s):
    a = 6378137
    b = 6356752.3142
    f = 1/298.257223563
    lat1=math.radians(float(lat1))
    lon1=math.radians(float(lon1))
    brg=math.radians(float(brng))
    
    
    sb=sin(brg)
    cb=cos(brg)
    tu1=(1-f)*tan(lat1)
    cu1=1/sqrt((1+tu1*tu1))
    su1=tu1*cu1
    s2=atan2(tu1, cb)
    sa = cu1*sb
    csa=1-sa*sa
    us=csa*(a*a - b*b)/(b*b)
    A=1+us/16384*(4096+us*(-768+us*(320-175*us)))
    B = us/1024*(256+us*(-128+us*(74-47*us)))
    s1=s/(b*A)
    s1p = 2*math.pi
    #Loop through the following while condition is true.
    while abs(s1-s1p) > 1e-12:
        cs1m=cos(2*s2+s1)
        ss1=sin(s1)
        cs1=cos(s1)
        ds1=B*ss1*(cs1m+B/4*(cs1*(-1+2*cs1m*cs1m)- B/6*cs1m*(-3+4*ss1*ss1)*(-3+4*cs1m*cs1m)))
        s1p=s1
        s1=s/(b*A)+ds1
    #Continue calculation after the loop.
    t=su1*ss1-cu1*cs1*cb
    lat2=atan2(su1*cs1+cu1*ss1*cb, (1-f)*sqrt(sa*sa + t*t))
    l2=atan2(ss1*sb, cu1*cs1-su1*ss1*cb)
    c=f/16*csa*(4+f*(4-3*csa))
    l=l2-(1-c)*f*sa* (s1+c*ss1*(cs1m+c*cs1*(-1+2*cs1m*cs1m)))
    d=atan2(sa, -t)
    finalBrg=d+2*math.pi
    backBrg=d+math.pi
    lon2 = lon1+l;
   # Convert lat2, lon2, finalBrg and backBrg to degrees
    lat2 =degrees( lat2) 
    lon2 = degrees(lon2 )
    finalBrg = degrees(finalBrg )
    backBrg = degrees(backBrg )
    #b = a - (a/flat)
    #flat = a / (a - b)
    finalBrg =(finalBrg+360) % 360;
    backBrg=(backBrg+360) % 360;
    
    return (lat2,lon2,finalBrg,backBrg)
    
def bearing(lat1,lon1,lat2,lon2):
    
    lat1=math.radians(float(lat1))
    lon1=math.radians(float(lon1))
    lat2=math.radians(float(lat2))
    lon2=math.radians(float(lon2))
    dLon=(lon2-lon1)
    
    y = math.sin(dLon) * math.cos(lat2);
    x = math.cos(lat1)*math.sin(lat2) - math.sin(lat1)*math.cos(lat2)*math.cos(dLon);
    brng = math.degrees(math.atan2(y, x))
          
    return (brng+360) % 360;

def print_message(msg):
    print msg
    print '-' * 80
    sys.stdout.flush()
    
def dbConnGrass(host,port,database,schema,user,password):
    print_message("Connecting to db-GRASS...")
    '''
    host = options['host']
    port = options['port']
    database = options['database']
    schema = options['schema']
    user = options['user']
    password = options['password']
    '''
    
    
    
    # Test connection
    conn = "dbname=" + database
    if host: conn += ",host=" + host
    if port: conn += ",port=" + port
    # Unfortunately we cannot test untill user/password is set
    if user or password:
        #print_message("Setting login (db.login) ... ")
        if grass.run_command('db.login', driver = "pg", database = conn, user = user, password = password) != 0:
            grass.fatal("Cannot login")

    # Try to connect

    if grass.run_command('db.select', quiet = True, flags='c', driver= "pg", database=conn, sql="select version()" ) != 0:
        if user or password:
            print_message( "Deleting login (db.login) ...")
            if grass.run_command('db.login', quiet = True, driver = "pg", database = conn, user = "", password = "") != 0:
                print_message("Cannot delete login.")
        grass.fatal("Cannot connect to database.")

    if grass.run_command('db.connect', driver = "pg", database = conn, schema = schema) != 0:
        grass.fatal("Cannot connect to database.")
    else:
        #print '-' * 80
        #print_message("Connected to...")
        grass.run_command('db.connect',flags='p')
    
def dbConnPy():
    print_message("Conecting to database by psycopg")
    db_host = options['host']
    db_name = options['database']
    db_user = options['user']
    db_password = options['password']
    
    try:
        #if require password and user    
        if db_password:        
            db = pg(dbname=db_name, host=db_host,
                    user=db_user, passwd = db_password)
        #if required only user
        elif db_user and not db_password:         
            db = pg(dbname=db_name, host=db_host,
                    user=db_user)
        #if not required user and passwd   
        else:
            db = pg(dbname=db_name, host=db_host)
            
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
    print_message("Prepare database for computing precipitation...")
    
    baseline_decibel= options['baseline']
    baseline_decibel=float(baseline_decibel)
    Aw=options['aw']
    Aw=float(Aw)
    
    #nuber of link and record in table link
    link_num=db.count("link")
    record_num=db.count("record")
    print_message("Number of records %s"%record_num)
    
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
    try:
        r= open(path+"/last_res","wr")
        r.write(temptb)
        r.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    
    #optimalization of commits
    db.setIsoLvl(0)
    #mesasure computing time
    st()
    
    try:
        io= open(path+"/precip","wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
        
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
    try:
        io.writelines(temp)
        io.flush
        io.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
        
    print_message("Write precipitation to database...")
    io=open(path+"/precip","r")
    db.copyfrom(io,"%s.%s"%(schema_name,temptb))
    io.close()
    
    #sql="drop table %s.%s"%(schema_name,temptb)
    #db.executeSql(sql,False,True) 
    st(False)
       
def sumPrecip(db,sumprecip,from_time,to_time):
    print_message("Creating time windows...")
    #@function sumPrecip make db views for all timestamps
    try:
        io=open(path+"/last_res",'r')
        temptb=io.read()
        io.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
            
    #make view per(->user) interval for all timestamp and for all links
    time_const=0
    if sumprecip=="minute":
        tc=60
    elif sumPrecip=="hour" :
        tc=3600
    else:
        tc=216000
        
    #summing values per (->user)timestep interval
    view_db="sum_"+randomWord(3)
    sql="CREATE %s %s.%s as select\
        linkid,(avg(precipitation))/%s as precip_mm_%s, date_trunc('%s',time)\
        as timestamp FROM %s.%s GROUP BY linkid, date_trunc('%s',time)\
        ORDER BY timestamp"%(view_statement, schema_name, view_db ,tc ,sumprecip ,sumprecip,schema_name,temptb ,sumprecip)    
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
     
    cur_timestamp=first_timestamp
    i=0

    try:
        io2=open(path+"/timewindow","wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
    temp=[]
    while str(cur_timestamp)!=str(last_timestamp):
        #crate name of view
        a=time.strftime("%Y_%m_%d_%H_%M", time.strptime(str(cur_timestamp), "%Y-%m-%d %H:%M:%S"))
        view_name="%s%s"%(view,a)
        #print "(timestamp'%s'+ %s * interval '1 second')" % (first_timestamp,time_const)
        vv=view_name+"\n"
        temp.append(vv)
        
        i+=1
        #create view
        sql="CREATE table %s.%s as select * from %s.%s where timestamp=(timestamp'%s'+ %s * interval '1 second')"%(schema_name,view_name,schema_name,view_db,first_timestamp,time_const)
        data=db.executeSql(sql,False,True)
        
        #go to next time interval
        time_const+=tc
        
        #compute cur_timestamp (need for loop)
        sql="select (timestamp'%s')+ %s* interval '1 second'"%(cur_timestamp,tc)
        cur_timestamp=db.executeSql(sql)[0][0]
        #print cur_timestamp
        #print last_timestamp
    
        
    #write values to flat file
    try:
        io2.writelines(temp)
        io2.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
    sql="drop table %s.%s"%(schema_name,view_db)
    db.executeSql(sql,False,True) 

def grassWork():
    #TODO
    print_message("GRASS analysis")
    host = options['host']
    port = options['port']
    database = options['database']
    schema = options['schema']
    user = options['user']
    password = options['password']
    
    mapset=options['mapset']
    
    try:
        io=open(path +"/linkpointsname","r")
        points=io.read()
        io.close
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    
  
    #zkousel jsem pripojit se k db pomoci funkce dbConnGrass() a pak v.in.ogr dsn='./' nejde to
    #dbConnGrass(host,port,database,schema_name,user,password)
    

    points_schema=schema_name+'.'+points
    points_ogr=points+"_ogr"
    print_message('v.in.ogr')
    
    dsn1="PG:host=localhost dbname="+database+" user="+user
    grass.run_command('v.in.ogr',
                    dsn=dsn1,
                    layer = points_schema,
                    output = points_ogr,
                    overwrite=True,
                    flags='t',
                    type='point')

    points_m=points_ogr+'@'+mapset
    points_nat=points + "_nat"
    
    #clear from before try
    rm=points_nat+'@'+mapset
    
    grass.run_command('g.remove',
                      vect=rm)
    
    print_message('v.category')
    grass.run_command('v.category',
                    input=points_m,
                    output=points_nat,
                    op="transfer",
                    overwrite=True,
                    layer="1,2")

    print_message('g.remove')
    grass.run_command('g.remove',
                      vect=points_m)   
 

   
    dbConnGrass(host,port,database,schema_name,user,password)
    points_nat_m=points_nat+'@'+mapset
    
    grass.run_command('v.db.connect',
                    map=points_nat_m,
                    table=points_schema,
                    key='linkid',
                    layer='1',
                    flags='o',
                    quiet=True)
    
    print_message('v.db.connect loop for')
    try:
        with open(path+"/timewindow",'r') as f:
            for win in f:
                #connect timewindow(precip    
                win=schema_name+'.'+win
                print_message(win)
                grass.run_command('v.db.connect',
                            map=points_nat_m,
                            table=win,
                            key='linkid',
                            layer='2',
                            overwrite=True,
                            flags='o',
                            quiet=True)
                
                #TODO(interpolace?)
                
                break# jen kvuli debug
                
                
                #delete connection to 2. layer
                grass.run_command('v.db.connect',
                            map=points_nat_m,
                            layer='2',
                            flags='d') 
              
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)


#------------------------------------------------------------------main-------------------------------------------    
def main():
    
    try: 
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
    
    
        print_message("Module is running...")
        db=dbConnPy()
        if flags['r']:
            sql="drop schema IF EXISTS %s CASCADE" % schema_name
            db.executeSql(sql,False,True)

        #first run- prepare db
        if flags['f']:
            firstRun(db)
       
        #compute precipitation
        if flags['c']:
            data=db.executeSql("CREATE SCHEMA %s" % schema_name,False,True)
            computePrecip(db)
           
        #make time windows
        if flags['m']:
            sumprecip=('minute', 'hour', 'day')
            sprc=sumprecip[0] 
            sumPrecip(db,sprc,fromtime,totime)
            
        #interpolate points    
        if flags['p']:
            if not options['step']:
                grass.fatal('Missing parameter "step" for interpolation')
            else:
                intrpolatePoints(db)
            
        #grass work
        if flags['i']:
            grassWork()
        
        
        #print first and last timestamp
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
            
        print_message('DONE')
    
if __name__ == "__main__":
    options, flags = grass.parser()

main()
