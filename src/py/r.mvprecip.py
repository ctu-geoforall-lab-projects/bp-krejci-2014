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
import shutil
import csv
from datetime import datetime
from pgwrapper import pgwrapper as pg
from math import sin, cos, atan2,degrees,radians, tan,sqrt

try:
    from grass.script import core as grass  
except ImportError:
    sys.exit("Cannot find 'grass' Python module. Python is supported by GRASS from version >= 6.4")


##########################################################
################## guisection: required ##################
##########################################################
#%module
#% description: Module for working with microwave links
#%end

#%option
#% key: database
#% type: string
#% key_desc : name
#% gisprompt: old_dbname,dbname,dbname
#% description: PotgreSQL database containing input data
#% guisection: Database
#% required : yes
#%end




##########################################################
############## guisection: Preprocessing #################
##########################################################

#%option
#% key: interval
#% description: Summing precipitation per
#% options: minute, hour, day
#% multiple: yes
#% guisection: Preprocessing
#% answer: minute
#%end

#%option 
#% key: fromtime
#% label: First timestamp "YYYY-MM-DD H:M:S"
#% description: Set first timestamp for create timewindows
#% type: string
#% guisection: Preprocessing
#%end

#%option 
#% key: totime
#% label: Last timestamp "YYYY-MM-DD H:M:S"
#% description: Set last timestamp in format for create timewindows
#% type: string
#% guisection: Preprocessing
#%end

#%option 
#% key: quantile
#% label: Quantile for compute baseline
#% description: Choose quantile in % 
#% type: integer
#% guisection: Preprocessing
#% answer: 96
#%end

#%option 
#% key: aw
#% label: Aw value
#% description: This options set Aw[dB] value for safety (see the manual)
#% type: double
#% guisection: Preprocessing
#% answer: 1.5
#%end

#%option 
#% key: step
#% label: Interpolation step per meter
#% description: Interpolate points along links per meter.
#% type: integer
#% guisection: Preprocessing
#% answer: 500
#%end

#%option G_OPT_F_INPUT 
#% key: baseline
#% label: Baseline value
#% description: Baseline in db in format "linkid;baseline"
#% guisection: Preprocessing
#% required: no
#%end


#%option G_OPT_F_INPUT
#% key: rgauges
#% label: Name of input rain gauges file
#% guisection: Preprocessing
#% required: no
#%end



##########################################################
################# guisection: Interpolation ##############
##########################################################

#%flag
#% key:g
#% description: Run GRASS analysis
#% guisection: Interpolation
#%end

#%option
#% key: interpolation
#% label: Type of interpolation
#% options: bspline, idw, rst
#% guisection: Interpolation
#% answer: rst
#%end


#%option 
#% key: isettings
#% label: Interpolation command string
#% description: Additional settings for choosen interpolation (see manual)
#% type: string
#% guisection: Interpolation
#%end









##########################################################
############## guisection: database work #################
##########################################################

#%option
#% key: user
#% type: string
#% label: User name
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
############### guisection: optional #####################
##########################################################
#%flag
#% key:p
#% description: Print info about timestamp(first,last) in db 
#%end

#%flag
#% key:r
#% description: Remove temporary working schema
#%end

#%flag
#% key:t
#% description: Remove temporary data folder
#%end

#%flag
#% key:q
#% description: Do not set region from modul settings
#%end

#%option
#% key: schema
#% type: string
#% label: Name of db schema for results
#% answer: temp4
#%end




#EXAMPLE
#r.mvprecip.py -g database=letnany baseline=1 fromtime=2013-09-10 04:00:00 totime=2013-09-11 04:00:00 schema=temp6


path=''
view="view"
view_statement = "TABLE"
  #new scheme, no source
record_tb_name= "record"
comp_precip="computed_precip"
R=6371
mesuretime=0
restime=0
temp_windows_names=[]


def readBaselineFromText(pathh):

    with open(pathh, mode='r') as infile:
        reader = csv.reader(infile,delimiter=';')
        mydict = {float(rows[0]):float(rows[1]) for rows in reader}
    return mydict

    
def readRaingaugesCsv():
    rgpath=options['rgauges']
    print_message(rgpath)
    try:
        with open(rgpath, 'rb') as f:
            csv.register_dialect('mydial', delimiter=';',skipinitialspace=True)
            reader = csv.reader(f, 'mydial')
            try: 
                for row in reader:
                    print row
                    #for val in row:        
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (rgpath, reader.line_num, e))   
                
  
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
       
def computeBaseline(db):
        quantile=options['quantile']
        link_num=db.count("link")               
        sql="SELECT linkid from link"
        linksid=db.executeSql(sql,True,True)
        tmp=[]
         #for each link  compute baseline
        for linkid in linksid:         
            linkid=linkid[0]
            sql="Select\
            max(a) as maxAmount\
            , avg(a) as avgAmount\
            ,quartile\
            FROM (SELECT a, ntile(%s) over (order by a) as quartile\
            FROM record where linkid=%s ) x\
            GROUP BY quartile\
            ORDER BY quartile\
            limit 1"%(quantile,linkid)
            resu=db.executeSql(sql,True,True)[0][0]
            #print_message(resu)
            tmp.append(str(linkid)+';'+ str(resu)+'\n')

        try:
            io0=open(os.path.join(path,"baseline"),"wr")
            io0.writelines(tmp)
            io0.close()
        except IOError as (errno,strerror):
            print "I/O error({0}): {1}".format(errno, strerror)

        try:
            io1=open(os.path.join(path,"compute_precip_info"),"wr")
            io1.write('quantile'+ quantile+'|'+options['aw'])
            io1.close
        except IOError as (errno,strerror):
            print "I/O error({0}): {1}".format(errno, strerror)




    
def precipInterpolationCustom(points_nat,win):
    
    itype=options['interpolation']
    attribute_col='precip_mm_' + options['interval']
    out=win + '_' + itype
    istring=options['isettings']
    
    if itype == 'rst':
        #grass.run_command('v.surf.rst',input=points_nat,zcolumn = attribute_col,elevation=out, overwrite=True)
        eval(istring)
    elif itype == 'bspline':
         a
         grass.run_command('v.surf.bspline',

                           input=points_nat,
                           column = attribute_col,
                           raster_output=out,
                           overwrite=True)
    else:
         grass.run_command('v.surf.idw',
                           eval(istring),
                           input=points_nat,
                           column = attribute_col,
                           output=out,
                           overwrite=True)        
       
def precipInterpolationDefault(points_nat,win):
    itype=options['interpolation']
    attribute_col='precip_mm_' + options['interval']
    out=win + '_' + itype
    
    if itype == 'rst':
        grass.run_command('v.surf.rst',
                          input=points_nat,
                          zcolumn = attribute_col,
                          elevation=out,
                          overwrite=True)
        
    elif itype == 'bspline':
         grass.run_command('v.surf.bspline',
                           input=points_nat,
                           column = attribute_col,
                           raster_output=out,
                           overwrite=True)
    else:
         grass.run_command('v.surf.idw',
                           input=points_nat,
                           column = attribute_col,
                           output=out,
                           overwrite=True)        

def isAttributExist(db,schema,table,columns):
        sql="SELECT EXISTS( SELECT * FROM information_schema.columns WHERE \
         table_schema = '%s' AND \
         table_name = '%s' AND\
         column_name='%s');"%(schema,table,columns)       
        return db.executeSql(sql,True,True)[0][0]  
            
def isTableExist(db,schema,table):
    sql="SELECT EXISTS( SELECT * \
         FROM information_schema.tables \
         WHERE \
         table_schema = '%s' AND \
         table_name = '%s');"%(schema,table)
    return db.executeSql(sql,True,True)[0][0]
    
def intrpolatePoints(db):
    
    print_message("Interpolating points along lines...")
    schema_name = options['schema']
    step=options['step'] #interpolation step per meters
    step=float(step)
    sql="select ST_AsText(link.geom),ST_Length(link.geom,false), linkid from link" 
    resu=db.executeSql(sql,True,True) #values from table link include geom, lenght and linkid
    

    nametable="linkpoints"+str(step).replace('.0','')           #create name for table with interopol. points.
    sql="DROP TABLE IF EXISTS %s.%s"%(schema_name,nametable)    #if table exist than drop
    db.executeSql(sql,False,True)
    
    try: #open file for interpol. points.
        io= open(os.path.join(path,"linkpointsname"),"wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    io.write(nametable)
    io.close
    
    sql="create table %s.%s (linkid integer,long real,lat real,point_id serial PRIMARY KEY) "%(schema_name,nametable)   #create table where will be intrpol. points.
    db.executeSql(sql,False,True)

    latlong=[] #list of  [lon1 lat1 lon2 lat2]
    dist=[]    #list of distances between nodes on one link
    linkid=[]   #linkid value
    
    a=0 #index of latlong row
    x=0 #just id in table with interpol. points (currently not necessery)
     
    try:
        io= open(os.path.join(path, "linknode"), "wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
    temp=[]
    for record in resu:
        tmp=record[0]
        tmp = tmp.replace("LINESTRING(", "") ### ???
        tmp = tmp.replace(" ", ",")
        tmp = tmp.replace(")", "")
        tmp = tmp.split(",")
        
        latlong.append(tmp)# add [lon1 lat1 lon2 lat2] to list latlong
        
        lon1=latlong[a][0]
        lat1=latlong[a][1]
        lon2=latlong[a][2]
        lat2=latlong[a][3]
        
        dist=record[1]      #distance between nodes on current link
        linkid=record[2]    #linkid value
    
        az=bearing(lat1,lon1,lat2,lon2) #compute approx. azimut on sphere
        a+=1
        while abs(dist) > step:         #compute points per step while is not achieve second node on link
            lat1 ,lon1, az, backBrg=destinationPointWGS(lat1,lon1,az,step)  #return interpol. point and set current point as starting point(for next loop), also return azimut for next point
            dist-=step  #reduce distance

            out=str(linkid)+"|"+str(lon1)+"|"+str(lat1)+'|'+str(x)+"\n" # set string for one row in table wit interpol points
            temp.append(out)
            x+=1

    io.writelines(temp)            #write interpolated points to flat file
    io.flush()
    io.close()
    
    print_message("Writing interpolated points to database...")
    io1=open(os.path.join(path,"linknode"),"r")         
    db.copyfrom(io1,"%s.%s"%(schema_name,nametable))        #write interpoalted points to database from temp flat file
    io1.close()
            
    sql="SELECT AddGeometryColumn  ('%s','%s','geom',4326,'POINT',2); "%(schema_name,nametable) #add geometry column for computed interpolated points
    db.executeSql(sql,False,True)        
            
    sql="UPDATE %s.%s SET geom = \
    (ST_SetSRID(ST_MakePoint(long, lat),4326)); "%(schema_name,nametable)       #make geometry for computed interpolated points
    db.executeSql(sql,False,True)
    
    sql="alter table %s.%s drop column lat"%(schema_name,nametable) #remove latitde column from table
    db.executeSql(sql,False,True)
    sql="alter table %s.%s drop column long"%(schema_name,nametable) #remove longtitude column from table
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
    grass.message(msg)
    grass.message('-' * 60)
    
def dbConnGrass(database,user,password):
    print_message("Connecting to db-GRASS...")
    # Unfortunately we cannot test untill user/password is set
    if user or password:
        #print_message("Setting login (db.login) ... ")
        if grass.run_command('db.login', driver = "pg", database = database, user = user, password = password) != 0:
            grass.fatal("Cannot login")

    # Try to connect
    if grass.run_command('db.select', quiet = True, flags='c', driver= "pg", database=database, sql="select version()" ) != 0:
        if user or password:
            print_message( "Deleting login (db.login) ...")
            if grass.run_command('db.login', quiet = True, driver = "pg", database = database, user = "", password = "") != 0:
                print_message("Cannot delete login.")
        grass.fatal("Cannot connect to database.")

    if grass.run_command('db.connect', driver = "pg", database = database) != 0:
        grass.fatal("Cannot connect to database.")
    else:
        #print '-' * 80
        #print_message("Connected to...")
        grass.run_command('db.connect',flags='p')

def dbConnPy():
    print_message("Connecting to database by Psycopg driver...")
    db_name = options['database']
    db_user = options['user']
    db_password = options['password']
    
    try:
        conninfo = { 'dbname' : db_name }
        if db_user:
            conninfo['user'] = db_user
        if db_password:
            conninfo['passwd'] = db_password
            
        db = pg(**conninfo)

    except psycopg2.OperationalError, e:
        grass.fatal("Unable to connect to the database <%s>. %s" % (db_name, e))
    
    return db
    
def firstRun(db):
        print_message("Preparing database...")
        
        sql="CREATE EXTENSION postgis;"
        db.executeSql(sql,False,True)
        print_message("1/16")
        sql="SELECT AddGeometryColumn   ('public','node','geom',4326,'POINT',2); "
        db.executeSql(sql,False,True)
        print_message("2/16")
        sql="SELECT AddGeometryColumn  ('public','link','geom',4326,'LINESTRING',2); "
        db.executeSql(sql,False,True)
        print_message("3/16")
        sql="UPDATE node SET geom = ST_SetSRID(ST_MakePoint(long, lat), 4326); "
        db.executeSql(sql,False,True)
        print_message("4/16")
        sql="UPDATE link SET geom = st_makeline(n1.geom,n2.geom) \
        FROM node AS n1 JOIN link AS l ON n1.nodeid = fromnodeid JOIN node AS n2 ON n2.nodeid = tonodeid WHERE link.linkid = l.linkid; "
        db.executeSql(sql,False,True)
        print_message("5/16")
        sql="alter table record add column polarization char(1); "
        db.executeSql(sql,False,True)
        print_message("6/16")
        sql="alter table record add column lenght real; "
        db.executeSql(sql,False,True)
        print_message("7/16")
        sql="alter table record add column precipitation real; "
        db.executeSql(sql,False,True)
        print_message("8/16")
        sql="alter table record add column a real; "
        db.executeSql(sql,False,True)
        print_message("9/16")
        sql="update record set a= rxpower-txpower;  "
        db.executeSql(sql,False,True)
        print_message("10/16")
        sql="update record  set polarization=link.polarization from link where record.linkid=link.linkid;"
        db.executeSql(sql,False,True)
        print_message("11/16")
        sql="update record  set lenght=ST_Length(link.geom,false) from link where record.linkid=link.linkid;"
        db.executeSql(sql,False,True)
        print_message("12/16")
        sql="CREATE SEQUENCE serial START 1; "
        db.executeSql(sql,False,True)
        print_message("13/16")
        sql="alter table record add column recordid integer default nextval('serial'); "
        db.executeSql(sql,False,True)
        print_message("14/16")
        sql="CREATE INDEX idindex ON record USING btree(recordid); "
        db.executeSql(sql,False,True)
        print_message("15/16")
        sql="CREATE INDEX timeindex ON record USING btree (time); "
        db.executeSql(sql,False,True)
        print_message("16/16")

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
    print_message("Preparing database for computing precipitation...")
    
    schema_name = options['schema']
    Awx=options['aw']
    Aw=float(Awx)
    
##nuber of link and record in table link
    xx=db.count("record")
    link_num=db.count("link")
##select values for computing
    sql=" select time, a,lenght,polarization,frequency,linkid from %s order by recordid limit %d ; "%(record_tb_name, xx)
    resu=db.executeSql(sql,True,True)
    
    sql="create table %s.%s ( linkid integer,time timestamp, precipitation real);"%(schema_name,comp_precip)
    db.executeSql(sql,False,True)
    
#save name of result table for next run without compute precip
    
##optimalization of commits
    db.setIsoLvl(0)

    try:
        io= open(os.path.join(path,"precip"),"wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    
    recordid = 1
    temp= []
    print_message("Computing precipitation...")

    mydict={}
    if not options['baseline']:
        print_message('Compute bselines...')
        computeBaseline(db)
        links_dict=readBaselineFromText(os.path.join(path,'baseline'))
    else:
        print_message('Read baseline from file...')
        links_dict=readBaselineFromText(options['baseline'])
        try:
            io1=open(os.path.join(path,"compute_precip_info"),"wr")
            io1.write('fromfile|'+options['aw'])
            io1.close
        except IOError as (errno,strerror):
            print "I/O error({0}): {1}".format(errno, strerror)

##check if baseline from text is correct
    if len(links_dict)<link_num:
        print_message("Missing baseline or linkid in text file")
        sys.exit()
        
    for record in resu:
        #coef_a_k[alpha, k]
        coef_a_k= computeAlphaK(record[4],record[3])
        #read value from dictionary
        baseline_decibel=(links_dict[record[5]])
        #final precipiatation is R1    
        Ar=record[1]- baseline_decibel - Aw
        if Ar > 0.0001:
            yr=Ar/record[2]  
            R1=(yr/coef_a_k[1])**(1/coef_a_k[0])
        else: R1=0
        #string for output flatfile
        out=str(record[5])+"|"+str(record[0])+"|"+str(R1)+"\n"
        temp.append(out)
        recordid += 1
        
##write values to flat file
    try:
        io.writelines(temp)
        io.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
    print_message("Writing precipitation to database...")
    io1=open(os.path.join(path,"precip"),"r")
    db.copyfrom(io1,"%s.%s"%(schema_name,comp_precip))
    io1.close()
    os.remove(os.path.join(path,"precip"))
       
def makeTimeWin(db):
    print_message("Creating time windows...")
    #@function sumPrecip make db views for all timestamps
    from_time=''
    to_time=''
    schema_name = options['schema']
    if options['fromtime']:
        from_time= datetime.strptime(options['fromtime'], "%Y-%m-%d %H:%M:%S")
    if options['totime']:    
        to_time=datetime.strptime(options['totime'], "%Y-%m-%d %H:%M:%S")
    sum_precip=options['interval']

    if sum_precip=="minute":
        tc=60
        tcc=60
    elif sum_precip=="hour" :
        tc=1
        tcc=3600
    else:
        tc=1/24
        tcc=216000
       
    #summing values per (->user)timestep interval
    view_db="sum_"+randomWord(3)
    sql="CREATE %s %s.%s as select\
        linkid,round(avg(precipitation)::numeric,3) as precip_mm_%s, date_trunc('%s',time)\
        as timestamp FROM %s.%s GROUP BY linkid, date_trunc('%s',time)\
        ORDER BY timestamp"%(view_statement, schema_name, view_db  ,sum_precip ,sum_precip,schema_name,comp_precip ,sum_precip)    
    data=db.executeSql(sql,False,True)
     
    #num of rows of materialized view
    record_num=db.count("%s.%s"%(schema_name,view_db))


##set first and last timestamp
    #get first timestamp
    sql="select timestamp from %s.%s limit 1"%(schema_name,view_db)
    timestamp_min=db.executeSql(sql)[0][0]
        
    #get last timestep
    sql="select timestamp from  %s.%s offset %s"%(schema_name,view_db,record_num-1)
    timestamp_max=db.executeSql(sql)[0][0]
    if options['fromtime']:        
        if timestamp_min <from_time:
            timestamp_min=from_time
    if options['totime']:       
        if timestamp_max > to_time: 
            timestamp_max=to_time


    #save first and last timewindow to file. On first line file include time step "minute","hour"etc
    try:
        io1=open(os.path.join(path,"time_window_info"),"wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
    io1.write(sum_precip+'|'+str(timestamp_min)+'|'+str(timestamp_max))
    io1.close    
        
        
    #open file for saveing names of timewindows 
    try:
        io2=open(os.path.join(path,"timewindow"),"wr")
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
                
    time_const=0    
    i=0
    temp=[]
    cur_timestamp=timestamp_min
    while str(cur_timestamp)!=str(timestamp_max):
        #crate name of view
        a=time.strftime("%Y_%m_%d_%H_%M", time.strptime(str(cur_timestamp), "%Y-%m-%d %H:%M:%S"))
        view_name="%s%s"%(view,a)
        #print "(timestamp'%s'+ %s * interval '1 second')" % (timestamp_min,time_const)
        vv=view_name+"\n"
        temp.append(vv)
        
        i+=1
        #create view
        sql="CREATE table %s.%s as select * from %s.%s where timestamp=(timestamp'%s'+ %s * interval '1 second')"%(schema_name,view_name,schema_name,view_db,timestamp_min,time_const)
        data=db.executeSql(sql,False,True)
        
        #go to next time interval
        time_const+=tcc
        
        #compute cur_timestamp (need for loop)
        sql="select (timestamp'%s')+ %s* interval '1 second'"%(cur_timestamp,tcc)
        cur_timestamp=db.executeSql(sql)[0][0]
        #print cur_timestamp
        #print timestamp_max
    
    #write values to flat file
    try:
        io2.writelines(temp)
        io2.close()
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        
    sql="drop table %s.%s"%(schema_name,view_db)
    #db.executeSql(sql,False,True) 

def grassWork():
    #TODO
    print_message("GRASS analysis")
    database = options['database']
    user = options['user']
    password = options['password']
    mapset=grass.gisenv()['MAPSET']
    schema_name = options['schema']
    
    dbConnGrass(database,user,password)
                
    try:
        io=open(os.path.join(path,"linkpointsname"),"r")
        points=io.read()
        io.close
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
         
    points_schema=schema_name+'.'+points
    points_ogr=points+"_ogr"
    
    dsn1="PG:dbname="+database+" user="+user
    
    print_message('v.in.ogr')
    grass.run_command('v.in.ogr',
                    dsn="PG:",
                    layer = points_schema,
                    output = points_ogr,
                    overwrite=True,
                    flags='tl',
                    type='point')
    
    points_nat=points + "_nat"
   
    # if vector already exits, remove dblink (original table)
    if grass.find_file(points_nat, element='vector')['fullname']:
        print_message('remove link to layer 1 and 2')
        grass.run_command('v.db.connect',
                          map=points_nat,
                          flags='d',
                          layer='1')
        grass.run_command('v.db.connect',
                          map=points_nat,
                          flags='d',
                          layer='2')
        
    print_message('v.category')
    grass.run_command('v.category',
                    input=points_ogr,
                    output=points_nat,
                    option="transfer",
                    overwrite=True,
                    layer="1,2")
    
    print_message('v.db.connect')
    grass.run_command('v.db.connect',
                    map=points_nat,
                    table=points_schema,
                    key='linkid',
                    layer='2',
                    quiet=True)
    
    if not flags['q']:
        grass.run_command('g.region',
                          vect=points_ogr,
                          n='n+00:00:01',
                          s='s+00:00:01',
                          e='e+00:00:01',
                          w='w+00:00:01',
                          res='00:00:01')
    
    try:
        with open(os.path.join(path,"timewindow"),'r') as f:            
            for win in f.read().splitlines():
                
                win=schema_name + '.' + win
                grass.run_command('v.db.connect',
                                map=points_nat,
                                table=win,
                                key='linkid',
                                layer='1',
                                quiet=True)
                
                precipInterpolationDefault(points_nat,win)
                
                if options['isettings']:
                    precipInterpolationCustom(points_nat,win)
                else:
                    precipInterpolationDefault(points_nat,win)
                    
                #remove connection to 2. layer
                grass.run_command('v.db.connect',
                                map=points_nat,
                                layer='1',
                                flags='d')
                
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)

############################ main ############################
def main():
        print_message("Module is running...")
        schema_name = options['schema']
        global path
        path= os.path.join(os.path.dirname(os.path.realpath(__file__)), "tmp_%s"%schema_name)

        try: 
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise
##connect to database by python lib psycopg
        db=dbConnPy()
        
#check database if is prepare
        if not isAttributExist(db,'public','link','geom'):
            firstRun(db)
            
#read rain gagues file             
        if options['rgauges']:
            sys.exit()
            readRaingaugesCsv()
            
            
##print first and last timestamp
        if flags['p']:
            sql="create view tt as select time from %s order by time"%record_tb_name
            db.executeSql(sql,False,True)
            #get first timestamp
            sql="select time from tt limit 1"
            timestamp_min=db.executeSql(sql,True,True)[0][0]
            print_message('First timestamp is %s'%timestamp_min)
            record_num=db.count(record_tb_name)
            
            #get last timestep
            sql="select time from  tt offset %s"%(record_num-1)
            timestamp_max=db.executeSql(sql,True,True)[0][0]
            print_message('Last timestamp is %s'%timestamp_max)
            sql="drop view tt"
            db.executeSql(sql,False,True)
            sys.exit()
        
##drop working schema
        if flags['r']:
            sql="drop schema IF EXISTS %s CASCADE" % schema_name
            db.executeSql(sql,False,True)
            print_message("Schema removed")
            sys.exit()
            
##remove temp folder
        if flags['t']:
            shutil.rmtree(path)
            print_message("Folder removed")
            sys.exit()
        
##compute precipitation
        curr_precip_config="null"        
        try:
            io0=open(os.path.join(path,"compute_precip_info"),"r")
            try:
                curr_precip_config=io0.readline()
                io0.close() 
            finally:
                    io0.close()
        except IOError:
                pass
            
        quantil='quantile'+ options['quantile']+'|'+options['aw']
        new_precip_config='fromfile|'+options['aw']
        
        compute=False
        if (curr_precip_config!=new_precip_config) and  options['baseline']:
            compute=True
            print_message('1')
        if not (isTableExist(db,schema_name,comp_precip)):
            compute=True
            print_message('2')
        if  quantil!=curr_precip_config and not options['baseline']:
            compute=True
            print_message(quantil)
            print_message(new_precip_config)
            print_message('3')
   
        if compute:  
            sql="drop schema IF EXISTS %s CASCADE" % schema_name
            shutil.rmtree(path)
            os.makedirs(path)
            db.executeSql(sql,False,True)
            sql="CREATE SCHEMA %s"% schema_name
            db.executeSql(sql,False,True)
            computePrecip(db)
        sys.exit()   
##make time windows
        curr_timewindow_config="null"
        try:
            io1=open(os.path.join(path,"time_window_info"),"r")
            curr_timewindow_config=io1.readline()
            io1.close
        except:
            pass
        #compare current and new settings   
        new_timewindow_config=options['interval']+'|'+options['fromtime']+'|'+options['totime']
        if curr_timewindow_config!=new_timewindow_config:
            intervals = options['interval'].split(',')
            if ',' in str(options['interval']):
                grass.fatal('You can choose only one method (minute, hour, day)')
    
            print_message('Removing time windows...')
            if os.path.exists(os.path.join(path,"timewindow")):
                with open(os.path.join(path,"timewindow"),'r') as f:
                    for win in f.read().splitlines():
                        sql="drop table IF EXISTS %s.%s "%(schema_name,win)
                        db.executeSql(sql,False,True)
            makeTimeWin(db)
                      
##interpol. points          
        step=options['step'] #interpolation step per meters
        step=float(step)
        new_table_name="linkpoints"+str(step).replace('.0','')
        curr_table_name='null'
        try:
            io2=open(os.path.join(path,"linkpointsname"),"r")
            curr_table_name=io2.readline()
            io2.close
        except:
            pass
        #check if table exist or if exist with different step or if -> interpol. one more time   
        if not (isTableExist(db,schema_name,curr_table_name) or new_table_name!=curr_table_name):                    
            if not options['step']:
                grass.fatal('Missing value for "step" for interpolation')
            intrpolatePoints(db)
   
##grass work
        if flags['g']:
            grassWork()
            


        print_message('DONE')
    
if __name__ == "__main__":
    options, flags = grass.parser()

main()
