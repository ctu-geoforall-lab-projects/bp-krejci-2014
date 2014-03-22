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

'''
first start hint 
psql createdb name
psql psql letnany < /path/to/sql/dump 
'''
first_fun=True #first run, make geometry, prepare columns



mesuretime=0
restime=0
# Import our wrapper.
from pgwrapper import pgwrapper as pg

#view_statement = "MATERIALIZED VIEW"
view_statement = "TABLE"
schema_name = "temp"  #new scheme, no source
fromtime= "2013-09-08 23:59:00"
totime="2013-09-09 00:03:00"
record_tb_name= "record"
recalculate_precitp=False


#------------------------------------------------------------------functions-------------------------------------------






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

def computePrecip(db,baseline_decibel,Aw):
    #nuber of link in table link
    link_num=db.count("link")
    
    record_num=db.count("record")
    print "num of record"
    print record_num
    
    #create view of record sorting by time asc!
    db_view=randomWord(5)
    #print "name of view %s"%db_view
    #db_view="testT"
    

    
    sql="CREATE %s %s.%s AS SELECT * from %s ORDER BY time::date asc ,time::time asc; "% (view_statement, schema_name,db_view,record_tb_name)
    db.executeSql(sql,False,True)
    
    xx=1000
    sql=" select time,a,lenght,polarization,frequency from %s.%s order by recordid limit %d ; "%(schema_name,db_view, xx)
    resu=db.executeSql(sql,True,True)
    
     #optimalization of commit
    db.setIsoLvl(0)
    temptb=randomWord(5)
    sql="create table %s.%s (precipitation real ,recordid integer);"%(schema_name,temptb)
    db.executeSql(sql,False,True)
    #mesasure computing time
    st()
    #open blankfile
    io= open("precip","wr")
    
    recordid = 1
    temp= []
    for record in resu:
    #coef_a_k[alpha, k]
        coef_a_k= computeAlphaK(record[4],record[3])
    #final precipiatation is R1    
        Ar=(-1)*record[1]-baseline_decibel-Aw
        yr=Ar/record[2]
        
        alfa=1/coef_a_k[1]
        beta=1/coef_a_k[0]
        R1=(yr/beta)**(1/alfa)
        #print "R1 %s"%R1
        out=str(R1)+"|"+str(recordid)+"\n"
        
        temp.append(out)
        recordid += 1
        
   
    io.writelines(temp)
    io.close()
    
    io=open("precip","r")
    db.copyfrom(io,"test_copy")
    io.close()
    
    sql="update %s set precipitation= %s.%s.precipitation from %s.%s where record2.recordid=%s.%s.recordid"%(record_tb_name,schema_name,temptb,schema_name,temptb,schema_name,temptb)
    db.executeSql(sql,False,True)
    st(False)
    
    
    print 'AMD Phenom X3 ocek cas minut %s'%((record_num * (restime / xx)) / 60)
    #sql="DROP %s %s"% (view_statement, db_view);
    #db.executeSql(sql,False,True)
    sql="drop table %s.%s"%(schema_name,db_view)
    db.executeSql(sql,False,True)
    sql="drop table %s.%s"%(schema_name,temptb)
    db.executeSql(sql,False,True)
    
def sumPrecip(db,sumprecip,from_time,to_time):
    #@function sumPrecip make db views for all timestamps
    view_db="matviewfortimestamp"
    
    #summing values per (->user)timestep interval
    
    sql="CREATE %s %s as select\
        linkid,sum(precipitation)as x,count(precipitation) as xx,date_trunc('%s',time)\
        as timestamp FROM %s GROUP BY linkid,date_trunc('%s',time)\
        ORDER BY timestamp"%(view_statement, view_db,sumprecip,record_tb_name,sumprecip)
    
    data=db.executeSql(sql,False,True)
   
    #num of rows of materialized view
    record_num=db.count(view_db)
    
    #the user's choice or all record 
    if from_time and to_time:
        first_timestamp=from_time
        last_timestamp=to_time
    else:    
        #get first timestamp
        sql="select timestamp from %s limit 1"%view_db
        first_timestamp=db.executeSql(sql)[0][0]
        
        #get last timestep
        sql="select timestamp from %s offset %s"%(view_db,record_num-1)
        last_timestamp=db.executeSql(sql)[0][0]
    
    #make view per(->user) interval for all timestamp, for all links
    time_const=0
    if sumprecip=="minute":
        tc=60
    elif sumprecip=='second':
        tc=0
    elif sumPrecip=="hour" :
        tc=3600
    else:
        tc=216000
        
    cur_timestamp=first_timestamp

    
    
    while cur_timestamp!=last_timestamp:
        a=time.strftime("%Y_%m_%d_%H_%M", time.strptime(str(cur_timestamp), "%Y-%m-%d %H:%M:%S"))
        view_name="view%s"%a
        print "(timestamp'%s'+ %s * interval '1 second')" % (first_timestamp,time_const)
        sql="CREATE VIEW %s.%s as select * from %s where timestamp=(timestamp'%s'+ %s * interval '1 second')"%(schema_name,view_name,view_db,first_timestamp,time_const)
        data=db.executeSql(sql,False,True)
        
        #go to next time interval
        time_const+=tc
        
        #compute cur_timestamp (need for loop while)
        sql="select (timestamp'%s')+ %s* interval '1 second'"%(cur_timestamp,tc)
        cur_timestamp=db.executeSql(sql)[0][0]   
    
#------------------------------------------------------------------main-------------------------------------------    
def main():
    #parser = argparse.ArgumentParser ()
    #group1 = parser.add_argument_group('group1', 'group database')
    #group1.add_argument("host", help = 'database host')
    #group1.add_argument("db_name", help = 'database name')
    #group1.add_argument("db_schema", help = 'database schema')
    #group1.add_argument("port", help = 'port')
    #group1.add_argument("user", help = 'database user')
    #group1.add_argument("password", help = 'database password')
    
    #group2 = parser.add_argument_group('group2', 'group precipitation')
    #group2.add_argument("sumprecip", help = 'summing interval of precipitation in ['second','minute', 'hour', 'day', 'week', 'month', 'quarter'] write string!!)
    #group2.add_argument("baseline_decibel", help = 'baseline of frequency for compute precip')
    #group2.add_argument("freq_const", help = 'minus constant in [decibel] ')
    
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
    

    try:
        #if required password and user    
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
            db = pg(dbname=db_name, host=db_host,
                    user=db_user)
            print "not required user and passwd"
    except psycopg2.OperationalError, e:
        sys.exit("I am unable to connect to the database (db=%s, user=%s). %s" % (db_name, db_user, e))
        
    
    
    
    
    if first_fun:
        sql="CREATE EXTENSION postgis;"
        db.executeSql(sql,False,True)
        sql="Enable Topology CREATE EXTENSION postgis_topology;"
        db.executeSql(sql,False,True)
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
        sql="alter table record add column id integer PRIMARY KEY DEFAULT nextval('oid'); " 
        db.executeSql(sql,False,True)
        sql="CREATE SEQUENCE serial START 1; "
        db.executeSql(sql,False,True)
        sql="alter table record add column id integer PRIMARY KEY DEFAULT nextval('serial'); "
        db.executeSql(sql,False,True)
        sql="CREATE INDEX idindex ON record USING btree(recordid); "
        db.executeSql(sql,False,True)
        sql="CREATE INDEX timeindex ON record USING btree (time); "
        db.executeSql(sql,False,True)
        
        
        
    sumprecip=('second','minute', 'hour', 'day')
    sprc=sumprecip[1]
    baseline_decibel=1
    Aw=1.5
   #compute precipitation
    data=db.executeSql("CREATE SCHEMA %s" % schema_name,False,True)
    if recalculate_precitp:
       computePrecip(db,baseline_decibel,Aw)
    
    #sumPrecip(db,sprc,fromtime,totime)
 
    
    
main()
