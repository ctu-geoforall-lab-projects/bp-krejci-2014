#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random
sys.path.append('/home/matt/Dropbox/BC/python/komodo/postgrespy/pgwrapper.py') 
# Import our wrapper.
from pgwrapper import pgwrapper as pg
#------------------------------------------------------------------functions-------------------------------------------    

def randomWord(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def alphaBeta():    
#Coefficients for kH    1
    aj_kh=[-5.33980,-0.35351,-0.23789,-0.94158]
    bj_kh=[-0.10008,1.26970,0.86036,0.64552]
    cj_kh=[1.13098,0.45400,0.15354,0.16817]
    mk_kh=-0.18961
    ck_kh=0.71147
#Coefficients for kV    2
    aj_kv=[-3.80595,-3.44965,-0.39902,0.50167]
    bj_kv=[0.56934,-0.22911,0.73042,1.07319]
    cj_kv=[0.81061,0.51059,0.11899,0.27195]
    mk_kv=-0.16398
    ck_kv=0.63297
#Coefficients for αH    3
    av_ah=[-0.14318, 0.29591, 0.32177,-5.37610,16.1721]
    bj_ah=[1.82442, 0.77564, 0.63773,-0.96230,-3.29980]
    cj_ah=[-0.55187, 0.19822, 0.13164,1.47828,3.43990]
    ma_ah=0.67849
    ca_ah=-1.95537
#Coefficients for α V   4
    av_av=[-0.07771, 0.56727, -0.20238,-48.2991,48.5833]
    bj_av=[2.33840, 0.95545, 1.14520,0.791669,0.791459]
    cj_av=[-0.76284, 0.54039, 0.26809,0.116226,0.116479]
    ma_av=-0.053739
    ca_av=-0.83433

def computePrecip(db,baseline_decibel,Aw):
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
    table=randomWord(5)
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