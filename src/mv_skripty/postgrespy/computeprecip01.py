#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random
import math
import timeit 
import time
mesuretime=0
restime=0
# Import our wrapper.
from pgwrapper import pgwrapper as pg
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
    '''
    #create temporaly table of record
    db_temp="temp"
    sql="CREATE TABLE temp AS SELECT * FROM record"
    data=(db_temp,)
    db.executeSql(sql,False)
    '''
    #nuber of link in table link
    #print "num of link"
    link_num=db.count("link")
    #print link_num
    
    #nuber of records in table record
    
    record_num=db.count("record")
    print "num of record"
    print record_num
    
    
    #create view of record sorting by time asc!
    db_view=randomWord(5)
    #print "name of view %s"%db_view 
    sql="CREATE MATERIALIZED VIEW %s AS SELECT * from record ORDER BY time::date asc ,time::time asc; "%db_view
    db.executeSql(sql,False)
    
    #db_view="ygyjb"
    st()
#loop compute precip for each rows in table record()
    xx=10
    for record in range(0,xx):
        
        sql="select time,(rxpower-txpower)as a,lenght,polarization,frequency from %s OFFSET %s limit 1 ; "%(db_view,record)
        resu=db.executeSql(sql)
        '''
        a=resu[0][1]
        length=resu[0][2]
        polarization=resu[0][3]
        freq=resu[0][4]
        '''
        time1=resu[0][0]
        print time1
    #coef_a_k[alpha, k]
        coef_a_k= computeAlphaK(resu[0][4],resu[0][3])
    #final precipiatation is R1    
        Ar=(-1)*resu[0][1]-baseline_decibel-Aw
        yr=Ar/resu[0][2]
        
        alfa=1/coef_a_k[1]
        beta=1/coef_a_k[0]
        R1=(yr/beta)**(1/alfa)
        #print "R1 %s"%R1
        
        sql="UPDATE record SET precipitation ='%s' where time='%s';"%(R1,resu[0][0])
        #print "sql %s"%sql
        db.executeSql(sql,False) 

    st(False)
    print 'AMD Phenom X3 ocek cas hodin %s'%((record_num*restime/xx)/3600)
    #sql="DROP MATERIALIZED VIEW %s"%db_view;
    #db.executeSql(sql,False)
    
 
#def sumPrecip(sumprecip):
    
  
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
    #group2.add_argument("sumprecip", help = 'summing interval of precipitation in [sec]')3
    #group2.add_argument("baseline_decibel", help = 'baseline of frequency for compute precip')
    #group2.add_argument("freq_const", help = 'minus constant in [decibel] ')
    
    print "hello "
    db_schema="public"
    db_name="letnany"
    db_host="localhost"
    db_port="5432"
    db_user='matt'
    db_password= None
    
    sumprecip=60
    baseline_decibel=1
    Aw=1.5
    
    
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
    except:
        print "I am unable to connect to the database."
        
        
   #compute precipitation    
    computePrecip(db,baseline_decibel,Aw)
    
   # sumPrecip(sumprecip)
 
    
    
main()