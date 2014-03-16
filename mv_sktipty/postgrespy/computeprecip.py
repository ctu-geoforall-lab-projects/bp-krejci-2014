#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random
import math
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

def computePrecip(db,baseline_decibel,Aw):
    #create temporaly table of record
    db_temp="temp_%s"%randomWord(2)
    sql='CREATE TABLE %s AS SELECT * FROM record'%db_temp
    db.executeSql(sql,False)
    
    #nuber of link in table link
    print "num of link"
    link_num=db.count("link")
    print link_num
    
    #nuber of records in table record
    print "num of record"
    record_num=db.count("record")
    print record_num 
    
    #create view of record sorting by time asc!                     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!limit 5, pak tu zadnej limit nebude
    db_view=randomWord(5)
    print "name of view %s"%db_view 
    sql="CREATE VIEW %s AS SELECT * from record ORDER BY time::date asc ,time::time asc LIMIT 3; "%db_view
    db.executeSql(sql,False)
    
    
#loop compute precip for each rows in table record    
    for record in range(1,3):
    #get mw intensity "rxpower-txpower" from first row from view
        sql="select (rxpower-txpower) from %s  limit 1 ;"%db_view
        a=db.executeSql(sql)[0][0]
        print "a %s "%a
        
    #get linkid from first row from view
        sql="SELECT linkid FROM %s LIMIT 1 ;"%db_view
        linkid=db.executeSql(sql)[0][0]
        print 'linkid %s'%linkid 
        
    #get length [meters] of link
        sql="SELECT ST_Length(link.geom,false)\
        FROM link join record on link.linkid=record.linkid \
        WHERE link.linkid = record.linkid AND link.linkid=%s\
        LIMIT 1;"%linkid
        length=db.executeSql(sql)[0][0]
        length=length/1000.0
        print 'length %s'%length
        
    #get frequency of link !!!!!!potreba optimalizace?  ( vypsat frequency u prvniho radku ve view(frequency je v tabulce link)    
        sql="SELECT distinct on(l.linkid) l.frequency\
            FROM link as l join \
            (SELECT linkid from %s limit 1) as t\
            on l.linkid=t.linkid ;"%db_view
        freq=db.executeSql(sql)[0][0]
        freq=freq/1000000.0
        print "freq %s"%freq
        
    #get polarization of link !!!!!!potreba optimalizace?( vypsat polarizaci u prvniho radku ve view(polarizace je v tabulce link)       
        sql="SELECT distinct on(l.linkid) l.polarization\
            FROM link as l join \
            (SELECT linkid from %s limit 1) as t\
            on l.linkid=t.linkid ;"%db_view
        polarization=db.executeSql(sql)[0][0]
        print "polarization %s" %polarization
    
    #coef_a_k[alpha, k]
        coef_a_k= computeAlphaK(freq,polarization)
    #final precipiatation is R    
        Ar=(-1)*a-baseline_decibel-Aw
        yr=Ar/length
        print "a k"
        print coef_a_k 
        print 'Ar %s'%Ar
        print 'yr %s'%yr
        
        alfa=1/coef_a_k[1]
        beta=1/coef_a_k[0]
        
        R=(alfa**beta)*yr**beta
        print "precip %s"%R
    #get time of first row in view     
        sql="select time from %s limit 1;"%db_view
        time=db.executeSql(sql)[0][0] 
        print time
        
        sql="UPDATE record SET precipitation =%s where time='%s';" %(R,time)
        db.executeSql(sql,False) 
          
    #delete first rows in view        
        sql="DELETE FROM %s where time='%s'" %(db_temp,time)  
        db.executeSql(sql,False)
        
        sql='DROP VIEW %s;\
        CREATE  VIEW %s AS SELECT * from %s ORDER BY time::date asc ,time::time asc;\
        DROP VIEW %s;\
        CREATE  VIEW %s AS SELECT * from %s ORDER BY time::date asc ,time::time asc;'%(db_view,db_view,db_temp,db_view,db_view,db_temp)
        db.executeSql(sql,False)

  
#------------------------------------------------------------------main-------------------------------------------    
def main():
    #parser = argparse.ArgumentParser ()
    #group1 = parser.add_argument_group('group1', 'group database')
    #group1.add_argument("phost", help = 'database host')
    #group1.add_argument("pdb_name", help = 'database name')
    #group1.add_argument("pdb_schema", help = 'database schema')
    #group1.add_argument("pport", help = 'port')
    #group1.add_argument("puser", help = 'database user')
    #group1.add_argument("ppassword", help = 'database password')
    print "hello "
    db_schema="public"
    db_name="letnany"
    db_host="localhost"
    db_port="5432"
    db_user='matt'
    db_password= None
    
    timestep=60
    baseline_decibel=1
    Aw=1.5
    

    
    #if required password and user    
    if db_password:        
        db = pg(dbname=db_name, host=db_host,
                user=db_user, passwd = db_password)   
    #if required only user
    elif db_user and not db_password:         
        db = pg(dbname=db_name, host=db_host,
                user=db_user)
    #if not required user adn passwd   
    else:
        db = pg(dbname=db_name, host=db_host,
                user=db_user)    
   #compute precipitation    
    computePrecip(db,baseline_decibel,Aw)
 
    
    
main()