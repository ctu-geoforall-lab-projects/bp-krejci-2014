#!/usr/bin/env python
# -*- coding: utf-8
import os
import sys
import argparse
import string, random


try:
    from grass.script import core as grass  
except ImportError:
    sys.exit("Cannot find 'grass' Python module. Python is supported by GRASS from version >= 6.4")


##########################################################
################## guisection: required ##################
##########################################################
#%module
#% description: Module for linking time-windows to vector link map 
#%end


#%option
#% key: schema
#% type: string
#% label: Set schema name containing timewindows
#% required : yes
#%end


#%option
#% key: time
#% type: string
#% label: Set time "YYYY-MM-DD H:M:S"
#% required : yes
#%end

#%option
#% key: type
#% label: Choose object type to connect
#% options: raingauge, links
#% multiple: yes
#% required : yes
#% answer: links
#%end

#%flag
#% key:c
#% description: Create vector map
#%end


##########################################################
################## guisection: optional ##################
##########################################################
#%option G_OPT_F_INPUT
#% key: color
#% label: Set color table
#% required: no
#%end


#%flag
#% key:p
#% description: Print attribut table
#%end

#%flag
#% key:r
#% description: Remove temp file
#%end


schema=''
time=''
path=''
ogr=''
nat=''
layer=''
key=''
prefix=''
typ=''
firstrun=''

def print_message(msg):
    grass.message(msg)
    grass.message('-' * 60)

def setFirstRun():
    try:
        io= open(os.path.join(path,firstrun),"wr")
        io.write(options['type'])
        io.close
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)

def firstConnect():

    print_message('v.in.ogr')
    grass.run_command('v.in.ogr',
                    dsn="PG:",
                    layer = layer,
                    output = ogr,
                    overwrite=True,
                    flags='t',
                    type=typ)
   
    # if vector already exits, remove dblink (original table)
    if grass.find_file(nat, element='vector')['fullname']:
        print_message('remove link to layer 1 and 2')
        grass.run_command('v.db.connect',
                          map=nat,
                          flags='d',
                          layer='1')
        grass.run_command('v.db.connect',
                          map=nat,
                          flags='d',
                          layer='2')
        
    print_message('v.category')
    grass.run_command('v.category',
                    input=ogr,
                    output=nat,
                    option="transfer",
                    overwrite=True,
                    layer="1,2")
    
    print_message('v.db.connect')
    grass.run_command('v.db.connect',
                    map=nat,
                    table=layer,
                    key=key,
                    layer='1',
                    quiet=True)
    
def nextConnect():
    view=schema+'.%sview'%prefix+time.replace('-','_').replace(':','_').replace(' ','_')
    view=view[:-3]
    grass.run_command('v.db.connect',
                    map=nat,
                    layer='2',
                    flags='d')
    
    grass.run_command('v.db.connect',
                    map=nat,
                    table=view,
                    key=key,
                    layer='2',
                    quiet=True)    

#def setColor():
    #TODO

def createVect():
    print_message('v.in.ogr')
    view=schema+'.view'+time.replace('-','_').replace(':','_').replace(' ','_')
    view_nat='view'+time.replace('-','_').replace(':','_').replace(' ','_')
    
    grass.run_command('v.in.ogr',
                    dsn="PG:",
                    layer = layer,
                    output = ogr,
                    overwrite=True,
                    flags='t',
                    type=typ)
   
    # if vector already exits, remove dblink (original table)
    if grass.find_file(view_nat, element='vector')['fullname']:
        print_message('remove link to layer 1 and 2')
        grass.run_command('v.db.connect',
                          map=view_nat,
                          flags='d',
                          layer='1')
        grass.run_command('v.db.connect',
                          map=view_nat,
                          flags='d',
                          layer='2')
        
    print_message('v.category')
    grass.run_command('v.category',
                    input=ogr,
                    output=view_nat,
                    option="transfer",
                    overwrite=True,
                    layer="1,2")
    
    print_message('v.db.connect')
    grass.run_command('v.db.connect',
                    map=view_nat,
                    table='link',
                    key=key,
                    layer='1',
                    quiet=True)

    grass.run_command('v.db.connect',
                    map=view_nat,
                    layer='2',
                    flags='d')
    
    grass.run_command('v.db.connect',
                    map=view_nat,
                    table=view,
                    key=key,
                    layer='2',
                    quiet=True)    

def run():
    try: 
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
        
    #dbConnGrass(options['database'],options['user'],options['password'])
    
    if not flags['c']:
        if not os.path.exists(os.path.join(path,firstrun)):
            setFirstRun()
            #print_message("first")
            firstConnect()
            nextConnect()
        else:
            #print_message("next")
            nextConnect()
            print_message("Layer connected")
    else:
        createVect()
        
        
    if flags['p']:
        view=schema+'.%sview'%prefix+time.replace('-','_').replace(':','_').replace(' ','_')
        view=view[:-3]
        sql='select %s, precip_mm_h from %s '%(key,view)
        grass.run_command('db.select',
                    sql=sql,
                    separator='  ')
        
        
        
def main():
    
    
    global schema,time,path,ogr,nat,layer,key,prefix,typ,firstrun
    schema=options['schema']
    time=options['time']
    path= os.path.join(os.path.dirname(os.path.realpath(__file__)), "tmp_%s"%schema)
    
    
    if flags['r']:
        try:
            os.remove(os.path.join(path,'firstrunlink'))
            os.remove(os.path.join(path,'firstrungauge'))
        except:
            print_message("Temp file not exists")
            
    if options['type'].find('l')!=-1:
        ogr='link_ogr'
        nat="link_nat"
        layer='link'
        key='linkid'
        prefix='l'
        typ='line'
        firstrun='firstrunlink'
        run()
        
    if options['type'].find('r')!=-1:  
        ogr='gauge_ogr'
        nat="gauge_nat"
        layer='%s.rgauge'%schema
        key='gaugeid'
        prefix='g'
        typ='point'            
        firstrun='firstrungauge'
        run()  


        
        

        
    print_message("DONE")    
    
if __name__ == "__main__":
    options, flags = grass.parser()

main()
