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
#% label: Set time "YYYY-MM-DD H:M"
#% required : yes
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


schema=''
time=''
path=''
link_ogr='link_ogr'
link_nat="link_nat"

def print_message(msg):
    grass.message(msg)
    grass.message('-' * 60)

def setFirstRun():
    try:
        io= open(os.path.join(path,"firstrun"),"wr")
        io.write('run')
        io.close
    except IOError as (errno,strerror):
        print "I/O error({0}): {1}".format(errno, strerror)

def firstConnect():

    print_message('v.in.ogr')
    grass.run_command('v.in.ogr',
                    dsn="PG:",
                    layer = 'link',
                    output = link_ogr,
                    overwrite=True,
                    flags='t',
                    type='point')
   
    # if vector already exits, remove dblink (original table)
    if grass.find_file(link_nat, element='vector')['fullname']:
        print_message('remove link to layer 1 and 2')
        grass.run_command('v.db.connect',
                          map=link_nat,
                          flags='d',
                          layer='1')
        grass.run_command('v.db.connect',
                          map=link_nat,
                          flags='d',
                          layer='2')
        
    print_message('v.category')
    grass.run_command('v.category',
                    input=link_ogr,
                    output=link_nat,
                    option="transfer",
                    overwrite=True,
                    layer="1,2")
    
    print_message('v.db.connect')
    grass.run_command('v.db.connect',
                    map=link_nat,
                    table='link',
                    key='linkid',
                    layer='1',
                    quiet=True)
    
def nextConnect():
    view=schema+'.view'+time.replace('-','_').replace(':','_').replace(' ','_')
    grass.run_command('v.db.connect',
                    map=link_nat,
                    layer='2',
                    flags='d')
    
    grass.run_command('v.db.connect',
                    map=link_nat,
                    table=view,
                    key='linkid',
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
                    layer = 'link',
                    output = link_ogr,
                    overwrite=True,
                    flags='t',
                    type='point')
   
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
                    input=link_ogr,
                    output=view_nat,
                    option="transfer",
                    overwrite=True,
                    layer="1,2")
    
    print_message('v.db.connect')
    grass.run_command('v.db.connect',
                    map=view_nat,
                    table='link',
                    key='linkid',
                    layer='1',
                    quiet=True)

    grass.run_command('v.db.connect',
                    map=view_nat,
                    layer='2',
                    flags='d')
    
    grass.run_command('v.db.connect',
                    map=view_nat,
                    table=view,
                    key='linkid',
                    layer='2',
                    quiet=True)    

def main():
    
    global schema,time,path
    schema=options['schema']
    time=options['time']
    
    path= os.path.join(os.path.dirname(os.path.realpath(__file__)), "tmp_%s"%schema)

    try: 
        os.makedirs(path)
        
    except OSError:
        if not os.path.isdir(path):
            raise
        
    #dbConnGrass(options['database'],options['user'],options['password'])
    
    if not flags['c']:
        if not os.path.exists(os.path.join(path,"firstrun")):
            setFirstRun()
            print_message("first")
            firstConnect()
            nextConnect()
        else:
            print_message("next")
            nextConnect()
            print_message("Layer connected")
    else:
        createVect()
        
        
    if flags['p']:
        view=schema+'.view'+time.replace('-','_').replace(':','_').replace(' ','_')
        sql='select linkid, precip_mm_h_minute from %s '%view
        grass.run_command('db.select',
                    sql=sql,
                    separator='  ')
        
        
        
        
        
        
        
        
    print_message("DONE")    
    
if __name__ == "__main__":
    options, flags = grass.parser()

main()
