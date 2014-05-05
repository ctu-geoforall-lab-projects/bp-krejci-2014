#!/usr/bin/env python
# -*- coding: utf-8

'''
INSTALACE
pyproj
https://pyproj.googlecode.com/files/pyproj-1.9.3.tar.gz

##http://lxml.de/
git clone git://github.com/lxml/lxml.git lxml

sql alchemy
https://pypi.python.org/pypi/SQLAlchemy/0.9.4?

shapely
http://toblerity.org/shapely/project.html#installation

owslib
http://geopython.github.io/OWSLib/index.html?highlight=cql#install

http://pycsw.org/docs/latest/installation.html

$ virtualenv pycsw && cd pycsw && . bin/activate
$ git clone https://github.com/geopython/pycsw.git && cd pycsw
$ pip install -e . && pip install -r requirements-dev.txt
$ cp default-sample.cfg default.cfg
$ vi default.cfg
# adjust paths in
# - server.home
# - repository.database
# set server.url to http://localhost:8000/
$ python csw.wsgi
$ curl http://localhost:8000/?service=CSW&version=2.0.2&request=GetCapabilities
'''


'''
Setup the database:
$ cd /var/www/pycsw
$ export PYTHONPATH=`pwd`
$ sudo python ./sbin/pycsw-admin.py -c setup_db -f default.cfg
'''

'''
Nahrani matadat do databaze pycsw
cd /var/www/pycsw
   pycsw-admin.py -c load_records -f default.cfg -p ~/gisdata-master/gisdata/metadata/good -r
'''



'''
SKRIPTY
http://nbviewer.ipython.org/github/rsignell-usgs/notebook/tree/master/CSW/

'''



import sys, os,  argparse
from owslib.csw import CatalogueServiceWeb

print sys.executable
csw = CatalogueServiceWeb('http://cida.usgs.gov/gdp/geonetwork/srv/en/csw')


csw.identification.type
print [op.name for op in csw.operations]


csw.getdomain('GetRecords.resultType')
print csw.results


csw.getrecords2(maxrecords=20)
print csw.results


for rec in csw.records:
    print csw.records[rec].title
    print csw.records[rec].identifier
   
csw.transaction(ttype='update', typename='csw:Record', propertyname='dc:title', propertyvalue='New Title')











'''
import sys, os,  argparse
from owslib.csw import CatalogueServiceWeb
from owslib.fes import FilterRequest

#csw = CatalogueServiceWeb('http://geodiscover.cgdi.ca/wes/serviceManagerCSW/csw')
csw = CatalogueServiceWeb('http://localhost:8000/pycsw')

#feslist=FilterRequest().set(False,'dataset',['birds'],'csw:Record', 'csw:AnyText', None, None)

print csw.identification.type



###Get supported resultType’s:
csw.getdomain('GetRecords.resultType')
print csw.results


csw.getrecordbyid
###Search for bird data:
csw.results
for rec in csw.records:
    print csw.records[rec].title
sys.exit()





###Search for a specific record:
csw.getrecordbyid(id=['9250AA67-F3AC-6C12-0CB9-0662231AA181'])
c.records['9250AA67-F3AC-6C12-0CB9-0662231AA181'].title


###Search with a CQL query
csw.getrecords(cql='csw:AnyText like "%birds%"')


###Transaction: insert
csw.transaction(ttype='insert', typename='gmd:MD_Metadata', record=open(file.xml).read())


###Transaction: update
# update ALL records
csw.transaction(ttype='update', typename='csw:Record', propertyname='dc:title', propertyvalue='New Title')
 # update records satisfying keywords filter
csw.transaction(ttype='update', typename='csw:Record', propertyname='dc:title', propertyvalue='New Title', keywords=['birds','fowl'])
# update records satisfying BBOX filter
csw.transaction(ttype='update', typename='csw:Record', propertyname='dc:title', propertyvalue='New Title', bbox=[-141,42,-52,84])


###Transaction: delete
# delete ALL records
csw.transaction(ttype='delete', typename='gmd:MD_Metadata')
# delete records satisfying keywords filter
csw.transaction(ttype='delete', typename='gmd:MD_Metadata', keywords=['birds','fowl'])
# delete records satisfying BBOX filter
csw.transaction(ttype='delete', typename='gmd:MD_Metadata', bbox=[-141,42,-52,84])


###Harvest a resource
csw.harvest('http://host/url.xml', 'http://www.isotc211.org/2005/gmd')

'''
