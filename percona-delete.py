#!/usr/bin/env python
# -*- coding: utf-8 -*-

# autor: Ignacio Gilbaja
# date: 2014-11-25
# mail: jose-ignacio.gilbaja@deimos-space.com,
# version: 1.0

###################################################################################################
# version 1.0 release notes:
# Initial version
####################################################################################################

import string, cgi, time
import time
import sys
import datetime
import os
from datetime import date, timedelta



#### VARIABLES ########################################################

# delta in seconds
delta = 90*24*3600

# date for keep data at database
timestamp = (int(time.time()) - delta) * 1000

logFile=time.strftime("%Y%m%d")+"-delete-old-data.log"
logFolder="/root/percona/logs/"

VEHICLES_TO_DELETE="SELECT VEHICLE_LICENSE FROM HAS, (SELECT FLEET_ID FROM FLEET WHERE SAVE_HISTORIC_TRACKING = false) FLEET_X WHERE HAS.FLEET_ID = FLEET_X.FLEET_ID"
TRACKING_ID="SELECT TRACKING_ID from TRACKING WHERE VEHICLE_LICENSE IN (#vehicles#)"
CONDITION="TRACKING_ID IN (#condition#)"
TEMPLATE="pt-archiver --source u=root,h=127.0.0.1,D=sumo,t=table,p=dat1234,b=true --where \'condition\' --commit-each --purge --progress 10000 --statistics --no-check-charset >> " + logFolder + logFile

CONDITION2="DATE_EVENT<#date#"

def LOGGEDEVENT():
        condition = CONDITION2.replace('#date#',str(timestamp))
        query = TEMPLATE.replace('condition', condition).replace('table', 'LOGGEDEVENT')
        return query

def VEHICLE_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE).replace('#date#',str(timestamp))
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'VEHICLE_EVENT')
        return query

def TRACKING_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE).replace('#date#',str(timestamp))
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING_EVENT')
        return query

def DRIVER_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE).replace('#date#',str(timestamp))
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'DRIVER_EVENT')
        return query

def TRACKING_HAS_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE).replace('#date#',str(timestamp))
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING_HAS_EVENT')
        return query

def TRACKING():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE).replace('#date#',str(timestamp))
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING')
        return query

os.system(VEHICLE_EVENT())
os.system(TRACKING_EVENT())
os.system(DRIVER_EVENT())
os.system(TRACKING_HAS_EVENT())
os.system(TRACKING())
os.system(LOGGEDEVENT())

