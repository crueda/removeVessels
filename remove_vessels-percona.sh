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

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./remove_vessels.properties')

MAX_LAT = config['maxLat']
MIN_LAT = config['minLat']
MAX_LON = config['maxLon']
MIN_LON = config['minLon']

#### VARIABLES ########################################################

logFile=time.strftime("%Y%m%d")+"-delete-old-data.log"
logFolder="./logs"

VEHICLES_TO_DELETE="SELECT DEVICE_ID from TRACKING_1 where ( ((POS_LATITUDE_DEGREE + POS_LATITUDE_MIN/60) > MAX_LAT) || ((POS_LATITUDE_DEGREE + POS_LATITUDE_MIN/60) < MIN_LAT) || ((POS_LONGITUDE_DEGREE + POS_LONGITUDE_MIN/60) > MAX_LON) || ((POS_LONGITUDE_DEGREE + POS_LONGITUDE_MIN/60) < MIN_LON) )"
VEHICLES_TO_DELETE = VEHICLES_TO_DELETE.replace('MAX_LAT',MAX_LAT).replace('MIN_LAT',MIN_LAT).replace('MAX_LON',MAX_LON).replace('MIN_LON',MIN_LON)

TRACKING_ID="SELECT TRACKING_ID from TRACKING WHERE DEVICE_ID IN (#vehicles#)"
CONDITION="TRACKING_ID IN (#condition#)"
CONDITION_DEVICE="DEVICE_ID IN (#condition#)"
TEMPLATE="pt-archiver --source u=root,h=127.0.0.1,D=sumo,t=table,p=dat1234,b=true --where \'condition\' --commit-each --purge --progress 10000 --statistics --no-check-charset >> " + logFolder + logFile


def VEHICLE_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE)
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'VEHICLE_EVENT')
        print query
        return query

def TRACKING_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE)
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING_EVENT')
        return query

def DRIVER_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE)
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'DRIVER_EVENT')
        return query

def TRACKING_HAS_EVENT():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE)
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING_HAS_EVENT')
        return query

def TRACKING():
        partial = TRACKING_ID.replace('#vehicles#', VEHICLES_TO_DELETE)
        condition = CONDITION.replace('#condition#',partial)
        query = TEMPLATE.replace('condition', condition).replace('table', 'TRACKING')
        return query

def HAS():
        condition = CONDITION_DEVICE.replace('#condition#',VEHICLES_TO_DELETE)
        query = TEMPLATE.replace('condition', condition).replace('table', 'HAS')
        return query

def OBT():
        condition = CONDITION_DEVICE.replace('#condition#',VEHICLES_TO_DELETE)
        query = TEMPLATE.replace('condition', condition).replace('table', 'OBT')
        return query

def VEHICLE():
        condition = CONDITION_DEVICE.replace('#condition#',VEHICLES_TO_DELETE)
        query = TEMPLATE.replace('condition', condition).replace('table', 'VEHICLE')
        return query

'''
os.system(VEHICLE_EVENT())
os.system(TRACKING_EVENT())
os.system(DRIVER_EVENT())
os.system(TRACKING_HAS_EVENT())
os.system(TRACKING())
os.system(HAS())
os.system(OBT())
os.system(VEHICLE())
'''

VEHICLE_EVENT()

