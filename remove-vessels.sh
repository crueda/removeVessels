#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-04-01
# mail: carlos.rueda@deimos-space.com
# version: 1.0

##################################################################################
# version 1.0 release notes:
# Initial version
##################################################################################

import time
import datetime
import os
import sys
import utm
import logging, logging.handlers
import MySQLdb as mdb

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./remove_vessels.properties')

LOG = config['directory_logs'] + "/remove_vessels_script.log"
LOG_FOR_ROTATE = 10

BBDD_HOST = config['BBDD_host']
BBDD_PORT = config['BBDD_port']
BBDD_USERNAME = config['BBDD_username']
BBDD_PASSWORD = config['BBDD_password']
BBDD_NAME = config['BBDD_name']

SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/remove_vessels_scheduler"

########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('remove_vessels')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if remove_vessels process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the remove_vessels process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start remove_vessels process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "remove_vessels process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

########################################################################
# Definicion de funciones
#
########################################################################


########################################################################
# Funcion principal
#
########################################################################

def main():
    con = None
    try:
        con = mdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)
        cur = con.cursor()

        sql = 'SELECT DEVICE_ID FROM TRACKING_1 where HEADING>0'
        #logger.debug(sql)
        cur.execute(sql)
        numrows = int(cur.rowcount)

        if (numrows>0):

            deviceList = ''
            for i in range(numrows):
                row = cur.fetchone()
                if (deviceList == ''):
                    deviceList = str(row[0])
                else:
                    deviceList = str(deviceList) + "," + str(row[0])

            logger.info('Lista de barcos a borrar: ' + deviceList)

            logger.info('Borrando TRACKING...')
            curTracking = con.cursor()
            sql = 'DELETE FROM TRACKING where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curTracking.execute(sql)
            curTracking.close() 

            logger.info('Borrando TRACKING_1...')
            curTracking1 = con.cursor()
            sql = 'DELETE FROM TRACKING_1 where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curTracking1.execute(sql)
            curTracking1.close() 
                
            logger.info('Borrando TRACKING_5...')
            curTracking5 = con.cursor()
            sql = 'DELETE FROM TRACKING_5 where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curTracking5.execute(sql)
            curTracking5.close() 

            logger.info('Borrando OBT...')
            curObt = con.cursor()
            sql = 'DELETE FROM OBT where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curObt.execute(sql)
            curObt.close() 

            logger.info('Borrando HAS...')
            curHas = con.cursor()
            sql = 'DELETE FROM HAS where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curHas.execute(sql)
            curHas.close() 

            logger.info('Borrando VEHICLE...')
            curVehicle = con.cursor()
            sql = 'DELETE FROM VEHICLE where DEVICE_ID IN (' + deviceList + ')'
            logger.debug(sql)
            curVehicle.execute(sql)
            curVehicle.close() 

            con.commit() 

    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()

if __name__ == '__main__':
    main()
