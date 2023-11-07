#!/usr/bin/env python
##
## Web site for AutoBRM.
##  Source : Yamcs-server @ Yamcs-pdc-fsl TM item APM/FSL_VMU_Recorder_OpMode
##  Destination : AutoBRM MySql database
##
## ADI - Space Applications Services - Dec2020
##
## KL - B.USOC  20230830 -  Added absolute path to the FSL DB


import threading
import datetime
import commands
import urllib2
import base64
import json
from time import sleep
import sqlite3

#Constants
##Local/PDC
db_database = '/opt/autobrm/fsl_hrd.db'
parameterName = 'FSL_VMU_Recorder_OpMode'
los_sensing_threshold_in_seconds = 10
los_gap_request_margin_seconds = 10
gap_request_delay_in_hours = 6

#Database operations function
def update_mysql(statement):
    #Local/PDC
    cnx = sqlite3.connect(db_database, isolation_level=None) #AutoCommit is enabled
    cnx.execute("PRAGMA foreign_keys = 1") #Foreign key constraints are enabled
    cursor = cnx.cursor()
    cursor.execute(statement)
    lastrowid = cursor.lastrowid
    cursor.close()
    cnx.commit()
    cnx.close()
    return lastrowid

#Returns a variable value from its name by reading it from the database variable table.    
def getVariableValue(varName):
    cnx = sqlite3.connect(db_database, isolation_level=None) #AutoCommit is enabled
    cnx.execute("PRAGMA foreign_keys = 1") #Foreign key constraints are enabled
    dbCur = cnx.cursor()
    #get Record ID
    queryStatement = 'SELECT value FROM variable WHERE name like "%s";' % varName
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #Existing?
    if len(queryResult) > 0:
        varValue = str(queryResult[0][0])
        if str.isdigit(varValue): varValue = int(varValue)
    else:
        varValue = None
        
    dbCur.close()
    cnx.close()
    return varValue
    
#Read and return parameter current value from Yamcs
def getYamcsParameterValue(parameterName):
    #Local/PDC
    server = 'yamcs-pdc'; username = '*******'; password = '********'
    request = urllib2.Request("http://%s:8090/api/processors/fsl-ops/realtime/parameters/APM/%s" % (server, parameterName))
    base64string = base64.b64encode('%s:%s' % (username, password))
    request.add_header("Authorization", "Basic %s" % base64string)
    result = urllib2.urlopen(request).read()
    resultJson = json.loads(result)
    
    returnDict = {}
    returnDict['modeValue'] = resultJson['engValue']['stringValue']
    returnDict['genDate'] = datetime.datetime.strptime(resultJson['generationTimeUTC'], '%Y-%m-%dT%H:%M:%S.%f')
    returnDict['acqDate'] =  datetime.datetime.strptime(resultJson['acquisitionTimeUTC'], '%Y-%m-%dT%H:%M:%S.%f')
    
    return (returnDict)
    
#Main
def main(arg):
    #Variables
    insideRelevantLOS = None
    los_startdate = None
    los_enddate = None
    losList = []
    
    #Main
    t = threading.currentThread()
    while getattr(t, "do_run", True):
        sleep(10)
        if getVariableValue('auto_los_sensing_replay_filler').lower() == 'on':
            groundDate = datetime.datetime.now()
            #get PP
            pp = getYamcsParameterValue(parameterName)
            'modeValue: %s' % pp['modeValue']
            'acqDate: %s' % pp['acqDate']
            #'genDate: %s' % pp['genDate']
            'groundDate: %s' % groundDate
            #parse PP and search for LOS
            if not insideRelevantLOS:
                'not insideRelevantLOS'
                if ((pp['modeValue'] == 'Playback') and ((groundDate - pp['acqDate']) >= datetime.timedelta(seconds=los_sensing_threshold_in_seconds))):
                    'in first if'
                    insideRelevantLOS = True
                    los_startdate = pp['acqDate'] - datetime.timedelta(seconds=los_gap_request_margin_seconds)
            else:
                'insideRelevantLOS'
                if ((groundDate - pp['acqDate']) < datetime.timedelta(seconds=los_sensing_threshold_in_seconds)):
                    'in second if'
                    insideRelevantLOS = False
                    los_enddate = pp['acqDate'] + datetime.timedelta(seconds=los_gap_request_margin_seconds)
                    #Add LOS (+margins) to list
                    losList.append([{ 'startDate': los_startdate, 'endDate': los_enddate }])
            '...'
            #Process LOSgap list and perform DB Insert
            iterationList = losList[:] #Copy list for iteration
            for replayItem in iterationList:
                if groundDate - replayItem[0]['endDate'] >= datetime.timedelta(hours=gap_request_delay_in_hours):
                    #Insert previously generated LOS list to AutoBRM database
                    lastrowid = update_mysql('INSERT INTO replay(timestamp,startdate,enddate,priority) VALUES (datetime("now"),"%s","%s",0);' % (replayItem[0]['startDate'], replayItem[0]['endDate']))
                    _ = update_mysql('INSERT INTO replay_job(timestamp,text,replay_id,replay_status_id) VALUES (datetime("now"),"Manual replay request inserted by the automatic LOS sensing script",%s,1);' % lastrowid)
                    #Remove LOS from the list
                    losList.remove(replayItem)

