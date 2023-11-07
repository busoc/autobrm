#!/usr/bin/env python
##
## Script to command the command line bitstream request, to automatically fill high rate data gaps.
##  Source : HRDP Raw RT.DAT data files.
##  Destination : Command line bitstream request tool.
##
## ADI - Space Applications Services - Dec2018
##
## KL - B.USOC - 20230830 - Added absolute path to the FSL DB

import signal, os
import logging
import commands
import threading
import re
import urllib2, base64, json
from datetime import datetime
from datetime import timedelta
from time import sleep
import autoLosSensingReplayFiller
import sqlite3

#Establishes the database connection and returns the connector object.                            
def dbConnectToDatabase():
    try:
        #Local/PDC
        dbCon = sqlite3.connect('/opt/autobrm/fsl_hrd.db', timeout=10, isolation_level=None) #AutoCommit is enabled
        dbCon.execute("PRAGMA foreign_keys = 1") #Foreign key constraints are enabled
        
        return dbCon
    except Exception, errorString:
        logger.error(errorString)
        return None

#Sets or creates a variable value on the database variable table.  
def setVariableValue(dbCon, varName, varValue):
    dbCur = dbCon.cursor()
    #get Record ID
    queryStatement = 'SELECT id FROM variable WHERE name like "%s";' % varName
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #Existing?
    if len(queryResult) > 0:
        #Update existing
        varID = queryResult[0][0]
        dbStatement = 'update variable set value="%s" where id = %s' % (varValue, varID)
    else:
        #Create new
        dbStatement = 'insert into variable(name, value) values("%s", "%s");' % (varName, varValue)
        
    logger.debug(dbStatement)
    dbCur.execute(dbStatement)
    dbCur.close()

#Returns a variable value from its name by reading it from the database variable table.    
def getVariableValue(dbCon, varName):
    dbCur = dbCon.cursor()
    #get Record ID
    queryStatement = 'SELECT value FROM variable WHERE name like "%s";' % varName
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #Existing?
    if len(queryResult) > 0:
        varValue = str(queryResult[0][0])
        if str.isdigit(varValue): varValue = int(varValue)
    else:
        varValue = None
        
    dbCur.close()
    return varValue

#Binds a VMU_packet_gap item to a replay item on the database.    
def linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID):
    dbCur = dbCon.cursor()
    insertStatement = 'INSERT INTO gap_replay_list(replay_id, hrd_packet_gap_id) \
    VALUES(%s, %s);' % (resultReplayID, gapItemID)
    logger.debug(insertStatement)
    dbCur.execute(insertStatement)
    dbCur.close()

#Sets the 'checked' flag on a HRD_packet_gap item after having processed it.
def markHrdGapItemAsChecked(dbCon, gapItemID):
    dbCur = dbCon.cursor()
    updateStatement = 'UPDATE hrd_packet_gap SET is_checked = 1 WHERE id = %s' % gapItemID
    logger.debug(updateStatement)
    dbCur.execute(updateStatement)
    dbCur.close()

#Returns the next workflow state for the given workflow state.
def getNextWorkflowStateID(dbCon, replayStateID):
    dbCur = dbCon.cursor()
    queryStatement = 'SELECT id, NAME, workflow FROM replay_status WHERE workflow>(SELECT workflow FROM replay_status WHERE id = %s) ORDER BY workflow ASC LIMIT 1;' % replayStateID
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    #Existing?
    if len(queryResult) > 0:
        return queryResult[0][0]
    else:
        return None

#Returns a replay state for the given replay item.        
def getReplayItemStateID(dbCon, replayItemID):
    dbCur = dbCon.cursor()
    #is the replay_job created?  
    queryStatement = 'SELECT id, replay_status_id FROM replay_job WHERE \
    replay_id = %s order by id desc limit 1;' % replayItemID
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    #Existing?
    if len(queryResult) > 0:
        replayJobID = queryResult[0][0]
        replayJobStateID = queryResult[0][1]
        return [replayJobID, replayJobStateID]
    else:
        return None
  
#Creates a new replay_job entry on the database, with the arguments provided.  
def setReplayItemState(dbCon, replayItemID, stateName, text = None):
    dbCur = dbCon.cursor()
    try:
        insertStatement = 'INSERT INTO replay_job(TIMESTAMP, replay_id, replay_status_id, text) \
        VALUES (datetime("now"), %s, (SELECT id FROM replay_status WHERE NAME LIKE "%s"), %s);' % (replayItemID, stateName, "null" if not bool(text) else '"%s"' % re.escape(text[:100]))
        logger.debug(insertStatement)
        dbCur.execute(insertStatement)
        dbCur.close()
    except Exception, errorString:
        logger.error(errorString)
        dbCur.close()

#Sets the replay_job state to the next state according following the replay_status workflow.    
def incrementReplayItemState(dbCon, replayItemID, text = None):
    dbCur = dbCon.cursor()
    #is the replay_job created?
    queryResult = getReplayItemStateID(dbCon, replayItemID)
    try:
        #Existing replay job?
        if queryResult is not None:
            #Existing, create next
            replayJobID = queryResult[0]
            replayJobStateID = queryResult[1]
            #Get next status ID according to workflow
            nextStateID = getNextWorkflowStateID(dbCon, replayJobStateID)
            #Insert new replayJob entry
            insertStatement = 'INSERT INTO replay_job(timestamp, replay_id, replay_status_id, text) \
            VALUES(datetime("now"), %s, %s, %s);' % (replayItemID, nextStateID, 'null' if bool(text) else '%s' % 'null' if not bool(text) else '"%s"' % re.escape(text[:100]))
        else:
            #Non existing, create first
            insertStatement = 'INSERT INTO replay_job(timestamp, replay_id, replay_status_id, text) \
            VALUES(datetime("now"), %s, (SELECT id FROM replay_status ORDER BY workflow ASC LIMIT 1), %s);' % (replayItemID, "null" if not bool(text) else '"%s"' % re.escape(text[:100]))

        logger.debug(insertStatement)
        dbCur.execute(insertStatement)
        dbCur.close()
    except Exception, errorString:
        logger.error(errorString)
        dbCur.close()

#Creates a replay item on the database. Returns its replay item database entry ID.  
def insertReplayItem(dbCon, startdate, enddate):
    dbCur = dbCon.cursor()
    insertStatement = 'INSERT INTO replay (TIMESTAMP, startdate, enddate) \
    VALUES (datetime("now"), datetime(\"%s\","-5 seconds"), datetime(\"%s\","+5 seconds"));' % (startdate, enddate)
    #Insert to Database
    logger.debug(insertStatement)
    dbCur.execute(insertStatement)
    #Get ID from inserted ReplayItem
    replayItemID = dbCur.lastrowid
    #Set replay status to its initial state
    incrementReplayItemState(dbCon, replayItemID)
    dbCur.close()
    return replayItemID

#Updates a replay item's start datetime and end datetime on the database.     
def updateReplayItem(dbCon, updateModifiers):
    dbCur = dbCon.cursor()
    #Compose the update statement iterating the updateModifiers dictionary that contains the field to set and its value, also the full where filter condition.
    updateStatement = 'UPDATE replay SET '
    updateStatement = '%s%s' % (updateStatement, "".join(['%s = %s, ' % (key, value) for (key, value) in updateModifiers['fields'].items()]))
    updateStatement = updateStatement.rstrip(', ')
    updateStatement = '%s%s' % (updateStatement, ' ')
    updateStatement = '%s%s' % (updateStatement, 'WHERE ')
    updateStatement = '%s%s' % (updateStatement, "".join(['%s %s AND ' % (key, value) for (key, value) in updateModifiers['where'].items()]))
    updateStatement = updateStatement.rstrip(' AND ')
    updateStatement = '%s%s' % (updateStatement, ';')
    
    logger.debug(updateStatement)
    dbCur.execute(updateStatement)
    dbCur.close()

#Returns a list of replay items based on a filter condition: Normally start datetime and end datetime comparison conditions.   
def queryReplayList(dbCon, queryModifier):
    dbCur = dbCon.cursor()
    #Compose the query statement iterating the updateModifiers dictionary that contains full where filter condition.
    queryStatement = 'SELECT id FROM replay LEFT JOIN latest_replay_status lrs ON lrs.replayID = id WHERE '
    queryStatement = '%s%s' % (queryStatement, "".join(['%s "%s" AND ' % (key, value) for (key, value) in queryModifier['where'].items()]))
    queryStatement = queryStatement.rstrip(' AND ')
    queryStatement = '%s%s' % (queryStatement, 'AND lrs.replayStatus LIKE "NEW";')
    
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    return queryResult

#Returns a list of replay items based on a time offset filter condition. Start datatime or end datetime are normally the arguments.
def queryReplayListTimeDelta(dbCon, queryModifier):
    dbCur = dbCon.cursor()
    #Compose the query statement iterating the updateModifiers dictionary that contains the select fields.
    queryStatement = 'SELECT id, startdate, enddate, '
    queryStatement = '%s%s' % (queryStatement, "".join(['ABS((strftime("%s","{}")-strftime("%s","{}"))/60) AS delta'.format(key, value) for (key, value) in queryModifier['select'].items()]))
    queryStatement = '%s%s' % (queryStatement, ' FROM replay LEFT JOIN latest_replay_status lrs ON lrs.replayID = id WHERE lrs.replayStatus LIKE "NEW"')
    queryStatement = '%s%s' % (queryStatement, ' GROUP BY id, startdate, enddate, delta')
    queryStatement = '%s%s' % (queryStatement, ' HAVING delta>=0 AND delta<=%s;' % getVariableValue(dbCon, 'scan_gap_offset_check_minutes'))
    
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    return queryResult
    
#Read the design documet to understand the match case concept. 
#VMU_packet_gap item has been marked as Case A. This procedure executes the steps according to this matching case.    
def GapItemCaseA (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    logger.debug("This is CASE A")
    #No query the DB - Insert directly
    #Insert ReplayItem & get ReplayItem identifier
    resultReplayID = insertReplayItem(dbCon, gapItemStartDate, gapItemEndDate)
    #Link gapItem to ReplayItem.
    linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
    #Mark gapItem as checked.
    markHrdGapItemAsChecked(dbCon, gapItemID)

#VMU_packet_gap item has been marked as Case B. This procedure executes the steps according to this matching case.
def isGapItemCaseB (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case B - if GapList startdate - ReplayList enddate =< 5 min
    queryResult = queryReplayListTimeDelta(dbCon, {"select":{"enddate":gapItemStartDate}})
    #If ocurrences
    if len(queryResult) > 0:
        logger.debug("This is CASE B")
        resultReplayID = queryResult[0][0]
        #Update ReplayItem enddate
        updateReplayItem(dbCon, {"fields":{"enddate":'datetime(\"%s\","+1 seconds")' % gapItemEndDate}, "where":{"id =":resultReplayID}})
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False

#VMU_packet_gap item has been marked as Case C. This procedure executes the steps according to this matching case.        
def isGapItemCaseC (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case C - if GapList startdate >= ReplayList startdate & GapList startdate <= ReplayList enddate ?
    queryResult = queryReplayList(dbCon, {"where":{"startdate <":gapItemStartDate,"enddate >=":gapItemStartDate}})
    #If ocurrences 
    if len(queryResult) > 0:
        logger.debug("This is CASE C")
        resultReplayID = queryResult[0][0]
        #Update ReplayItem enddate
        updateReplayItem(dbCon, {"fields":{"enddate":'datetime(\"%s\","+1 seconds")' % gapItemEndDate}, "where":{"id =":resultReplayID}})
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False

#VMU_packet_gap item has been marked as Case D. This procedure executes the steps according to this matching case.        
def isGapItemCaseD (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case D - if GapList stardate <= ReplayList startdate & GapList enddate >= ReplayList enddate ?
    queryResult = queryReplayList(dbCon, {"where":{"startdate >=":gapItemStartDate,"enddate <=":gapItemEndDate}})
    #If ocurrences
    if len(queryResult) > 0:
        logger.debug("This is CASE D")
        resultReplayID = queryResult[0][0]
        #Update ReplayItem startdate and enddate
        updateReplayItem(dbCon, {"fields":{"startdate":'datetime(\"%s\","-1 seconds")' % gapItemStartDate, "enddate":'datetime(\"%s\","+1 seconds")' % gapItemEndDate}, "where":{"id =":resultReplayID}})
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False

#VMU_packet_gap item has been marked as Case E. This procedure executes the steps according to this matching case.
def isGapItemCaseE (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case E - if GapList stardate >= ReplayList startdate & GapList enddate <= ReplayList enddate ?
    queryResult = queryReplayList(dbCon, {"where":{"startdate <=":gapItemStartDate,"enddate >=":gapItemEndDate}})
    #If ocurrences
    if len(queryResult) > 0:
        logger.debug("This is CASE E")
        resultReplayID = queryResult[0][0]
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False

#VMU_packet_gap item has been marked as Case F. This procedure executes the steps according to this matching case.        
def isGapItemCaseF (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case F - if GapList enddate >= ReplayList startdate & GapList enddate <= ReplayList enddate ?
    queryResult = queryReplayList(dbCon, {"where":{"startdate <=":gapItemEndDate,"enddate >=":gapItemEndDate}})
    #If ocurrences
    if len(queryResult) > 0:
        logger.debug("This is CASE F")
        resultReplayID = queryResult[0][0]
        #Update ReplayItem startdate
        updateReplayItem(dbCon, {"fields":{"startdate":'datetime(\"%s\","-1 seconds")' % gapItemStartDate}, "where":{"id =":resultReplayID}})
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False

#VMU_packet_gap item has been marked as Case G. This procedure executes the steps according to this matching case.        
def isGapItemCaseG (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    #query the DB - Case G - if GapList enddate - ReplayList startdate =< 5 min
    queryResult = queryReplayListTimeDelta(dbCon, {"select":{"startdate":gapItemEndDate}})
    #If ocurrences
    if len(queryResult) > 0:
        logger.debug("This is CASE G")
        resultReplayID = queryResult[0][0]
        #Update ReplayItem startdate
        updateReplayItem(dbCon, {"fields":{"startdate":'datetime(\"%s\","-1 seconds")' % gapItemStartDate}, "where":{"id =":resultReplayID}})
        #Link gapItem to ReplayItem.
        linkHrdGapItem2ReplayItem(dbCon, resultReplayID, gapItemID)
        #Mark gapItem as checked.
        markHrdGapItemAsChecked(dbCon, gapItemID)
        return True
    else:
        return False
        
#VMU_packet_gap item has been marked as Case H. This procedure executes the steps according to this matching case.        
def GapItemCaseH (dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
    logger.debug("This is CASE H")
    #Same as Case A
    #Insert GapItem as ReplayItem
    GapItemCaseA(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel)

#Determines if the given vmu phase, record and source is to be skipped during the gap filling.    
def isVmuRecordIDrelevant(dbCon, gapItemVmuRecordID):
    #Is this specific Phase, recorn or source relevant? is this generic source relevant?
    return True

#Determines if the given HRD channel is to be skipped during the gap filling.    
def isHrdChanelrelevant(dbCon, gapItemChanel):
    #Is this specific Phase, record or source relevant? is this generic source relevant?
    return True
    
#Procedure that coordinate the various vmu_packet_gap items insertion into replay items. As vmu_packet_gap items are per vmu phase, record and source, an initial merge is performed in order to not have duplicated replay items which are per start end end datetime exclusivelly.
def insertHrdGapItem2ReplayList():
    #Make database connection
    dbCon = dbConnectToDatabase()
    if dbCon:
        #Query for all unchecked gapItems on vmu_packet_gap
        dbCur = dbCon.cursor()
        queryStatement = 'SELECT hrdpaga.id, hrdpaga.last_timestamp, hrdpaga.next_timestamp, hrdpaga.chanel FROM hrd_packet_gap hrdpaga \
        WHERE hrdpaga.is_checked = 0;'
        dbCur.execute(queryStatement)
        queryResults = dbCur.fetchall()
        logger.debug(queryStatement)
        #if results, we continue for insertion
        for row in queryResults:
            #Parse the relevant data obtained from the database
            gapItemID = row[0]
            gapItemStartDate = row[1]
            gapItemEndDate = row[2]
            gapItemChanel = row[3]
            
            #is VMU Phase, Recordname or Source relevant?
            if isHrdChanelrelevant(dbCon, gapItemChanel):
                #for each, decide the type of case and act acordingly
                #See documentation for extended information on GapItem to ReplayItem cases
                if isGapItemCaseD(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                elif isGapItemCaseE(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                elif isGapItemCaseC(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                elif isGapItemCaseF(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                elif isGapItemCaseB(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                elif isGapItemCaseG(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel):
                    pass
                #Case A, H, and other
                else:
                    GapItemCaseA(dbCon, gapItemID, gapItemStartDate, gapItemEndDate, gapItemChanel)
                    
        dbCon.close()
        

#Returns a vmu_record item database ID from an input vmu phase, record and source. If it doesn't exist it creates it on the database.    
def getVmuRecordDataID(dbCon, phaseName, recordName, source):
    dbCur = dbCon.cursor()
    #get Record ID
    queryStatement = 'SELECT vmure.id FROM vmu_record vmure \
    WHERE vmure.phase %s and vmure.recordname %s and vmure.source %s' % ("like \"%s\"" % phaseName if phaseName else "is null", "like \"%s\"" % recordName if recordName else "is null", "= %s" % source if source else "is null")
    dbCur.execute(queryStatement)
    logger.debug(queryStatement)
    queryResult = dbCur.fetchall()
    #New combination of VMU phase, record and source?
    if len(queryResult) > 0:
        #Existing entry, get id
        vmu_record_id = queryResult[0][0]
    else:
        #New combination, create a new entry and get id
        insertStatement = 'insert into vmu_record(timestamp, phase, recordname, source) \
        values(datetime("now"), %s, %s, %s);' % ("\"%s\"" % phaseName[:20] if phaseName else "null", "\"%s\"" % recordName[:20] if recordName else "null", "%s" % source if source else "null")
        logger.debug(insertStatement)
        print insertStatement
        dbCur.execute(insertStatement)
        vmu_record_id = dbCur.lastrowid
    
    dbCur.close()
    return vmu_record_id

#Returns a data source id from an input data source name. If it doesn't exist it creates it on the database.    
def getDataSource(dbCon, dataSourceName):
    dbCur = dbCon.cursor()
    #get Data Source ID
    queryStatement = 'SELECT id FROM data_source WHERE name like "%s";' % (dataSourceName)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #New source or existing one?
    if len(queryResult) > 0:
        #Existing, get id
        dataSourceID = queryResult[0][0]
    else:
        #New source, create new entry and get id
        insertStatement = 'insert into data_source(name) values("%s");' % (dataSourceName)
        logger.debug(insertStatement)
        dbCur.execute(insertStatement)
        dataSourceID = dbCur.lastrowid
    
    dbCur.close()
    return dataSourceID

#Creates an entry on the database table with the packet cound for a specific data source and vmu_record (phase, recordname, source)    
def insertVmuPacketCount(dbCon, dataSourceName, recordID, count):
    dbCur = dbCon.cursor()
    #get Data Source ID
    dataSourceID = getDataSource(dbCon, dataSourceName)
    insertStatement = 'INSERT INTO vmu_packet_count (TIMESTAMP, data_source_id, vmu_record_id, count) \
    VALUES (datetime("now"), %s, %s, %s);' % (dataSourceID, recordID, count)
    #Insert to Database
    logger.debug(insertStatement)
    dbCur.execute(insertStatement)
    dbCur.close()

#Creates a hrd_packet_gap item entry on the database.    
def insertHrdGapItem(dbCon, channel, startdate, last_sequence_count, enddate, next_sequence_count):
    dbCur = dbCon.cursor()
    
    #Does it exist already? Maybe contained into another gap? and unprocessed.
    queryStatement = 'SELECT id FROM hrd_packet_gap WHERE is_checked = 0 AND chanel = "%s" AND \
    (last_sequence_count >= %s AND next_sequence_count <= %s) AND \
    (last_timestamp >= "%s" AND next_timestamp <= "%s");' % (channel, last_sequence_count, next_sequence_count, startdate, enddate)
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #Existing?
    if len(queryResult) > 0:
        #Yes, get id
        hrdItemID = queryResult[0][0]
    else:
        #No, Insert & get ID
        #Build insert statement
        insertStatement = 'INSERT INTO hrd_packet_gap (TIMESTAMP, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, is_checked, chanel) \
        VALUES (datetime("now"), %s, "%s", %s, "%s", 0, "%s");' % (last_sequence_count, startdate, next_sequence_count, enddate, channel)
        #Insert to Database
        logger.debug(insertStatement)
        dbCur.execute(insertStatement)
        hrdItemID = dbCur.lastrowid
        
    dbCur.close()
    return hrdItemID

#Creates a vmu_packet_gap item entry on the database.    
def insertVmuGapItem(dbCon, phaseName, recordName, source, startdate, last_sequence_count, enddate, next_sequence_count, hrd_packet_gap_id):
    dbCur = dbCon.cursor()
    #get Record ID
    vmu_record_id = getVmuRecordDataID(dbCon, phaseName, recordName, source)
    insertStatement = 'INSERT INTO vmu_packet_gap (TIMESTAMP, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, is_checked, vmu_record_id, hrd_packet_gap_id) \
    VALUES (datetime("now"), %s, "%s", %s, "%s", 0, %s, %s);' % (last_sequence_count, startdate, next_sequence_count, enddate, vmu_record_id, hrd_packet_gap_id if hrd_packet_gap_id else "null")
    #Insert to Database
    logger.debug(insertStatement)
    dbCur.execute(insertStatement)
    dbCur.close()
    
#Returns the full data patch from the database. Normally /mainpool/FSL/Archive/Ops/HRDL/2/RealTime/    
def getFolderPath(dbCon, daysBack):
    baseDir = getVariableValue(dbCon, 'meex_data_path_basedir')
    #Compute year and day of year
    yearNumber = (datetime.now() - timedelta(days=daysBack, hours=12)).timetuple().tm_year
    doyNumber = (datetime.now() - timedelta(days=daysBack, hours=12)).timetuple().tm_yday
    
    return "%s%s%s%s" % (baseDir, yearNumber, "/", '%03d' % doyNumber)
     
#Procedure to scan the archive for gaps using the Meex software. It determines how many days in the past it needs to look and starts launching Meex list processes. The output is used to create vmu_packet_gap items, hrd_packet_gap items, and its link in the database. 
def scanForVmuHrdGaps():
    logger.info("Start scanning the archive for VMU/HRD gaps.")
    #Make the database connection
    dbCon = dbConnectToDatabase()
    if dbCon:
        daysBack = getVariableValue(dbCon, 'scan_days_back')
        meexCommandBin = getVariableValue(dbCon, 'meex_command_bin')
        #Iterate backwards starting from today
        for dayNumber in range(daysBack,0,-1):
            dataPath = getFolderPath(dbCon, dayNumber)
            #Compose scan command
            scanCommand = 'ionice -c3 %s list -e -k vmu %s | /opt/autobrm/bin/sqchk.awk |grep -vE "IMG|SCC"' % (meexCommandBin, dataPath)
            logger.debug(scanCommand)
            try:
                #Execute command
                gapOutput = commands.getstatusoutput(scanCommand)       
                if gapOutput[0] == 0:
                    #Parse results
                    gaps = gapOutput[1].split('\n')
                    for gapItem in gaps:
                        logger.debug(gapItem)
                        if '|' in gapItem:
                            #Split HRD and VMU part
                            hrd = {}
                            vmu = {}
                            hrd_string = gapItem.split('||')[0]
                            vmu_string = gapItem.split('||')[1]

                            #Determine sqchk.awk output entry item type: either 'G' or 'B'
                            ## 'G' for 'GAP' is a regular gap. Missing data between two received packets
                            ## 'B' for 'BAD' is a gap composed by one or more corrupt packets.
                            itemType = hrd_string.split('|')[0].strip()
                            
                            #Parse HRD line
                            try:
                                hrd['channel'] = hrd_string.split('|')[1].strip()
                                hrd['startdate'] = hrd_string.split('|')[2].strip()
                                hrd['last_sequence_count'] = hrd_string.split('|')[4].strip()
                                hrd['enddate'] = hrd_string.split('|')[3].strip()
                                hrd['next_sequence_count'] = hrd_string.split('|')[5].strip()
                                hrd['difference'] = int(hrd_string.split('|')[6].strip())
                            except Exception, errorString:
                                    logger.error(errorString)
                                    hrd['channel'] = None
                                    hrd['startdate'] = None
                                    hrd['last_sequence_count'] = None
                                    hrd['enddate'] = None
                                    hrd['next_sequence_count'] = None
                                    hrd['difference'] = None
                            
                            #Parse the VMU line
                            if itemType == 'G':
                                #This is a regular Gap
                                try:
                                    vmu['source'] = vmu_string.split('|')[0].strip()
                                    vmu['startdate'] = vmu_string.split('|')[1].strip()
                                    vmu['last_sequence_count'] = vmu_string.split('|')[3].strip()
                                    vmu['enddate'] = vmu_string.split('|')[2].strip()
                                    vmu['next_sequence_count'] = vmu_string.split('|')[4].strip()
                                    vmu['difference'] = int(vmu_string.split('|')[5].strip())
                                    #Parse user VMU record name
                                    upi_string = vmu_string.split('|')[6].strip()
                                    vmu['phaseName'] = None
                                    vmu['recordName'] = upi_string
                                    
                                except Exception, errorString:
                                    logger.error(errorString)
                                    vmu['source'] = None
                                    vmu['phaseName'] = None
                                    vmu['recordName'] = None
                                    vmu['difference'] = None
                                    
                            elif itemType == 'B':
                                #This is a corrupt packet Gap
                                vmu['source'] = vmu_string.split('|')[0].strip()
                                vmu['difference'] = None

                            #Save the data
                            #if no VMU gap sequence count skip is observed (hrd header sequence counts are not contiguous), then contonue to input the gap/corrupt range into the replay queue table
                            if ((hrd['difference'] >=  vmu['difference']) and itemType == 'G') or (itemType == 'B'):
                                #Save the data in the hrd_packet_gap table
                                if (hrd['difference'] > 0 and itemType == 'G') or (itemType == 'B'):
                                    ## If there is an HRD gap (VMU gaps doesn't always correspond to an HRD gap: i.e. Packet skipped when sent out by VMU) OR it is a gap involving corruption (bad)
                                    #### Query and get ID if gap contained in existing gap, otherwise Insert and get ID of the inserted entry.
                                    hrdItemID = insertHrdGapItem(dbCon, hrd['channel'], hrd['startdate'], hrd['last_sequence_count'], hrd['enddate'], hrd['next_sequence_count'])
                                else:
                                    hrdItemID = None

                                #Save the data in the vmu_packet_gap table & And link to HRD item (if any)
                                if itemType == 'G':
                                    ## Only if it a regular gap. A corrupt gap (bad) contains useless (corrupt) data.
                                    insertVmuGapItem(dbCon, vmu['phaseName'], vmu['recordName'], vmu['source'], vmu['startdate'], vmu['last_sequence_count'], vmu['enddate'], vmu['next_sequence_count'], hrdItemID)
                                
                        
            except Exception, errorString:
                logger.error(scanCommand)
                logger.error(errorString)
                        
        dbCon.close()
     
#Procedure to scan the archive in order to obtain the amount of packets per VMU phase, recordname and source. It determines how many days in the past it needs to look and starts launching Meex count processes. The output is used to create tiestamped vmu_packet_count items in the database.
def scanForVmuNumberOfFiles():
    logger.info("Start scanning the archive for packet counts.")
    #Make the database connection
    dbCon = dbConnectToDatabase()
    if dbCon:
        #Some initialization
        dataSourceName = 'hrdp meex'
        totalFiles = {}
        #Hoy many days back do we have to scan?
        daysBack = getVariableValue(dbCon, 'scan_days_back')
        meexCommandBin = getVariableValue(dbCon, 'meex_command_bin')

        #Iterate backwards over the days starting today
        for dayNumber in range(daysBack,0,-1):
            dataPath = getFolderPath(dbCon, dayNumber)
            scanCommand = 'ionice -c3 %s count -k hrd %s | egrep -av "(SCC/SCC|IMG/IMG)"' % (meexCommandBin, dataPath)
            logger.debug(scanCommand)
            try:
                #Execute command
                countOutput = commands.getstatusoutput(scanCommand)
                if countOutput[0] == 0:
                    #Parse output
                    counts = countOutput[1].split('\n')
                    #Populate the list
                    for countItem in counts:
                        if '|' in countItem:
                            try:
                                date = countItem.split('|')[0].strip()
                                source = countItem.split('|')[1].split('/')[0].strip()
                                #Parse user VMU record name
                                upi_string = countItem.split('|')[1].split('/')[2].strip()
                                phaseName = None
                                recordName = upi_string
                                
                                if re.match("^[0-9_]*$", countItem.split('|')[2].strip()) and re.match("^[0-9_]*$", countItem.split('|')[3].strip()):
                                    #Is Numeric count data
                                    packetCount = int(countItem.split('|')[2].strip())
                                    
                            except Exception, errorString:
                                logger.error(errorString)
                                date = None
                                source = None
                                phaseName = None
                                recordName = None
                                packetCount = None
                            
                            #if data: Save in a cumulative dictionary
                            if source is not None and phaseName is not None and recordName is not None and packetCount is not None:
                                vmuRecordID = getVmuRecordDataID(dbCon, phaseName, recordName, source)
                                if vmuRecordID in totalFiles:
                                    totalFiles.update({vmuRecordID: totalFiles[vmuRecordID] + packetCount})
                                else:
                                    totalFiles.update({vmuRecordID: packetCount})
                                
            except Exception, errorString:
                logger.error(errorString)
            
        #Save the data from the cumulative dictionary into the vmu_packet_count table           
        if len(totalFiles) > 0:
            for recordID, count in totalFiles.iteritems():
                insertVmuPacketCount(dbCon, dataSourceName, recordID, count)
        
        dbCon.close()
            
#Determines when it is Ok to start scanning the archive with Meex again. In order not to stress the archive unnecessary, a scan time offset is defined in the database.            
def isTime2Scan():
    dbCon = dbConnectToDatabase()
    scanFrequencyMinutes = getVariableValue(dbCon, 'scan_frequency_minutes')
    dbCur = dbCon.cursor()
    #What's the latest time we scanned: Give how many minutes since last data in DB
    queryStatement = 'SELECT (strftime("%s","now") - strftime("%s",MAX(TIMESTAMP)))/60 FROM vmu_packet_gap;'
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    dbCon.close()
    #Is there data? OR Is the data old enough?
    if queryResult[0][0] is None or queryResult[0][0] > scanFrequencyMinutes:
        return True
    else:
        return False

#Returns the next replay item to be processed by the tool. The database view queried has the sorting order.
def getNextReplayItem(dbCon):
    dbCur = dbCon.cursor()
    #Query retrieves next replay item to process.
    queryStatement = 'SELECT replayID, replayStatus, functionName FROM next_replay_in_queue LIMIT 1'
    #get Replay data ID
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    #Existing?
    if len(queryResult) > 0:
        #Parse query data and return it
        replayItemID = queryResult[0][0]
        replayItemState = queryResult[0][1]
        replayItemFunctionName = str(queryResult[0][2])
        returnValue = [replayItemID, replayItemState, replayItemFunctionName]
    else:
        #No data to return
        returnValue = [None, None, None]
        
    dbCur.close()
    return returnValue

#Returns the replay item start and end datetimes, from the replay id input argument.
def getReplayItemDetails(dbCon, replayItemID):
    dbCur = dbCon.cursor()
    queryStatement = 'SELECT strftime("%%Y.%%j.%%H.%%M.%%S",startdate) as startdate, strftime("%%Y.%%j.%%H.%%M.%%S",enddate) as enddate FROM replay WHERE id = %s;' % replayItemID
    logger.debug(queryStatement)
    dbCur.execute(queryStatement)
    queryResult = dbCur.fetchall()
    dbCur.close()
    #Get relevant data
    startDate = str(queryResult[0][0])
    endDate = str(queryResult[0][1])
    return [startDate, endDate]
 
#Launches the bitstream request process using the input arguments. The bitstream command line tool is run on the remote host (usually SDMKernel). Returns the brm exit code for parsing.
def issueBitstreamRequest(dbCon, source_user, source_ip, brm_instance, startDate, endDate, bitrate, brm_delivery_host_protocol, brm_delivery_host_ip, brm_delivery_host_port, brm_delivery_host_mission_mode, source, dataMode):
    bitstreamBin = getVariableValue(dbCon, 'bitstreamrequest_bin')
    bitstreamFile = getVariableValue(dbCon, 'bitstreamrequest_profile_file')
    #Compose the command witt the parameters
    brmCommand = 'ssh -q %s@%s \'java -jar %s \
    -file %s \
    -I %s \
    -startTime %s \
    -stopTime %s \
    -rate %s \
    -deliveryuri %s://%s:%s \
    -missionMode %s \
    -source %s \
    -dm %s\'' % (source_user, source_ip, bitstreamBin, bitstreamFile, brm_instance, startDate, endDate, bitrate, brm_delivery_host_protocol, brm_delivery_host_ip, brm_delivery_host_port, brm_delivery_host_mission_mode, source, dataMode)
    logger.info(brmCommand)
    #Launch the command
    commandOutputCode, commandOutputString = commands.getstatusoutput(brmCommand)
    #Parse the output
    commandExitCode = commandOutputCode >> 8
    commandSignalNum = commandOutputCode % 256
    logger.info('OutputStatus %s, ExitCode %s, SignalNum %s, Output %s' % (commandExitCode, commandExitCode, commandSignalNum,commandOutputString))   
    return [commandExitCode, commandOutputString]

#Collects the necesary replay item parameters neede to launch a bitstream request. Input is a replay item ID and DaSS source and data mode against where to place the bitstream request.
def processReplayBrm(replayItemID, source, dataMode):
    global globalCaduCounter

    #Reset CADU counter
    globalCaduCounter['ocurrences'] = 0

    #Make database connection
    dbCon = dbConnectToDatabase()
    if dbCon:
        #Get local host where to execute the command line bitstream request
        source_ip = getVariableValue(dbCon, 'bitstreamrequest_source_ip') 
        source_user = getVariableValue(dbCon, 'bitstreamrequest_source_user')
        #Get ReplayItem details & issue request
        brm_instance = getVariableValue(dbCon, 'bitstreamrequest_instance')
        brm_delivery_host_ip = getVariableValue(dbCon, 'bitstreamrequest_delivery_ip')
        brm_delivery_host_port = getVariableValue(dbCon, 'bitstreamrequest_delivery_port')
        brm_delivery_host_protocol = getVariableValue(dbCon, 'bitstreamrequest_delivery_protocol')
        brm_delivery_host_mission_mode = getVariableValue(dbCon, 'bitstreamrequest_mission_mode')
        #source = source
        bitrate = getVariableValue(dbCon, 'bitstreamrequest_bitrate')
        startDate, endDate = getReplayItemDetails(dbCon, replayItemID)
        #Launch
        try:
            outputCode, outputString = issueBitstreamRequest(dbCon, source_user, source_ip, brm_instance, startDate, endDate, bitrate, brm_delivery_host_protocol, brm_delivery_host_ip, brm_delivery_host_port, brm_delivery_host_mission_mode, source, dataMode)
        except Exception, errorString:
            logger.error(errorString)
            outputCode = -1
            outputString = errorString
        
        #Parse the Bitstream request exit code
        if outputCode == 0:
            #Command successful
            #Change Job state to next in line
            incrementReplayItemState(dbCon, replayItemID, 'Err %s: %s' % (outputCode, outputString[-90:]))
        else:
            #Command abnormal exit code, set as failed
            stateName = 'FAILED'
            #Change Job state to next in line
            setReplayItemState(dbCon, replayItemID, stateName, '%s: %s' % (outputCode, outputString[-90:]))
            
        dbCon.close()                 

#Specifies the specific DaSS source and data mode against where to place the bitstream request. In this case its a rate adapted AOS archive high rate data request.    
def processReplayBrmRT(replayItemID):
    logger.info("Starting a Bitstream request to the AOS Archive")
    source = 'COLVC_RealTime'
    dataMode = 'DaSSPlayback'
    #Launch the main bitstream procedure
    processReplayBrm(replayItemID, source, dataMode)

#Specifies the specific DaSS source and data mode against where to place the bitstream request. In this case its a rate adapted LOS archive high rate data request.    
def processReplayBrmExtPB(replayItemID):
    logger.info("Starting a Bitstream request to the LOS Archive")
    source = 'COLVC_PDSS_Playback'
    dataMode = 'DaSSPlayback'
    #Launch the main bitstream procedure
    processReplayBrm(replayItemID, source, dataMode)

#Scans the database for replay items in the queue to be executed and starts processing them.    
def issueReplayFromReplayList():
    global threadCommandLineBitstreamRequest
    
    #Make database connection
    dbCon = dbConnectToDatabase()
    if dbCon:
        #Get new ReplayItem in line to be process
        replayItemID, replayItemState, replayItemFunctionName = getNextReplayItem(dbCon)

        #Is no request is on-going; Is no request is taking too long ? Process request
        if (replayItemID is not None) and (not threadCommandLineBitstreamRequest.isAlive()):
            #Increment State
            incrementReplayItemState(dbCon, replayItemID)
            
            #Base on its state, act acordingly
            if replayItemFunctionName in globals():
                #Create new thread!
                threadCommandLineBitstreamRequest = threading.Thread(target = globals()[replayItemFunctionName], name='BitstreamClient', args=[replayItemID])
                #Call the function name defined on the table replay_status: processReplayBrmRT() or processReplayBrmExtPB() for instance
                threadCommandLineBitstreamRequest.start()
        
        dbCon.close()

#Read and return parameter current value from Yamcs
def getYamcsParameterValue(parameterName):
    logger.debug('Get parameter value from Yamcs')
    #Local/PDC
    server = 'yamcs-pdc-em.fsl'; username = '********'; password = '********'
    request = urllib2.Request("http://%s:8090/api/processors/fsl-em-dev/realtime/parameters/DaSS_PP/%s" % (server, parameterName))
    base64string = base64.b64encode('%s:%s' % (username, password))
    request.add_header("Authorization", "Basic %s" % base64string)
    result = urllib2.urlopen(request).read()
    resultJson = json.loads(result)
    
    logger.debug(resultJson)
    return resultJson
    
#Check if we receiving high rate data on the VC0 (Archive request)    
def isHRDbeingReceived(globalCaduCounter):
    parameterName = 'HRDFE_vc1_caduCount_PP'
    currentCaduCounter = getYamcsParameterValue(parameterName)
   
    #Is the VC0 Cadu counter increasing?   
    if (currentCaduCounter['engValue']['sint64Value'] > globalCaduCounter['count']):
        isReceivingData = True
        logger.debug('Receiving data')
        globalCaduCounter['count'] = currentCaduCounter['engValue']['sint64Value']
        globalCaduCounter['lastTimestamp'] = currentCaduCounter['generationTimeUTC']
        globalCaduCounter['ocurrences'] = 0
        
    else:
        isReceivingData = False
        logger.debug('NOT Receiving data')
        globalCaduCounter['count'] = currentCaduCounter['engValue']['sint64Value']
        globalCaduCounter['lastTimestamp'] = currentCaduCounter['generationTimeUTC']
        globalCaduCounter['ocurrences'] += 1
        logger.debug('Seting glocalCaduCounter ocurrences to %s' % globalCaduCounter['ocurrences'])

    return isReceivingData

#Check if the bitstream client request process stuck? If it's taking more time than expected to process it might be stuck.
def scanForStuckBitstreamClientRequest():
    global threadCommandLineBitstreamRequest
    global globalCaduCounter
    maxOcurrences = 72 #72 times 10 seconds makes for 12 minutes of no data while having an active BitsteamClient request
    
    #Initiate the 'global' Cadu counter variable
    if not 'count' in globalCaduCounter:
        logger.debug('Set globalCaduCounter variable')
        globalCaduCounter['count'] = 0; globalCaduCounter['lastTimestamp'] = ''; globalCaduCounter['ocurrences'] = 0
        
    #Check if HRD is coming in. If not we might have a problem of stuck BitstreamClient requests
    if threadCommandLineBitstreamRequest.isAlive() and not isHRDbeingReceived(globalCaduCounter) and globalCaduCounter['ocurrences'] > maxOcurrences:
        logger.info('BitstreamClient request is stuck -> Terminate')
        #Do we have a problem? -> Terminate BitstreamClient requests
        terminateBitstreamClient()
        #Reset counters
        logger.debug('Re-setting globalCaduCounter variable')
        globalCaduCounter['count'] = 0; globalCaduCounter['lastTimestamp'] = ''; globalCaduCounter['ocurrences'] = 0
    else:
        logger.debug('Scanning for stuck BitsteamClient requests: No stuck request found')
    
#Terminates any running (or stuck) Bitstream Client request
def terminateBitstreamClient():
        #Kill all stuck Bitstream Client
        logger.info('Terminating Bitstream Client requests (if any)')
        commandOutputCode, commandOutputString = commands.getstatusoutput('ssh -q hrdp@sdmkernel \'pkill -15 -f "bitstreamClient.jar"\'')
        logger.info('terminateBitstreamClient, ssh exit code: %s' % commandOutputCode)
        logger.info('terminateBitstreamClient, kill (sig 15) output: %s' % commandOutputString)
        commandOutputCode, commandOutputString = commands.getstatusoutput('ssh -q hrdp@sdmkernel \'pkill -9 -f "bitstreamClient.jar"\'')
        logger.info('terminateBitstreamClient, ssh exit code: %s' % commandOutputCode)
        logger.info('terminateBitstreamClient, kill (sig 9) output: %s' % commandOutputString)

#Tries to terminate the threads
def exitCleanUp():
    try:
        if threadScanForVmuHrdGaps.isAlive():
            logger.info('threadScanForVmuHrdGaps is alive')
            threadScanForVmuHrdGaps.join(1)
        if threadScanForVmuNumberOfFiles.isAlive():
            logger.info('threadScanForVmuNumberOfFiles is alive')
            threadScanForVmuNumberOfFiles.join(1)
        if threadCommandLineBitstreamRequest.isAlive():
            logger.info('threadCommandLineBitstreamRequest is alive')
            threadCommandLineBitstreamRequest.join(1)
        if threadScanForStuckBitstreamClientRequest.isAlive():
            logger.info('threadScanForStuckBitstreamClientRequest is alive')
            threadScanForStuckBitstreamClientRequest.join(1)
        if threadAutoLosSensingReplayFiller.isAlive():
            logger.info('threadAutoLosSensingReplayFiller is alive')
            threadAutoLosSensingReplayFiller.do_run = False
            threadAutoLosSensingReplayFiller.join(1)
        
        #Kill all stuck Bitstream Client
        sleep (2)
        terminateBitstreamClient()
    except Exception, errorString:
        logger.error(errorString)

#Set the process oom_score_adj score to have AutoBRM and specially its childs (MEEX) killed first in case of an out of memory situation on the operating system.
def setOomScoreAdj(pid):
    logger.debug('Setting process oom_score_adj score')
    _,_ = commands.getstatusoutput('echo 1000 > /proc/%s/oom_score_adj' % pid)
        
#Executes as soon as a SIGTERM signal is catched.        
def exitHandler(signal, frame):
    logger.debug('SIGTERM captured')
    raise SystemExit
   
#Process ID in PID file writing and clearing function
def pidfileOperation(pidfile, pidnumber):
    try:
        #Writes the PID to the file
        if pidnumber is not None:
            with open(pidfile, 'w') as file: file.write('%s' % pidnumber)
        #Empties the file
        else:
            open(pidfile, 'w').close()
        
    except Exception, errorString:
        logger.error(errorString)
   
#Main procedure. Iterates in an infinite loop launching the archive gap and count scan procedure, the gap item to replay item merge procedure, and the command line bitstream request execution procedure.
def main():
    global threadScanForVmuHrdGaps
    global threadScanForVmuNumberOfFiles
    global threadScanForStuckBitstreamClientRequest
    global threadAutoLosSensingReplayFiller
    
    try:
        #Create a Replay list from a Gap list
        insertHrdGapItem2ReplayList()
        #Launch Bitstream replays from the replay list
        issueReplayFromReplayList()
        
        #Scan the Archive for gaps and do the packet file count per VMU phase, record and source
        if not (threadScanForVmuHrdGaps.isAlive() or threadScanForVmuNumberOfFiles.isAlive()) and isTime2Scan():
            threadScanForVmuHrdGaps = threading.Thread(target = scanForVmuHrdGaps, name='scanForVmuHrdGaps')
            threadScanForVmuNumberOfFiles = threading.Thread(target = scanForVmuNumberOfFiles, name='scanForVmuNumberOfFiles')
            #Start scan in new threads
            threadScanForVmuHrdGaps.start()
            threadScanForVmuNumberOfFiles.start()
            
        #Scan for stuck BitstreamClient requests    
        threadScanForStuckBitstreamClientRequest = threading.Thread(target = scanForStuckBitstreamClientRequest, name='scanForStuckBitstreamClientRequest')
        threadScanForStuckBitstreamClientRequest.start()
        
        #Scan for relevant LOS and add to replay request queue
        if not threadAutoLosSensingReplayFiller.isAlive():
            threadAutoLosSensingReplayFiller = threading.Thread(target = autoLosSensingReplayFiller.main, name='autoLosSensingReplayFiller', args=("task",))
            threadAutoLosSensingReplayFiller.start()
        
    except Exception, errorString:
        logger.error(errorString)
      
#main
if __name__=='__main__':
    #Set up logging
    logfile = '/var/log/autobrm.log'

    #Set up exit handler
    signal.signal(signal.SIGTERM, exitHandler)
    
    #Local/PDC
    logging.basicConfig(level=logging.INFO)
    #logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    filehandler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s %(message)s')
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    
    #Populate PID file
    pidfile = '/var/run/autobrm.pid'
    pidnumber = os.getpid()
    pidfileOperation(pidfile, pidnumber)
    
    #Set the process oom_score_adj score
    setOomScoreAdj(pidnumber)
    
    #Instantiate threads & globals
    if 'threadScanForVmuHrdGaps' not in locals():
        threadScanForVmuHrdGaps = threading.Thread()
    if 'threadScanForVmuNumberOfFiles' not in locals():
        threadScanForVmuNumberOfFiles = threading.Thread()
    if 'threadCommandLineBitstreamRequest' not in locals():
        threadCommandLineBitstreamRequest = threading.Thread()
    if 'threadScanForStuckBitstreamClientRequest' not in locals():
        threadScanForStuckBitstreamClientRequest = threading.Thread() 
    if 'threadAutoLosSensingReplayFiller' not in locals():
        threadAutoLosSensingReplayFiller = threading.Thread() 
    if 'globalCaduCounter' not in locals():
        globalCaduCounter = {}
        
    #Launches the main procedure loop  
    logger.info('##############################################################')
    logger.info('OK: Beginning of script')
    try:
        while True:
            main()
            sleep(10)
    except (SystemExit, KeyboardInterrupt):
        logger.info('OK: Exit signal received')
        
    #Terminates threads and BitstreamClient requests
    exitCleanUp()
    #Clears PID file
    pidfileOperation(pidfile, None)
    
    logger.info('OK: Ending script')
    logger.info('##############################################################')
    logger.info('Sending SIGHUP')
    os.kill(os.getpid(), signal.SIGHUP)
    sleep(1)
    logger.info('Sending SIGTERM')
    os.kill(os.getpid(), signal.SIGTERM)
    sleep(1)
    logger.info('Sending SIGKILL')
    os.kill(os.getpid(), signal.SIGKILL)
    logger.info('OK: NOT OK, it didnt stop')
    logger.info('OK: End of script')
    logger.info('##############################################################')

