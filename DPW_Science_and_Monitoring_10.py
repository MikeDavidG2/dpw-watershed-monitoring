#-------------------------------------------------------------------------------
# Name:        DPW_Science_and_Monitoring_10.py
# Purpose:     TODO: comment on the whole script.
#
# Author:      mgrue
#
# Created:     22/11/2016
# Copyright:   (c) mgrue 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# TODO: setup emailing service
# TODO: setup processing data
# TODO: figure out how to run the get attachments function on a different schedule than the get data function.
#       Actually I could use the Get_DateAndTime function to return a day and I
#       could then have an if/else statement to activate the get attachments
#       function if the day == 01 (or something like that)
# Import modules

import arcpy
import ConfigParser
import datetime
import json
import math
import mimetypes
import os
import time
import logging
import smtplib
import string
import sys
import time
import urllib
import urllib2
import csv

from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.text import MIMEText

arcpy.env.overwriteOutput = True



#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                                    DEFINE MAIN
def main():
    #---------------------------------------------------------------------------
    #                               Set Variables

    # Variables to control which Functions are run
    run_Set_Logger         = True
    run_Get_DateAndTime    = True
    run_Get_Last_Data_Ret  = True
    run_Get_Token          = True
    run_Get_Attachments    = False
    run_Get_Data           = True
    run_Set_Last_Data_Ret  = False
    run_Process_Data       = True
    run_Get_Field_Mappings = True
    run_Append_Data        = True
    run_Email_Results      = False

    # Control CSV files
    control_CSVs           = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring'
    last_data_retrival_csv = control_CSVs + '\\LastDataRetrival.csv'
    add_fields_csv         = control_CSVs + '\\FieldsToAdd.csv'
    calc_fields_csv        = control_CSVs + '\\FieldsToCalculate.csv'
    delete_fields_csv      = control_CSVs + '\\FieldsToDelete.csv'
    map_fields_csv         = control_CSVs + '\\MapFields.csv'

    # Token and AGOL variables
    cfgFile     = r"U:\grue\Scripts\Testing_or_Developing\configFiles\accounts.txt"
    gtURL       = "https://www.arcgis.com/sharing/rest/generateToken"
    AGOfields   = '*'

    # Service URL that ends with .../FeatureServer
    ### Below is the service for the Test Photos service
    ##serviceURL  = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/service_e0c08b861bae4df895e6567c6199412f/FeatureServer'
    ##serviceURL  = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/service_24373c124c564417a03275b88b098b32/FeatureServer'
    serviceURL  = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/service_114e8ccf829743ceade2988eb7f06bc2/FeatureServer'
    queryURL    =  serviceURL + '/0/query'
    gaURL       =  serviceURL + '/CreateReplica'

    # Working locations and names
    wkgFolder   = r"U:\grue\Scripts\Testing_or_Developing\data"
    wkgGDB      = "DPW_Science_and_Monitoring_wkg.gdb"
    origFC      = "DPW_Data_orig"
    wkgFC       = 'DPW_Data_wkg'
        # There is no wkgPath variable yet since we will append the date and
        # time to it in a function below

    # Production locations and names
    prodFolder  = wkgFolder
    prodGDB     = "DPW_Science_and_Monitoring_prod.gdb"
    prodFC      = 'DPW_Data_prod'
    prodPath    = prodFolder + '\\' + prodGDB + '\\' + prodFC

    # Misc variables
    fileLog = r'U:\grue\Scripts\Testing_or_Developing\Logs\DPW_Science_and_Monitoring.log'
    errorSTATUS = 0
    #---------------------------------------------------------------------------
    # Set up the logger
    if (errorSTATUS == 0 and run_Set_Logger):
        try:
            #If you need to debug, set the level=logging.INFO to logging.DEBUG
            logging.basicConfig(filename = fileLog, level=logging.INFO)

            #Header for the log file
            logging.info('\n\n\n')
            logging.info('---------------------------------------------------' )
            logging.info('             ' + str(datetime.datetime.now()))
            logging.info('           Running DPW_Science_and_Monitoring.py')
            logging.info('---------------------------------------------------' )

        except Exception as e:
            errorSTATUS = Error_Handler('logger', e)

    #---------------------------------------------------------------------------
    # Get the current date and time to append to the end of various files
    # and the start time of the script to be used in the queries.
    if (errorSTATUS == 0 and run_Get_DateAndTime):
        try:
            dt_to_append, start_time = Get_DateAndTime()

        except Exception as e:
            errorSTATUS = Error_Handler('Get_DateAndTime', e)

    #---------------------------------------------------------------------------
    # Get the last time the data was retrieved from AGOL
    # so that it can be used in the where clauses
    if (errorSTATUS == 0 and run_Get_Last_Data_Ret):
        try:
            dt_last_ret_data = Get_Last_Data_Retrival(last_data_retrival_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Last_Data_Retrival', e)

    #---------------------------------------------------------------------------
    # Get the token
    if (errorSTATUS == 0 and run_Get_Token):
        try:
            token = Get_Token(cfgFile, gtURL)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Token', e)

    #---------------------------------------------------------------------------
    # Get the attachments from the online database and store it locally
    if (errorSTATUS == 0 and run_Get_Attachments):
        try:
            Get_Attachments(AGOfields, token, gaURL, wkgFolder, origFC, dt_to_append)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Attachments', e)

    #---------------------------------------------------------------------------
    # GET DATA from AGOL and store in: wkgFolder\wkgGDB\origFC_dt_to_append
    if (errorSTATUS == 0 and run_Get_Data):
        try:
            origPath = Get_Data(AGOfields, token, queryURL, wkgFolder, wkgGDB,
                                origFC, dt_to_append, dt_last_ret_data)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Data', e)

    #---------------------------------------------------------------------------
    # SET THE LAST TIME the data was retrieved from AGOL to the start_time
    # so that it can be used in the where clauses the next time the script is run
    if (errorSTATUS == 0 and run_Set_Last_Data_Ret):
        try:
            Set_Last_Data_Ret(last_data_retrival_csv, start_time)

        except Exception as e:
            errorSTATUS = Error_Handler('Set_Last_Data_Ret', e)
    #---------------------------------------------------------------------------
    # PROCESS the data
    if (errorSTATUS == 0 and run_Process_Data):
        try:
            wkgPath = Process_Data(wkgFolder, wkgGDB, wkgFC, origPath,
                                  dt_to_append, add_fields_csv, calc_fields_csv,
                                  delete_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Process_Data', e)

    #---------------------------------------------------------------------------
    # GET FIELD MAPPINGS
    if (errorSTATUS == 0 and run_Get_Field_Mappings):
        try:
            field_mappings = Get_Field_Mappings(wkgPath, prodPath, map_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Field_Mappings', e)

    #---------------------------------------------------------------------------
    # APPEND the data
    if (errorSTATUS == 0 and run_Append_Data):
        try:
            Append_Data(wkgPath, prodPath, field_mappings)

        except Exception as e:
            errorSTATUS = Error_Handler('Append_Data', e)

    #---------------------------------------------------------------------------
    # Email results
    if (run_Email_Results):
            try:
                Email_Results()

            except Exception as e:
                errorSTATUS = Error_Handler('Email_Results', e)
    #---------------------------------------------------------------------------
    #                  Print out info about the script


    if errorSTATUS == 0:
        print 'SUCCESSFULLY ran DPW_Science_and_Monitoring.py'
        logging.info('\n\n')
        logging.info('SUCCESSFULLY ran DPW_Science_and_Monitoring.py\n\n')

        ##raw_input('Press ENTER to continue...')

    else:
        print '\n*** ERROR!  There was an error with the script. See above for messages ***'
        logging.info('\n\n')
        logging.error('*** ERROR!  There was an error with the script. See above for messages ***\n\n')

        ##raw_input('Press ENTER to continue...')

            #Header for the log file
    logging.info('-----------------------------------------------------------' )
    logging.info('             ' + str(datetime.datetime.now()))
    logging.info('           Finished DPW_Science_and_Monitoring.py')
    logging.info('-----------------------------------------------------------' )


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#                                 DEFINE FUNCTIONS
#-------------------------------------------------------------------------------
#                        FUNCTION:    Get Start Date and Time
# Get the current date and time so that it can be used in multiple places
# where a unique name is desired
# TODO: Make this function get the official 'start' of the script so that any datetime requirement for the rest of the script uses this 'now()' function.
def Get_DateAndTime():
    print 'Getting Date and Time (dt)...'
    logging.info('Getting Date and Time...')

    start_time = datetime.datetime.now()
    date = '%s_%s_%s' % (start_time.year, start_time.month, start_time.day)
    time = '%s_%s_%s' % (start_time.hour, start_time.minute, start_time.second)
    dt_to_append = '%s__%s' % (date, time)

    print 'Successfully got Date and Time\n'
    logging.debug('Successfully got Date and Time\n')

    return dt_to_append, start_time

#-------------------------------------------------------------------------------
#                       FUNCTION:    Get_Last_Data_Retrival Date

def Get_Last_Data_Retrival(last_ret_csv):
    """The purpose of this function is to use a control CSV to read in the date
    that the AGOL data was last retrieved from.  The string is turned into a
    datetime obj that is returned back to main.  This is so we can use the
    datetime in our queries to limit the data that we pull down from AGOL in the
    'Get_Attachments()' and the 'Get_Data()' functions.
    """
    print 'Getting last data retrival...'
    logging.info('Getting last data retrival...')

    # Create a reader object that will read a Control CSV
    with open (last_ret_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Grab the data that is in the 2nd row, 1st column in the CSV
        # and store it as a string in last_ret_str
        row_num = 0
        for row in readCSV:
            if row_num > 1:
                last_ret_str = row[0]
            row_num += 1

        # Turn the string obtained from the CSV file into a datetime object
        dt_last_ret_data = datetime.datetime.strptime(last_ret_str, '%m/%d/%Y %I:%M:%S %p')
        ##last_ret_formatted = last_ret_obj.strftime('%m/%d/%Y %I:%M:%S %p')

    print '  Last data retrival happened at: %s' % str(dt_last_ret_data)
    logging.info('  Last data retrival happened at: %s' % str(dt_last_ret_data))

    print 'Successfully got the last data retrival\n'
    logging.info('Successfully got the last data retrival\n')

    return dt_last_ret_data

#-------------------------------------------------------------------------------
#                       FUNCTION:    Get AGOL token
# We first need to get a token from AGOL that will allow use to access the data
def Get_Token(cfgFile_, gtURL_):

    print "Getting Token..."
    logging.info("Getting Token...")

    configRMA = ConfigParser.ConfigParser()
    configRMA.read(cfgFile_)
    usr = configRMA.get("AGOL","usr")
    pwd = configRMA.get("AGOL","pwd")

    ##print '  Getting values'
    gtValues = {'username' : usr, 'password' : pwd, 'referer' : 'http://www.arcgis.com', 'f' : 'json' }
    gtData = urllib.urlencode(gtValues)
    ##print '  Getting Request'
    gtRequest = urllib2.Request(gtURL_,gtData)
    ##print '  Getting response'
    gtResponse = urllib2.urlopen(gtRequest)
    ##print '  Getting Json'
    gtJson = json.load(gtResponse)
    token = gtJson['token']

    print "Successfully retrieved token.\n"
    logging.debug("Successfully retrieved token.\n")
    ##print token

    return token

#-------------------------------------------------------------------------------
#                         FUNCTION:   Get Attachments
# Attachments (images) are obtained by hitting the REST endpoint of the feature
# service (gaURL_) and returning a URL that downloads a JSON file (which is a
# replica of the database).  The script then uses that downloaded JSON file to
# get the URL of the actual images.  The JSON file is then used to get the
# StationID and SampleEventID of the related feature so thye can be used to name
# the downloaded attachment.
#TODO: get this function to use the dt_last_ret_data to only get attachments from the recent samples
def Get_Attachments(AGOfields_, token_, gaURL_, wkgFolder_, origFC_, dt_to_append):
    print 'Getting Attachments...'
    logging.info('Getting Attachments...')

    #---------------------------------------------------------------------------
    #                       Get the attachments url (ga)
    ##print '  gaURL_ = '+ gaURL_
    # Set the values
    gaValues = {
    'f' : 'json',
    'replicaName' : 'Bacteria_TMDL_Replica',
    'layers' : '0',
    'geometryType' : 'esriGeometryPoint',
    'transportType' : 'esriTransportTypeUrl',
    'returnAttachments' : 'true',
    'returnAttachmentDatabyURL' : 'false',
    'token' : token_
    }
    # Get the URL
    gaData = urllib.urlencode(gaValues)
    gaRequest = urllib2.Request(gaURL_, gaData)
    gaResponse = urllib2.urlopen(gaRequest)
    gaJson = json.load(gaResponse)
    replicaUrl = gaJson['URL']
    ##print '  Replica URL: %s' % str(replicaUrl)

    # Set the token into the URL so it can be accessed
    replicaUrl_token = replicaUrl + '?&token=' + token_ + '&f=json'
    ##print '  Replica URL Token: %s' % str(replicaUrl_token)

    #---------------------------------------------------------------------------
    #                         Save the JSON file
    # Access the URL and save the file to the current working directory named
    # 'myLayer.json'.  This will be a temporary file and will be deleted
    # NOTE: the file is saved to the 'current working directory' + 'locToSaveJsonFile'
    locToSaveJsonFile = 'myLayer_%s.json' % dt_to_append

    # Save the file
    urllib.urlretrieve(replicaUrl_token, locToSaveJsonFile)

    # Allow the script to access the saved JSON file
    cwd = os.getcwd()  # Get the current working directory
    jsonFilePath = cwd + '\\' + locToSaveJsonFile # Path to the downloaded json file
    ##print '  JSON file saved to: ' + jsonFilePath
    logging.debug('  JSON file saved to: ' + jsonFilePath)

    #---------------------------------------------------------------------------
    #                       Save the attachments
    # Make the gaFolder (to hold attachments).
    gaFolder = wkgFolder_ + '\\TMDL_Attach_%s' % dt_to_append
    os.makedirs(gaFolder)

    # Open the JSON file
    with open (jsonFilePath) as data_file:
        data = json.load(data_file)

    # Save the attachments
    # Loop through each 'attachment' and get its parentGlobalId so we can name it
    #  based on its corresponding feature
    ##print '  Saving attachments:'
    logging.debug('  Saving attachments:')

    for attachment in data['layers'][0]['attachments']:
        ##print '\nAttachment: '
        ##print attachment
        gaRelId = attachment['parentGlobalId']
        ##print 'gaRelId:'
        ##print gaRelId

        # Now loop through all of the 'features' and stop once the corresponding
        #  GlobalId's match so we can save based on the 'StationID'
        #  and 'SampleEventID'
        for feature in data['layers'][0]['features']:
            origId = feature['attributes']['globalid']
            ##print '  origId' + origId
            origName1 = feature['attributes']['StationID']
            origName2 = str(feature['attributes']['SampleEventID'])
            if origId == gaRelId:
                break

        attachName = '%s__%s' % (origName1, origName2)
        ##print '  attachName = ' + attachName
        # 'i' and 'dupList' are to prevent the possibility that there could be
        #  multiple photos with the same StationID and SampleEventID.  If they
        #  do have the same attributes as an already saved attachment, the letter
        #  suffix at the end of the attachment name will increment to the next
        #  letter.  Ex: if there are two SDR-100__9876, the first will always be
        #  named 'SDR-1007__9876_A.jpg', while the second will be 'SDR-1007__9876_B'
        i = 0
        dupList = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
        attachPath = gaFolder + '\\' + attachName + '_' + dupList[i] + '.jpg'

        # Test to see if the attachPath currently exists
        while os.path.exists(attachPath):
            # The path does exist, so go through the dupList until a 'new' path is found
            i += 1
            attachPath = gaFolder + '\\' + attachName + '_' + dupList[i] + '.jpg'
            # Test the new path to see if it exists.  If it doesn't exist, break out
            # of the while loop to save the image to that new path
            if not os.path.exists(attachPath):
                break


        # Get the URL and data for the attachment
        gaUrl = attachment['url']
        gaValues = {'token' : token_ }
        gaData = urllib.urlencode(gaValues)

        # Get the attachment and save as attachPath
        ##print '    %s' % attachName
        logging.debug('    %s' % attachName)

        urllib.urlretrieve(url=gaUrl, filename=attachPath,data=gaData)

    print '  Attachments saved to: %s' % gaFolder
    logging.info('  Attachments saved to: %s' % gaFolder)

    # Delete the JSON file since it is no longer needed.
    ##print '  Deleting JSON file'
    logging.debug('  Deleting JSON file')
    os.remove(jsonFilePath)

    print 'Successfully got attachments.\n'
    logging.debug('Successfully got attachments.\n')

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION:    Get AGOL Data
#############################################################################################################
### http://blogs.esri.com/esri/arcgis/2013/10/10/quick-tips-consuming-feature-services-with-geoprocessing/
### https://geonet.esri.com/thread/118781
### WARNING: Script currently only pulls up to the first 10,000 (1,000?) records - more records will require
###     a loop for iteration - see, e.g., "Max Records" section at the first (blogs) URL listed above or for
###     example code see the second (geonet) URL listed above
#############################################################################################################

def Get_Data(AGOfields_, token_, queryURL_, wkgFolder_, wkgGDB_, origFC_, dt_to_append, dt_last_ret_data_):
    """ The Get_Data function takes the token we obtained from 'Get_Token'
    function, establishs a connection to the data, creates a FGDB (if needed),
    creates a unique FC to store the data, and then copies the data from AGOL
    to the unique FC"""

    print "Getting data..."
    logging.info("Getting data...")

    #---------------------------------------------------------------------------
    #Set the feature service URL (fsURL = the query URL + the query)

    # TODO: work with the below clause to find errors that can be sent to the error handler to explain that the dates are resulting in no data to download
    # We want to set the 'where' clause to get records where the [CreationDate]
    # field is between the date the data was last retrieved and tomorrow.
    # This is so we will make sure to grab the most recent data (data that is
    # collected on the day the script is run).
    # The BETWEEN means that data collected on the first date will be retrieved,
    # while the data collected on the second date will not be retrieved
    # For ex: If data is collected on the 28th, and 29th and the where clause is:
    #   BETWEEN the 28th and 29th. You will get the data collected on the 28th only
    #   BETWEEN the 29th and 30th. You will get the data collected on the 29th only
    #   BETWEEN the 28th and 30th. You will get the data collected on the 28th AND 29th

    # TODO: May have to play with the 'days' variable below to make sure the data is retrieved properly
    plus_one_day = datetime.timedelta(days=1)
    now = datetime.datetime.now()
    tomorrow = now + plus_one_day
    ##print '  tomorrow: ' + str(tomorrow)

    # Use the dt_last_ret_data variable and tomorrow variable to set the 'where'
    # clause
    where = "CreationDate BETWEEN '{dt.year}-{dt.month}-{dt.day}' and '{tom.year}-{tom.month}-{tom.day}'".format(dt = dt_last_ret_data_, tom = tomorrow)
    print '  Getting data where: {}'.format(where)
    logging.debug('  Getting data where: {}'.format(where))

    # Encode the where statement so it is readable by URL protocol (ie %27 = ' in URL
    # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding
    where_encoded = urllib.quote(where)

    # If you suspect the where clause is causing the problems, uncomment the below 'where = "1=1"' clause
    ##where = "1=1"
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where_encoded,AGOfields_,token_)
    fsURL = queryURL_ + query

    #Get connected to the feature service
    ##print '  Feature Service: %s' % str(fsURL)
    fs = arcpy.FeatureSet()
    fs.load(fsURL)

    #---------------------------------------------------------------------------
    #Create working FGDB if it does not already exist.  Leave alone if it does...
    ##Create_FGDB(wkgFolder_, wkgGDB_)
    FGDB_path = wkgFolder_ + '\\' + wkgGDB_
    if os.path.exists(FGDB_path):
        ##print '  %s \n    already exists. No need to create it.' % FGDB_path
        pass
    else:
        print '  Creating FGDB: %s at: %s' % (wkgGDB_, wkgFolder_)
        logging.info('  Creating FGDB: %s at: %s' % (wkgGDB_, wkgFolder_))
        # Process
        arcpy.CreateFileGDB_management(wkgFolder_,wkgGDB_)

    #---------------------------------------------------------------------------
    #Copy the features to the FGDB.
    origFC_ = '%s_%s' % (origFC_,dt_to_append)
    origPath_ = wkgFolder_ + "\\" + wkgGDB_ + '\\' + origFC_
    print '  Copying features to: %s' % origPath_
    logging.info('  Copying features to: %s' % origPath_)

    # Process
    arcpy.CopyFeatures_management(fs,origPath_)

    #---------------------------------------------------------------------------
    print "Successfully retrieved data.\n"
    logging.debug("Successfully retrieved data.\n")

    return origPath_

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                    FUNCTION: Set Last Data Retrival time
def Set_Last_Data_Ret(last_ret_csv, start_time):
    # TODO: write a function synopsis
    """This function is """
    print 'Setting start_time to CSV...'
    logging.info('Setting start_time to CSV...')

    #---------------------------------------------------------------------------
    # Get original data from the CSV and make a list out of it
    with open (last_ret_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')
        orig_rows = []

        row_num = 0
        for row in readCSV:
            orig_row = row[0]

            orig_rows.append(orig_row)
            row_num += 1

    for orig_row in orig_rows:
        print 'Orig data: ' + orig_row
    print ''


    #---------------------------------------------------------------------------
    # overwrite the original file, BUT write the original information to it
    # except for the date and time which is obtained from the start_time var

    # Format dt so that is in the expected format AND in a list so it is written
    #  w/o extra commas when using 'writerow' command below
    formated_dt = [start_time.strftime('%m/%d/%Y %I:%M:%S %p')]

    with open (last_ret_csv, 'wb') as csv_file:
        writeCSV = csv.writer(csv_file)
        row_num = 0
        for row in orig_rows:
            # This copies the first two rows from the orig script to the new one
            if row_num < 2:
                print 'Writing: ' + row
                writeCSV.writerow([row])
            # This writes the new start time to the third row
            else:
                print 'New time: %s' % formated_dt[0]
                writeCSV.writerow(formated_dt)
            row_num += 1

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION:  Process Data

# TODO: find a way to delete accidental multiple sent surveys.  This happens if
#    the monitor hits 'Continue this survey' while they are editing the survey.

# TODO: rename this function Copy_Orig_Data and have it simply copy the original data to working data.  Remove the sub functions and have them full functions on their own

def Process_Data(wkgFolder_, wkgGDB_, wkgFC_, origPath_, dt_to_append,
                 add_fields_csv, calc_fields_csv, delete_fields_csv):
    print 'Processing Data...'
    logging.info('Processing Data...')

    #---------------------------------------------------------------------------
    # Copy the orig FC to a working TABLE to run processing on.
    in_features = origPath_
    wkgPath = out_feature_class = r'%s\%s\%s_%s' % (wkgFolder_, wkgGDB_, wkgFC_, dt_to_append)

    print '  Copying Data...'
    print '    From: ' + in_features
    print '    To:   ' + out_feature_class

    # Process
    arcpy.CopyRows_management(in_features, out_feature_class)

    #---------------------------------------------------------------------------
    # Below are sub functions that the Process_Data function calls
    # Add Fields
    __Add_Fields(wkgPath, add_fields_csv)

    # Calculate fields
    __Calculate_Fields(wkgPath, calc_fields_csv)

    # Delete fields (may not need to use this function)
    ##__Delete_Fields(wkgPath, delete_fields_csv)

    #---------------------------------------------------------------------------
    # TODO: Create an Lat and Long column and calculate the geometry of the point
    #    if the [site_location_correct] field is 'no'. Then calculate [site_lat]
    #    and [site_long] based off of those calculated fields.
        #---------------------------------------------------------------------------

    print 'Successfully processed data.\n'
    logging.debug('Successfully processed data.\n')

    return wkgPath
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        SUB-FUNCTION: ADD FIELDS
#TODO: make these full fledged functions
def __Add_Fields(wkg_data, add_fields_csv):
    print '  ------------------------------------------------------------------'
    print '  Adding fields to:\n    %s' % wkg_data
    with open (add_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Create blank lists
        f_names = []
        f_types = []
        f_lengths = []
        ##f_aliases = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                f_name   = row[0]
                f_type   = row[1]
                f_length = row[2]
                ##f_alias  = row[3]

                f_names.append(f_name)
                f_types.append(f_type)
                f_lengths.append(f_length)
                ##f_aliases.append(f_alias)
            row_num += 1

    num_new_fs = len(f_names)
    print '    There are %s new fields to add.\n' % str(num_new_fs)

    f_counter = 0
    while f_counter < num_new_fs:
        print ('    Creating field: %s, with a type of: %s, and a length of: %s'
        % (f_names[f_counter], f_types[f_counter], f_lengths[f_counter])) ##, f_aliases[f_counter]))

        # TODO: Add the arcpy command to create a field here
        in_table          = wkg_data
        field_name        = f_names[f_counter]
        field_type        = f_types[f_counter]
        field_precision   = '#'
        field_scale       = '#'
        field_length      = f_lengths[f_counter]
        field_alias       = '#'
        field_is_nullable = '#'
        field_is_required = '#'
        field_domain      = '#'


        try:
            # Process
            arcpy.AddField_management(in_table, field_name, field_type,
                        field_precision, field_scale, field_length, field_alias,
                        field_is_nullable, field_is_required, field_domain)
        except Exception as e:
            print '*** WARNING! Field: %s was not able to be added.***' % field_name
            print str(e)
        f_counter += 1

    print '  Successfully added fields.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                     SUB-FUNCTION: CALCULATE FIELDS

def __Calculate_Fields(wkg_data, calc_fields_csv):
    print '  ------------------------------------------------------------------'
    print '  Calculating fields in:\n  %s' % wkg_data

    # Make a table view so we can perform selections
    arcpy.MakeTableView_management(wkg_data, 'wkg_data_view')

    #---------------------------------------------------------------------------
    #                     Get values from the CSV file
    with open (calc_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        where_clauses = []
        calc_fields = []
        calcs = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                where_clause = row[0]
                calc_field   = row[2]
                calc         = row[4]

                where_clauses.append(where_clause)
                calc_fields.append(calc_field)
                calcs.append(calc)
            row_num += 1

    num_calcs = len(where_clauses)
    print '    There are %s calculations to perform.\n' % str(num_calcs)

    #---------------------------------------------------------------------------
    #                    Select features and calculate them
    f_counter = 0
    while f_counter < num_calcs:
        #-----------------------------------------------------------------------
        #               Select features using the where clause

        in_layer_or_view = 'wkg_data_view'
        selection_type   = 'NEW_SELECTION'
        my_where_clause  = where_clauses[f_counter]

        print '    Selecting features where: "%s"' % my_where_clause

        # Process
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, selection_type, my_where_clause)

        #-----------------------------------------------------------------------
        #              If features selected, perform the calculation

        countOfSelected = arcpy.GetCount_management(in_layer_or_view)
        count = int(countOfSelected.getOutput(0))
        print '      There was/were %s feature(s) selected.' % str(count)

        if count != 0:
            in_table   = in_layer_or_view
            field      = calc_fields[f_counter]
            calc       = calcs[f_counter]

            # Test if the user wants to calculate the field being equal to
            # ANOTHER FIELD by seeing if the calculation starts or ends with an '!'
            if (calc.startswith('!') or calc.endswith('!')):
                f_expression = calc

                try:
                    # Process
                    arcpy.CalculateField_management(in_table, field, f_expression, expression_type="PYTHON_9.3")

                    print ('      From selected features, calculated field: %s, so that it equals FIELD: %s\n'
                            % (field, f_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***' % field
                    print str(e)

            # If calc does not start or end with a '!', it is probably because the
            # user wanted to calculate the field being equal to a STRING
            else:
                s_expression = "'%s'" % calc

                try:
                # Process
                    arcpy.CalculateField_management(in_table, field, s_expression, expression_type="PYTHON_9.3")

                    print ('      From selected features, calculated field: %s, so that it equals STRING: %s\n'
                            % (field, s_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***' % field
                    print str(e)

        else:
            print ('      WARNING.  No records were selected.  Did not perform calculation.\n')

        #-----------------------------------------------------------------------

        # Clear selection before looping back through to the next selection
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, 'CLEAR_SELECTION')

        f_counter += 1

    print '  Successfully calculated fields.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          SUB-FUNCTION DELETE FIELDS

def __Delete_Fields(wkg_data, delete_fields_csv):
    print '-------------------------------------------------------------------'
    print '  Deleting fields in:\n    %s' % wkg_data

    #---------------------------------------------------------------------------
    #                     Get values from the CSV file
    with open (delete_fields_csv) as csv_file:
        readCSV  = csv.reader(csv_file, delimiter = ',')

        delete_fields = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                delete_field = row[0]
                delete_fields.append(delete_field)
            row_num += 1

    num_calcs = len(delete_fields)
    print '    There is/are %s deletion(s) to perform.\n' % str(num_calcs)

    #---------------------------------------------------------------------------
    #                          Delete fields

    # If there is at least one field to delete, delete it
    if num_calcs > 0:
        f_counter = 0
        while f_counter < num_calcs:
            drop_field = delete_fields[f_counter]
            print '    Deleting field: %s...' % drop_field

            arcpy.DeleteField_management(wkg_data, drop_field)

            f_counter += 1

        print '  Successfully deleted fields.\n'

    else:
        print '  WARNING.  NO fields were deleted.'

    # TODO: Delete the following fields:
    #    location_calculation.  This is only used for the survey.
    #    <list other fields to delete here.  These should only be deleted after all
    #    other processing has been completed.
    #    This function may not be needed since we are appending the working database to the prod database.

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION: GET FIELD MAPPINGS
def Get_Field_Mappings(orig_table, prod_table, map_fields_csv):
    print 'Getting Field Mappings...'

    #---------------------------------------------------------------------------
    #                      Get values from the CSV file
    with open (map_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Create blank lists
        orig_fields = []
        prod_fields = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                # Get values for that row
                orig_field = row[0]
                prod_field = row[1]

                orig_fields.append(orig_field)
                prod_fields.append(prod_field)

            row_num += 1

    num_fm = len(orig_fields)
    print '  There are %s non-default fields to map.\n' % str(num_fm)

    #---------------------------------------------------------------------------
    #           Set the Field Maps into the Field Mapping object
    # Create FieldMappings obj
    fms = arcpy.FieldMappings()

    # Add the schema of the production table so that all fields that are exactly
    # the same name between the orig table and the prod table will be mapped
    # to each other by default
    fms.addTable(prod_table)

    # Loop through each pair of listed fields (orig and prod) and add the
    # FieldMap object to the FieldMappings obj
    counter = 0
    while counter < num_fm:
        print ('  Mapping Orig Field: "%s" to Prod Field: "%s"'
                                 % (orig_fields[counter], prod_fields[counter]))
        # Create FieldMap
        fm = arcpy.FieldMap()

        # Add the input field
        fm.addInputField(orig_table, orig_fields[counter])

        # Add the output field
        out_field_obj = fm.outputField
        out_field_obj.name = prod_fields[counter]
        fm.outputField = out_field_obj

        # Add the FieldMap to the FieldMappings
        fms.addFieldMap(fm)

        del fm
        counter += 1

    print '  Field Mappings:\n  %s  \n\nSuccessfully Got Field Mappings\n' % fms
    return fms

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION:  APPEND DATA
def Append_Data(wkgPath_, prodPath_, field_mappings_):

    print 'Appending Data...'
    print '  From: ' + wkgPath_
    print '  To:   ' + prodPath_
    logging.info('Appending data...\n     From: %s\n     To:   %s' % (wkgPath_, prodPath_))

    # Append working data to production data
    inputs = wkgPath_
    target = prodPath_
    schema_type = 'NO_TEST'
    field_mapping = field_mappings_

    # Process
    arcpy.Append_management(inputs, target, schema_type, field_mapping)

    print 'Successfully appended data.\n'
    logging.debug('Successfully appended data.\n')

#-------------------------------------------------------------------------------
#                                   Email results
def Email_Results():
    print '\nEmailing Results...'
    logging.info('Emailing Results...')

    print '  This is a placeholder for future script.  This function is empty.'
    logging.debug('  This is a placeholder for future script.  This function is empty.')

    print 'Successfully emailed results.\n'
    logging.debug('Successfully emailed results.\n')

#-------------------------------------------------------------------------------
#                                     Error Handler
def Error_Handler(func_w_err, e):

    e_str = str(e)
    print '\n*** ERROR in function: "%s" ***\n' % func_w_err
    logging.error('\n*** ERROR in function: %s ***\n' % func_w_err)
    print '    error: = %s' % e_str
    logging.error(' error: = %s ' % e_str)

    #---------------------------------------------------------------------------
    #                                Help comments
    # TODO: have the help comments be a part of a list so that I can print out a list of help comments.
    help_comment = ''

    # Help comments for 'Get_Token' function

    if (func_w_err == 'Get_Token'):
        if (e_str.startswith('No section:')):
            help_comment = '    The section in brakets in "cfgFile" variable may not be in the file, or the file cannot be found.  Check both!'

    # Help comments for 'Get_Attachments' function
    if (func_w_err == 'Get_Attachments'):
        if e_str == "'*'":
            help_comment = '    This error may be the result that the field you are using to parse the data from is not correct.  Double check your fields.'
        if e_str == "'URL'":
            help_comment = '    This error may be the result of the feature layer may not be shared.\n    Or user in the "cfgFile" may not have permission to access the URL.  Try logging onto AGOL with that user account to see if that user has access to the database.\n    Or the problem may be that the feature layer setting doesn\'t have Sync enabled.\n    Or the URL is incorrect somehow.'

    # Help comments for 'Get_Data' function
    if (func_w_err == 'Get_Data'):
        if e_str == 'RecordSetObject: Cannot load a table into a FeatureSet':
            help_comment =  '    Error may be the result of a bad query format.  Try the query "where = \'1=1\'" to see if that query works.\n    Or the dates you are using to select does not have any data to download.'
        if e_str == 'RecordSetObject: Cannot open table for Load':
            help_comment = '     Error may be the result of the Feature Service URL not being correctly set.'

    # Help comments for 'Append_Data' function
    if(func_w_err == 'Append_Data'):
        pass

    #---------------------------------------------------------------------------
    if (help_comment == ''):
        pass
    else:
        print '    Help Comment: ' + help_comment
        logging.error('    Help Comment: ' + help_comment)

    # Change errorSTATUS to 1 so that the script doesn't try to perform any
    # other functions
    errorSTATUS = 1
    return errorSTATUS

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                                  RUN MAIN
if __name__ == '__main__':
    main()
