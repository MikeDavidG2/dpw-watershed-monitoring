#-------------------------------------------------------------------------------
# Name:        DPW_Science_and_Monitoring_10.py
# Purpose:     TODO: comment on the whole script.
#
# Author:      mgrue
#
# Created:     22/11/2016
# Copyright:   (c) mgrue 2016
# Licence:     <your licence>
# Modified:    02/24/2017
#-------------------------------------------------------------------------------
# TODO: setup emailing service
# TODO: figure out how to run the get attachments function on a different schedule than the get data function.
#       Actually I could use the Get_DateAndTime function to return a day and I
#       could then have an if/else statement to activate the get attachments
#       function if the day == 01 (or something like that)
# TODO: May need a 'Date Survey Filled Out' field to distinguis it from the 'Date Survey Submitted' field...
# TODO: Try to have the print statements get added to a log file

# Import modules

import arcpy
import ConfigParser
import datetime
import json
import math
import mimetypes
import os
import time
##import logging
import smtplib
import string
import sys
import time
import urllib
import urllib2
import csv

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
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
    run_Set_Logger              = True
    run_Get_DateAndTime         = True
    run_Write_Print_To_Log      = True
    run_Get_Last_Data_Ret       = True
    run_Get_Token               = True
    run_Get_Data                = True
    run_Get_Attachments         = True # Requires 'run_Get_Data = True'
    run_Set_Last_Data_Ret       = True # Should be 'False' if testing
    run_Copy_Orig_Data          = True  # Requires 'run_Get_Data = True'
    run_Add_Fields              = True  # Requires 'run_Copy_Orig_Data = True'
    run_Calculate_Fields        = True  # Requires 'run_Copy_Orig_Data = True'
    run_Delete_Fields           = False # Requires 'run_Copy_Orig_Data = True'
    run_New_Loc_LocDesc         = True
    run_FC_To_Table             = True
    run_Get_Field_Mappings      = True  # Requires 'run_Copy_Orig_Data = True'
    run_Append_Data             = True  # Requires 'run_Copy_Orig_Data = True'
    run_Export_To_Excel         = True
    run_Sites_Data_To_Survey123 = True
    run_Email_Results           = True

    # Email lists
    ##dpw_email_list   = ['michael.grue@sdcounty.ca.gov', 'Joanna.Wisniewska@sdcounty.ca.gov', 'Ryan.Jensen@sdcounty.ca.gov', 'Steven.DiDonna@sdcounty.ca.gov', 'Kenneth.Liddell@sdcounty.ca.gov']
    dpw_email_list   = ['michael.grue@sdcounty.ca.gov', 'mikedavidg2@gmail.com']
    lueg_admin_email = ['michael.grue@sdcounty.ca.gov', 'mikedavidg2@gmail.com']#['Michael.Grue@sdcounty.ca.gov', 'Gary.Ross@sdcounty.ca.gov', 'Randy.Yakos@sdcounty.ca.gov']

    # Control CSV files
    control_CSVs           = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\Master'
    last_data_retrival_csv = control_CSVs + '\\LastDataRetrival.csv'
    add_fields_csv         = control_CSVs + '\\FieldsToAdd.csv'
    calc_fields_csv        = control_CSVs + '\\FieldsToCalculate.csv'
    delete_fields_csv      = control_CSVs + '\\FieldsToDelete.csv'
    map_fields_csv         = control_CSVs + '\\MapFields.csv'
    report_TMDL_csv        = control_CSVs + '\\Report_TMDL.csv'

    # Token and AGOL variables
    cfgFile     = r"U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\Master\accounts.txt"
    gtURL       = "https://www.arcgis.com/sharing/rest/generateToken"
    AGOfields   = '*'

    # Service URL that ends with .../FeatureServer
    ### Below is the service for the Test Photos service
    ##serviceURL  = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/service_e0c08b861bae4df895e6567c6199412f/FeatureServer'
    serviceURL  = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/service_9405c12d48364f03815cae025a981d18/FeatureServer'
    queryURL    =  serviceURL + '/0/query'
    gaURL       =  serviceURL + '/CreateReplica'

    # Working database locations and names
    wkgFolder   = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\Data'
    wkgGDB      = "DPW_Science_and_Monitoring_wkg.gdb"
    origFC      = "A_DPW_Data_orig"
    wkgFC       = 'B_DPW_Data_wkg'
      # There is no wkgPath variable yet since we will append the date and
      # time to it in a function below

    # Production locations and names
    prodGDB       = wkgFolder+ "\\DPW_Science_and_Monitoring_prod.gdb"
    prodPath_FldData       = prodGDB + '\\Field_Data'
    prodPath_SitesData     = prodGDB + '\\Sites_Data'
    prod_attachments       = wkgFolder + '\\Sci_Monitoring_pics'
    prodPath_Excel         = wkgFolder + '\\Excel'

    # Survey123 CSV file related variables
    # site_info = the CSV Survey123 uses to locate the sites in the app. It gets
    # its data refreshed from the Sites_Data Feature Class in the
    # Sites_Data_To_Survey123_csv() function
    site_info = r"C:\Users\mgrue\ArcGIS\My Survey Designs\DPW Sci and Mon DEV\media\Site_Info.csv"
    Sites_Export_To_CSV_tbl = wkgFolder + '\\' + wkgGDB + '\\E_Sites_Data_export_to_csv'

    # Misc
    log_file = wkgFolder + r'\Logs\DPW_Science_and_Monitoring'
    errorSTATUS = 0
    os.chdir(wkgFolder) # Makes sure we are in the correct directory (if called from Task Scheduler)
    excel_report = ''   # Sets variable to '' if the Export_To_Excel() is not run to create it.

    #---------------------------------------------------------------------------
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #---------------------------------------------------------------------------
    #                        Start calling functions

    #---------------------------------------------------------------------------
    # Get the current date and time to append to the end of various files
    # and the start time of the script to be used in the queries.
    if (errorSTATUS == 0 and run_Get_DateAndTime):
        try:
            dt_to_append, start_time = Get_DateAndTime()

        except Exception as e:
            errorSTATUS = Error_Handler('Get_DateAndTime', e)

    #---------------------------------------------------------------------------
    #            Turn the 'print' statement into a logging object
    if (run_Write_Print_To_Log):
        try:
            # os.path.split below is used to return just the folder where the log_file lives
            print 'Setting "print" command to write to a log file found at: {}'.format(os.path.split(log_file)[0])

            # Get the original sys.stdout so it can be returned to normal at the
            #    end of the script.
            orig_stdout = sys.stdout

            # Create the log file with the datetime appended to the file name
            log_file_date = '{}_{}.log'.format(log_file,dt_to_append)
            write_to_log = open(log_file_date, 'w')

            # Make the 'print' statement write to the log file
            sys.stdout = write_to_log

            # Header for log file
            start_time_str = [start_time.strftime('%m/%d/%Y  %I:%M:%S %p')][0]
            print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
            print '                  {}'.format(start_time_str)
            print '             START DPW_Science_and_Monitoring.py'
            print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n'

        except Exception as e:
            errorSTATUS = Error_Handler('Write_Print_To_Log', e)

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
    # GET DATA from AGOL and store in: wkgFolder\wkgGDB\origFC_dt_to_append
    if (errorSTATUS == 0 and run_Get_Data):
        try:
            origPath, SmpEvntIDs_dl = Get_Data(AGOfields, token, queryURL,
                                                wkgFolder, wkgGDB, origFC,
                                                dt_to_append, dt_last_ret_data)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Data', e)

        # Set flag data_was_downloaded based on the number of records in
        # SmpEvntIDs_dl.  This will be used to determine if other functions are
        # called in the main() function.
        if (len(SmpEvntIDs_dl) == 0):
            data_was_downloaded = False

            # These lists are needed in the Email_Results(), but will not be
            # created by the New_Loc_LocDesc() because that function will not be
            # called due to no data being downloaded.  Created here to not cause a fail
            new_loc_descs = []
            new_locs      = []
        else:
            data_was_downloaded = True

    #---------------------------------------------------------------------------
    # Get the ATTACHMENTS from the online database and store it locally
    if (errorSTATUS == 0 and data_was_downloaded and run_Get_Attachments):
        try:
            attach_fldr = Get_Attachments(token, gaURL, prod_attachments,
                                          SmpEvntIDs_dl, dt_to_append)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Attachments', e)

    #---------------------------------------------------------------------------
    # SET THE LAST TIME the data was retrieved from AGOL to the start_time
    # so that it can be used in the where clauses the next time the script is run
    if (errorSTATUS == 0 and data_was_downloaded and run_Set_Last_Data_Ret):
        try:
            Set_Last_Data_Ret(last_data_retrival_csv, start_time)

        except Exception as e:
            errorSTATUS = Error_Handler('Set_Last_Data_Ret', e)
    #---------------------------------------------------------------------------
    # COPY the original data to a working table
    if (errorSTATUS == 0 and data_was_downloaded and run_Copy_Orig_Data):
        try:
            wkgPath = Copy_Orig_Data(wkgFolder, wkgGDB, wkgFC, origPath,
                                     dt_to_append)

        except Exception as e:
            errorSTATUS = Error_Handler('Copy_Orig_Data', e)

    #---------------------------------------------------------------------------
    # ADD FIELDS to the working table
    if (errorSTATUS == 0 and data_was_downloaded and run_Add_Fields):
        try:
            Add_Fields(wkgPath, add_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Add_Fields', e)

    #---------------------------------------------------------------------------
    # CALCULATE FIELDS in the working table
    if (errorSTATUS == 0 and data_was_downloaded and run_Calculate_Fields):
        try:
            Calculate_Fields(wkgPath, calc_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Calculate_Fields', e)

    #---------------------------------------------------------------------------
    # DELETE FIELDS from the working table
    if (errorSTATUS == 0 and data_was_downloaded and run_Delete_Fields):
        try:
            Delete_Fields(wkgPath, delete_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Delete_Fields', e)

    #---------------------------------------------------------------------------
    # Get NEW LOCATION DESCRIPTIONS and set NEW LOCATIONS
    if (errorSTATUS == 0 and data_was_downloaded and run_New_Loc_LocDesc):
        try:
            new_loc_descs, new_locs = New_Loc_LocDesc(wkgPath, prodPath_SitesData)

        except Exception as e:
            errorSTATUS = Error_Handler('New_Loc_LocDesc', e)
    #---------------------------------------------------------------------------
    # EXPORT FC to TABLE
    if (errorSTATUS == 0 and data_was_downloaded and run_FC_To_Table):
        try:
            exported_table = FC_To_Table(wkgFolder, wkgGDB, dt_to_append, wkgPath)

        except Exception as e:
            errorSTATUS = Error_Handler('FC_To_Table', e)

    #---------------------------------------------------------------------------
    # GET FIELD MAPPINGS
    if (errorSTATUS == 0 and data_was_downloaded and run_Get_Field_Mappings):
        try:
            field_mappings = Get_Field_Mappings(exported_table, prodPath_FldData, map_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Field_Mappings', e)

    #---------------------------------------------------------------------------
    # APPEND the data
    if (errorSTATUS == 0 and data_was_downloaded and run_Append_Data):
        try:
            Append_Data(exported_table, prodPath_FldData, field_mappings)

        except Exception as e:
            errorSTATUS = Error_Handler('Append_Data', e)

    #---------------------------------------------------------------------------
    # EXPORT to EXCEL
    if (errorSTATUS == 0 and data_was_downloaded and run_Export_To_Excel):
        try:
            excel_report = Export_To_Excel(wkgFolder, wkgGDB, prodPath_FldData, prodPath_Excel, dt_to_append, report_TMDL_csv)


        except Exception as e:
            errorSTATUS = Error_Handler('Export_To_Excel', e)

    #---------------------------------------------------------------------------
    # Sites Data to Survey123 CSV
    if (errorSTATUS == 0 and run_Sites_Data_To_Survey123):
        try:
            Sites_Data_To_Survey123_csv(Sites_Export_To_CSV_tbl, prodPath_SitesData, site_info)

        except Exception as e:
            errorSTATUS = Error_Handler('Sites_Data_To_Survey123_csv', e)

    #---------------------------------------------------------------------------
    # Email results
    if (run_Email_Results):
            try:
                Email_Results(errorSTATUS, cfgFile, dpw_email_list, lueg_admin_email,
                              log_file, start_time, dt_last_ret_data, prodGDB,
                              prod_attachments, SmpEvntIDs_dl, new_loc_descs,
                              new_locs, excel_report)

            except Exception as e:
                errorSTATUS = Error_Handler('Email_Results', e)
    #---------------------------------------------------------------------------
    #                  Print out info about the script

    if errorSTATUS == 0:
        print '--------------------------------------------------------------------'
        print 'SUCCESSFULLY ran DPW_Science_and_Monitoring.py'

        ##raw_input('Press ENTER to continue...')

    else:
        print '\n*** ERROR!  There was an error with the script. See above for messages ***'

        ##raw_input('Press ENTER to continue...')

    # Footer for log file
    finish_time_str = [datetime.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')][0]
    print '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print '                    {}'.format(finish_time_str)
    print '              Finished DPW_Science_and_Monitoring.py'
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

    if (run_Write_Print_To_Log):

        # Return sys.stdout back to its original setting
        sys.stdout = orig_stdout
        print 'Done with script.  Please find log file location above for more info.'

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#                                 DEFINE FUNCTIONS
#-------------------------------------------------------------------------------
#                 FUNCTION:    Get Start Date and Time

def Get_DateAndTime():
    """
    Get the current date and time so that it can be used in multiple places in
    the rest of the script where a unique name is desired.
    It is also used to get the start_time variable which is written to the
    LastDataRetrival.csv so a record is kept for when the data was last
    retrieved from AGOL.

    Vars:
        start_time (dt object)
        date (str): in the format YYYY_MM_DD - No '0' padding
        time (str): in the format HH_MM_SS   - No '0' padding
        dt_to_append (str) : date and time merged into one string

    Returns:
        dt_to_append
        start_time
    """

    print '--------------------------------------------------------------------'
    print 'Getting Date and Time (dt)...'

    start_time = datetime.datetime.now()

    date = '%s_%s_%s' % (start_time.year, start_time.month, start_time.day)
    time = '%s_%s_%s' % (start_time.hour, start_time.minute, start_time.second)

    dt_to_append = '%s__%s' % (date, time)

    print '  Date to append: {}'.format(dt_to_append)

    print 'Successfully got Date and Time\n'

    # TODO: uncomment the below command and troubleshoot script
    ##print thiswillcausethefunctiontofail

    return dt_to_append, start_time

#-------------------------------------------------------------------------------
#              FUNCTION:    Get_Last_Data_Retrival Date

def Get_Last_Data_Retrival(last_ret_csv):
    """
    Reads the CSV at the last_ret_csv path to get when the script was last run.
    We want to know this so that we can use it in the query to ask AGOL
    to return features that have been created between the last time the
    script was run and the start_time obtained in the FUNCTION Get_DateAndTime()

    Args:
        last_ret_csv (str):
            the path to the CSV that stores the date and time the script was
            last run

    Vars:
        last_ret_str (str):
            the date and time string read from the CSV's 3rd row, 1st column
        dt_last_ret_data (dt obj):
            a datetime object converted from last_ret_str

    Returns:
        dt_last_ret_data
    """

    print '--------------------------------------------------------------------'
    print 'Getting last data retrival...'

    # Create a reader object that will read a Control CSV
    with open (last_ret_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Grab the data that is in the 3rd row, 1st column in the CSV
        # and store it as a string in last_ret_str
        row_num = 0
        for row in readCSV:
            if row_num == 2:
                last_ret_str = row[0]
            row_num += 1

        # Turn the string obtained from the CSV file into a datetime object
        dt_last_ret_data = datetime.datetime.strptime(last_ret_str, '%m/%d/%Y %I:%M:%S %p')

    print '  Last data retrival happened at: %s' % str(dt_last_ret_data)

    print 'Successfully got the last data retrival\n'

    return dt_last_ret_data

#-------------------------------------------------------------------------------
#                       FUNCTION:    Get AGOL token

def Get_Token(cfgFile, gtURL):
    """
    Gets a token from AGOL that allows access to the AGOL data.

    Args:
        cfgFile (str):
            Path to the .txt file that holds the user name and password of the
            account used to access the data.  This account must be in a group
            that has access to the online database.
        gtURL: URL where ArcGIS generates tokens.

    Vars:
        token (str):
            a string 'password' from ArcGIS that will allow us to to access the
            online database.

    Returns:
        token
    """

    print '--------------------------------------------------------------------'
    print "Getting Token..."

    # Get the user name and password from the cfgFile
    configRMA = ConfigParser.ConfigParser()
    configRMA.read(cfgFile)
    usr = configRMA.get("AGOL","usr")
    pwd = configRMA.get("AGOL","pwd")

    # Create a dictionary of the user name, password, and 2 other keys
    gtValues = {'username' : usr, 'password' : pwd, 'referer' : 'http://www.arcgis.com', 'f' : 'json' }

    # Encode the dictionary so they are in URL format
    gtData = urllib.urlencode(gtValues)

    # Create a request object with the URL adn the URL formatted dictionary
    gtRequest = urllib2.Request(gtURL,gtData)

    # Store the response to the request
    gtResponse = urllib2.urlopen(gtRequest)

    # Store the response as a json object
    gtJson = json.load(gtResponse)

    # Store the token from the json object
    token = gtJson['token']
    ##print token

    print "Successfully retrieved token.\n"

    return token

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION:    Get AGOL Data
# TODO: what happens if there is no data to download.  This is not an error, but should be returned so other functions are not run.
#############################################################################################################
### http://blogs.esri.com/esri/arcgis/2013/10/10/quick-tips-consuming-feature-services-with-geoprocessing/
### https://geonet.esri.com/thread/118781
### WARNING: Script currently only pulls up to the first 10,000 (1,000?) records - more records will require
###     a loop for iteration - see, e.g., "Max Records" section at the first (blogs) URL listed above or for
###     example code see the second (geonet) URL listed above
#############################################################################################################

def Get_Data(AGOfields_, token, queryURL_, wkgFolder, wkgGDB_, origFC, dt_to_append, dt_last_ret_data_):
    """ The Get_Data function takes the token we obtained from 'Get_Token'
    function, establishs a connection to the data, creates a FGDB (if needed),
    creates a unique FC to store the data, and then copies the data from AGOL
    to the unique FC"""

    print '--------------------------------------------------------------------'
    print "Getting data..."

    #---------------------------------------------------------------------------
    #Set the feature service URL (fsURL = the query URL + the query)

    # We want to set the 'where' clause to get records where the [CreationDate]
    # field is BETWEEN the date the data was last retrieved and plus two days.
    # This is because AGOL tracks the CreationDate as UTC, which is +8 hours
    # ahead of PST. So if any records are added after 4:00pm, the AGOL database
    # thinks the record was submitted on our TOMORROW.

    # The format for the where query is "CreationData BETWEEN 'First Date' and
    # 'Second Date'".  The data collected on the first date will be retrieved,
    # while the data collected on the second date will NOT be retrieved.
    # For ex: If data is collected on the 28th, and 29th and the where clause is:
    #   BETWEEN the 28th and 29th. You will get the data collected on the 28th only
    #   BETWEEN the 29th and 30th. You will get the data collected on the 29th only
    #   BETWEEN the 28th and 30th. You will get the data collected on the 28th AND 29th

    # For these reasons, if the record was submitted on Feb 1 at 4:01pm PST.
    # The AGOL database says the record was submitted on Feb 2 at 12:01am UTC.
    # This means that if the query is run on Feb 1 at 4:02pm PST (right after
    # submitting the record), and the query has a 'Second Date' SOONER
    # than Feb 3, the script will not grab the data.  By adding 2 days into the
    # future we account for the possibility that the record may have been recorded
    # as being entered TOMORROW.
    two_days = datetime.timedelta(days=2)
    now = datetime.datetime.now()
    plus_two_days = now + two_days
    ##print '  tomorrow: ' + str(tomorrow)

    # Use the dt_last_ret_data variable and tomorrow variable to set the 'where'
    # clause
    where = "CreationDate BETWEEN '{dt.year}-{dt.month}-{dt.day}' and '{ptd.year}-{ptd.month}-{ptd.day}'".format(dt = dt_last_ret_data_, ptd = plus_two_days)
    print '  Getting data where: {}\n    (Second date projected 2 days into future because of conversion from PST to UTC)'.format(where)

    # Encode the where statement so it is readable by URL protocol (ie %27 = ' in URL
    # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding
    where_encoded = urllib.quote(where)

    # If you suspect the where clause is causing the problems, uncomment the below 'where = "1=1"' clause
    ##where = "1=1"
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where_encoded,AGOfields_,token)
    fsURL = queryURL_ + query

    # Create empty Feature Set object
    fs = arcpy.FeatureSet()

    #---------------------------------------------------------------------------
    #                 Try to load data into Feature Set object
    # This try/except is because the fs.load(fsURL) will fail whenever no data
    # is returned by the query; something that will happen when there are no
    # records within the datetime that data was last retrieved "dt_last_ret_data_"
    # and the current time
    try:
        ##print 'fsURL %s' % fsURL
        fs.load(fsURL)
    except:
        print '  "fs.load(fsURL)" yielded no data at fsURL.'
        print '  Query dates may not have yielded any records.'
        print '  Could simply mean there was no data added for these dates.'
        print '  Or could be another problem with the Get_Data() function.'
        print '  Feature Service: %s' % str(fsURL)

        # Set the values of the expected return variables
        origPath_ = 'No original path, data not downloaded.'

        # This empty list will be used to show the rest of the script that
        # no data was downloaded
        SmpEvntIDs_dl = []

        print 'Finished Get_Data function.  No data retrieved.\n'

        # If no data downloaded, stop the function here
        return origPath_, SmpEvntIDs_dl

    #---------------------------------------------------------------------------
    #             Data was loaded, CONTINUE the downloading process

    #Create working FGDB if it does not already exist. Leave alone if it does...
    FGDB_path = wkgFolder + '\\' + wkgGDB_
    if not os.path.exists(FGDB_path):
        print '  Creating FGDB: %s at: %s' % (wkgGDB_, wkgFolder)

        # Process
        arcpy.CreateFileGDB_management(wkgFolder,wkgGDB_)

    #---------------------------------------------------------------------------
    #Copy the features to the FGDB.
    ##origFC = '%s_%s' % (origFC,dt_to_append)
    origPath_ = wkgFolder + "\\" + wkgGDB_ + '\\' + origFC
    print '  Copying AGOL database features to: %s' % origPath_

    # Process
    arcpy.CopyFeatures_management(fs,origPath_)

    #---------------------------------------------------------------------------
    # Get a list of STRINGS of all the Sample Event ID's that were downloaded this run
    SmpEvntIDs_dl = []

    with arcpy.da.SearchCursor(origPath_, ['SampleEventID']) as cursor:

        for row in cursor:
            SampleEventID = str(row[0])

            SmpEvntIDs_dl.append(SampleEventID)

    ##print SmpEvntIDs_dl

    #---------------------------------------------------------------------------
    print "Successfully retrieved data.\n"

    return origPath_, SmpEvntIDs_dl

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                         FUNCTION:   Get Attachments
# Attachments (images) are obtained by hitting the REST endpoint of the feature
# service (gaURL) and returning a URL that downloads a JSON file (which is a
# replica of the database).  The script then uses that downloaded JSON file to
# get the URL of the actual images.  The JSON file is then used to get the
# StationID and SampleEventID of the related feature so they can be used to name
# the downloaded attachment.

#TODO: find a way to rotate the images clockwise 90-degrees
#TODO: attachments are downloaded based on their SampleEventID, this could lead
#  to downloading a picture again if the sample event id is accidentally reused.
#  Is this a problem though?  There shouldn't be two sample even id's...
def Get_Attachments(token, gaURL, gaFolder, SmpEvntIDs_dl, dt_to_append):
    """
    Gets the attachments (images) that are related to the database features and
    stores them as .jpg in a local file inside the wkgFolder.

    Args:
        token (str):
            The string token obtained in FUNCTION Get_Token().
        gaURL (str):
            The variable set in FUNCTION main() where we can request to create a
            replica FGDB in json format.
        wkgFolder (str):
            The variable set in FUNCTION main() which is a path to our working
            folder.
        dt_to_append (str):
            The date and time string returned by FUNCTION Get_DateAndTime().

    Vars:
        replicaUrl (str):
            URL of the replica FGDB in json format.
        JsonFileName (str):
            Name of the temporary json file.
        gaFolder (str):
            A folder in the wkgFolder that holds the attachments.
        gaRelId (str):
            The parentGlobalId of the attachment.  This = the origId for the
            related feature.
        origId (str):
            The GlobalId of the feature.  This = the parentGlobalId of the
            related attachment.
        origName1 (str):
            The StationID of the related feature to the attachment.
        origName2 (str):
            The SampleEventID of the related feature to the attachment.
        attachName (str):
            The string concatenation of origName1 and origName2 to be used to
            name the attachment.
        dupList (list of str):
            List of letters ('A', 'B', etc.) used to append to the end of an
            image to prevent multiple images with the same StationID and
            SampleEventID overwriting each other.
        attachmentUrl:
            The URL of each specific attachment.  Need a token to actually
            access and download the image at this URL.

    Returns:
        gaFolder (str):
            So that the email can send that information.
    """

    print '--------------------------------------------------------------------'
    print 'Getting Attachments...'

    # Flag to set if Attachments were downloaded.  Set to 'True' if downloaded
    attachment_dl = False

    #---------------------------------------------------------------------------
    #                       Get the attachments url (ga)
    ##print '  gaURL = '+ gaURL
    # Set the values in a dictionary
    gaValues = {
    'f' : 'json',
    'replicaName' : 'Bacteria_TMDL_Replica',
    'layers' : '0',
    'geometryType' : 'esriGeometryPoint',
    'transportType' : 'esriTransportTypeUrl',
    'returnAttachments' : 'true',
    'returnAttachmentDatabyURL' : 'false',
    'token' : token
    }

    # Get the Replica URL
    gaData = urllib.urlencode(gaValues)
    gaRequest = urllib2.Request(gaURL, gaData)
    gaResponse = urllib2.urlopen(gaRequest)
    gaJson = json.load(gaResponse)
    replicaUrl = gaJson['URL']
    ##print '  Replica URL: %s' % str(replicaUrl)

    # Set the token into the URL so it can be accessed
    replicaUrl_token = replicaUrl + '?&token=' + token + '&f=json'
    ##print '  Replica URL Token: %s' % str(replicaUrl_token)

    #---------------------------------------------------------------------------
    #                         Save the JSON file
    # Access the URL and save the file to the current working directory named
    # 'myLayer.json'.  This will be a temporary file and will be deleted

    JsonFileName = 'Temp_JSON_%s.json' % dt_to_append

    # Save the file
    # NOTE: the file is saved to the 'current working directory' + 'JsonFileName'
    urllib.urlretrieve(replicaUrl_token, JsonFileName)

    # Allow the script to access the saved JSON file
    cwd = os.getcwd()  # Get the current working directory
    jsonFilePath = cwd + '\\' + JsonFileName # Path to the downloaded json file
    print '  JSON file saved to: ' + jsonFilePath

    #---------------------------------------------------------------------------
    #                       Save the attachments
    ##gaFolder = wkgFolder + '\\Sci_Monitoring_pics'
    ## Uncomment below if you want to create a new folder each time the attachments are pulled
    ##gaFolder = wkgFolder + '\\Sci_Monitoring_pics__%s' % dt_to_append

    # Make the gaFolder (to hold attachments) if it doesn't exist.
    if not os.path.exists(gaFolder):
        os.makedirs(gaFolder)

    # Open the JSON file
    with open (jsonFilePath) as data_file:
        data = json.load(data_file)

    # Save the attachments
    # Loop through each 'attachment' and get its parentGlobalId so we can name
    #  it based on its corresponding feature
    print '  Attempting to save attachments:'

    for attachment in data['layers'][0]['attachments']:
        ##print '\nAttachment: '
        ##print attachment
        gaRelId = attachment['parentGlobalId']
        ##print 'gaRelId:'
        ##print gaRelId

        # Now loop through all of the 'features' and break once the corresponding
        #  GlobalId's match so we can save based on the 'StationID'
        #  and 'SampleEventID'
        for feature in data['layers'][0]['features']:
            origId = feature['attributes']['globalid']
            ##print '  origId' + origId
            StationID = feature['attributes']['StationID']
            SampleEventID = str(feature['attributes']['SampleEventID'])
            if origId == gaRelId:
                break

        # Test to see if the StationID is one of the features downloaded in
        # FUNCTION Get_Data. Download if so, pass if not
        if SampleEventID in SmpEvntIDs_dl:
            attachName = '%s__%s' % (StationID, SampleEventID)
            ##print '  attachName = ' + attachName
            # 'i' and 'dupList' are used in the event that there are
            #  multiple photos with the same StationID and SampleEventID.  If they
            #  do have the same attributes as an already saved attachment, the letter
            #  suffix at the end of the attachment name will increment to the next
            #  letter.  Ex: if there are two SDR-100__9876, the first will always be
            #  named 'SDR-1007__9876_A.jpg', the second will be 'SDR-1007__9876_B'
            i = 0
            dupList = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
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

            # Only download the attachment if the picture is from A - G
            # 'H' is a catch if there are more than 7 photos with the same Station ID
            # and Sample Event ID, shouldn't be more than 7 so an 'H' pic is passed.
            if (dupList[i] != 'H'):
                # Get the token to download the attachment
                gaValues = {'token' : token }
                gaData = urllib.urlencode(gaValues)

                # Get the attachment and save as attachPath
                print '    Saving %s' % attachName
                attachment_dl = True

                attachmentUrl = attachment['url']
                urllib.urlretrieve(url=attachmentUrl, filename=attachPath,data=gaData)

            else:
                print '  WARNING.  There were more than 7 pictures with the same Station ID and Sample Event ID. Picture not saved.'

        # The SampleEventID was not downloaded in FUNCTION Get_Data so pass
        else:
            ##print '  SampleEventID: %s wasn\'t downloaded this run, not downloading related attachment' % SampleEventID
            pass

    if (attachment_dl == False):
        print '  No attachments saved this run.  OK if no attachments submitted since last run.'

    print '  All attachments can be found at: %s' % gaFolder

    # Delete the JSON file since it is no longer needed.
    print '  Deleting JSON file'
    os.remove(jsonFilePath)

    print 'Successfully got attachments.\n'

    return gaFolder

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                    FUNCTION: Set Last Data Retrival time
def Set_Last_Data_Ret(last_ret_csv, start_time):
    # TODO: write a function synopsis
    """This function is """
    print '--------------------------------------------------------------------'
    print 'Setting LastDataRetrival.csv time so it equals this runs start_time...'

    ##print '  New Last Data Retrival time: {}'.format(start_time)
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
                ##print 'Writing: ' + row
                writeCSV.writerow([row])
            # This writes the new start time to the third row
            else:
                print '  New time: %s' % formated_dt[0]
                writeCSV.writerow(formated_dt)
            row_num += 1

    print 'Successfully set last data retrival time\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION:  Copy_Orig_Data

def Copy_Orig_Data(wkgFolder, wkgGDB, wkgFC, origPath, dt_to_append):
    print '--------------------------------------------------------------------'
    print 'Copying original data...'

    #---------------------------------------------------------------------------
    # Copy the orig FC to a working TABLE to run processing on.
    in_features = origPath
    wkgPath = out_feature_class = r'%s\%s\%s' % (wkgFolder, wkgGDB, wkgFC)
    ##wkgPath = out_feature_class = r'%s\%s\%s_%s' % (wkgFolder, wkgGDB, wkgFC, dt_to_append)

    print '  Copying Data...'
    print '    From: ' + in_features
    print '    To:   ' + out_feature_class

    # Process
    arcpy.CopyFeatures_management(in_features, out_feature_class)

    print 'Successfully copied original data.\n'

    return wkgPath
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION: ADD FIELDS

def Add_Fields(wkg_data, add_fields_csv):
    print '--------------------------------------------------------------------'
    print 'Adding fields to:\n  %s' % wkg_data
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

                f_names.append(f_name)
                f_types.append(f_type)
                f_lengths.append(f_length)
            row_num += 1

    num_new_fs = len(f_names)
    print '    There are %s new fields to add:' % str(num_new_fs)

    f_counter = 0
    while f_counter < num_new_fs:
        print ('      Creating field: %s, with a type of: %s, and a length of: %s'
        % (f_names[f_counter], f_types[f_counter], f_lengths[f_counter]))

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

    print 'Successfully added fields.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                     FUNCTION: CALCULATE FIELDS

def Calculate_Fields(wkg_data, calc_fields_csv):
    print '--------------------------------------------------------------------'
    print 'Calculating fields in:\n  %s' % wkg_data

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
    print '    There are %s calculations to perform:\n' % str(num_calcs)

    #---------------------------------------------------------------------------
    #                    Select features and calculate them
    f_counter = 0
    while f_counter < num_calcs:
        #-----------------------------------------------------------------------
        #               Select features using the where clause

        in_layer_or_view = 'wkg_data_view'
        selection_type   = 'NEW_SELECTION'
        my_where_clause  = where_clauses[f_counter]

        print '      Selecting features where: "%s"' % my_where_clause

        # Process
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, selection_type, my_where_clause)

        #-----------------------------------------------------------------------
        #     If features selected, perform one of the following calculations
        # The calculation that needs to be performed depends on the field or the calc
        #    See the options below:

        countOfSelected = arcpy.GetCount_management(in_layer_or_view)
        count = int(countOfSelected.getOutput(0))
        print '        There was/were %s feature(s) selected.' % str(count)

        if count != 0:
            in_table   = in_layer_or_view
            field      = calc_fields[f_counter]
            calc       = calcs[f_counter]

            #-------------------------------------------------------------------
            # Test to see if the field is one of the two special TIME FIELDS
            # that need a special calculation that is not available in the CSV
            # The calculation in option 1 is needed because the CreationDate
            # is downloaded in UTC, but it needs to be converted to PCT.
            # OPTION 1:
            # TODO: Rearrange this Option so that it has the try/except in the right place
            if (field == 'DateSurveySubmit' or field == 'TimeSurveySubmit'):

                # Create an Update Cursor to loop through values
                with arcpy.da.UpdateCursor(wkg_data, ['CreationDateString', 'DateSurveySubmit', 'TimeSurveySubmit']) as cursor:

                    for row in cursor:
                        try:
                            # Turn the string obtained from the field into a datetime object
                            UTC_dt_obj = datetime.datetime.strptime(row[0], '%m/%d/%Y %I:%M:%S %p')

                            # Subtract 8 hours from the UTC (Universal Time Coordinated)
                            # to get PCT
                            PCT_offset = -8
                            t_delta = datetime.timedelta(hours = PCT_offset)
                            PCT_dt_obj = UTC_dt_obj + t_delta

                            # Set the format for the date and time
                            survey_date = [PCT_dt_obj.strftime('%m/%d/%Y')]
                            survey_time = [PCT_dt_obj.strftime('%H:%M')]

                            # Update the rows with the correct formatting
                            # row[1] is 'DateSurveySubmit' and row[2] is 'TimeSurveySubmit'
                            # as defined when creating the UpdateCursor above
                            ##print 'Survey Date: ' + survey_date[0]
                            row[1] = survey_date[0]
                            ##print 'Survey Time: ' + survey_time[0]
                            row[2] = survey_time[0]

                            # Update the cursor with the updated list
                            cursor.updateRow(row)

                        except Exception as e:
                            print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                            print str(e)

                print '        From selected features, calculated field: %s, so that it equals %s hours than CreationDateString\n' % (field, str(PCT_offset))
            #-------------------------------------------------------------------
            # Strip the auto-appended Time from the DateSurveyStart field
            # OPTION 2:
            # TODO: Add try/except to this option
            elif (field == 'DateSurveyStart'):
                expression = "Remove_Time_From_Date(!DateSurveyStart!)"
                expression_type="PYTHON_9.3"
                code_block = """def Remove_Time_From_Date(date):\n    if date is None:\n        return None\n    else:\n        return date.split(" ").pop(0)"""

                # Process
                arcpy.CalculateField_management(in_table, field, expression,
                                                    expression_type, code_block)
                print '        From selected features, calculated field: %s, so that it removed the auto-appended time from the date\n' % field

            #-------------------------------------------------------------------
            # Test if the user wants to calculate the field being equal to
            # ANOTHER FIELD by seeing if the calculation starts or ends with an '!'
            # OPTION 3:
            elif (calc.startswith('!') or calc.endswith('!')):
                f_expression = calc

                try:
                    # Process
                    arcpy.CalculateField_management(in_table, field, f_expression, expression_type="PYTHON_9.3")

                    print ('        From selected features, calculated field: %s, so that it equals FIELD: %s\n'
                            % (field, f_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

            #-------------------------------------------------------------------
            # If calc does not start or end with a '!', it is probably because the
            # user wanted to calculate the field being equal to a STRING
            # OPTION 4:
            else:
                s_expression = "'%s'" % calc

                try:
                # Process
                    arcpy.CalculateField_management(in_table, field, s_expression, expression_type="PYTHON_9.3")

                    print ('        From selected features, calculated field: %s, so that it equals STRING: %s\n'
                            % (field, s_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

        else:
            print ('        WARNING.  No records were selected.  Did not perform calculation.\n')

        #-----------------------------------------------------------------------

        # Clear selection before looping back through to the next selection
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, 'CLEAR_SELECTION')

        f_counter += 1

    print 'Successfully calculated fields.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION DELETE FIELDS

def Delete_Fields(wkg_data, delete_fields_csv):
    print '--------------------------------------------------------------------'
    print 'Deleting fields in:\n    %s' % wkg_data

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

        print 'Successfully deleted fields.\n'

    else:
        print 'WARNING.  NO fields were deleted.'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                    FUNCTION: New Loc and Loc Desc
def New_Loc_LocDesc(wkg_data, Sites_Data):

    print '--------------------------------------------------------------------'
    print 'Getting new Location Descriptions and Locations from:\n  {}\n'.format(wkg_data)

    #---------------------------------------------------------------------------
    #                      Get new Location Descriptions.

    # Create list and add the first item
    New_LocDescs = ['  The following are New Location Description suggested changes (Please edit associated feature class appropriately):']

    # Create a Search cursor and add data to lists
    cursor_fields = ['SampleEventID', 'Creator', 'StationID', 'site_loc_desc_new']
    where = "site_loc_desc_cor = 'No'"
    with arcpy.da.SearchCursor(wkg_data, cursor_fields, where) as cursor:

        for row in cursor:
            New_LocDesc = ('    For SampleEventID: "{}", Monitor: "{}" said the Location Description for StationID: "{}" was innacurate.  Suggested change: "{}"\n'.format(row[0], row[1], row[2], row[3]))
            New_LocDescs.append(New_LocDesc)

    del cursor

    # If there is only the original New_LocDescs string, then there were no new
    # suggested changes to make, replace the original string with below
    if (len(New_LocDescs) == 1):
        New_LocDescs = ['  There were no New Location Description suggested changes.\n']

    for desc in New_LocDescs:
        print desc

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                           Set new Locations

    # Create needed lists
    New_Locs = ['  The following are the sites that were relocated in the field (The changes will be automatically made to the Sites_Data):']
    StationIDs, ShapeXs, ShapeYs, SampEvntIDs, Creators = ([] for i in range(5))

    # Create Search cursor and add data to lists
    cursor_fields = ['StationID', 'Shape@X', 'Shape@Y', 'SampleEventID', 'Creator']
    where = "site_loc_map_cor = 'No'"
    with arcpy.da.SearchCursor(wkg_data, cursor_fields, where) as cursor:

        for row in cursor:
            StationID    = row[0]
            ShapeX       = row[1]
            ShapeY       = row[2]
            SampleEvntID = row[3]
            Creator      = row[4]

            StationIDs.append(StationID)
            ShapeXs.append(ShapeX)
            ShapeYs.append(ShapeY)
            SampEvntIDs.append(SampleEvntID)
            Creators.append(Creator)

            ##print 'StationID: "{}" has an NEW X of: "{}" and a NEW Y of: "{}"'.format(StationID, ShapeX, ShapeY)

            New_Loc = ('    For SampleEventID: "{}", Monitor: "{}" said the Location Description for StationID: "{}" was innacurate.  Site has been moved.\n'.format(SampleEvntID, Creator, StationID))
            New_Locs.append(New_Loc)

    del cursor

   # If there is only the original New_Locs string, then there were no new
   #  locations to move; no need to update the Sites_Data
    if(len(New_Locs) == 1):
        New_Locs = ['  There were no relocated sites.\n']

    #---------------------------------------------------------------------------
    # Create an Update cursor to update the Shape column in the Sites_Data
    else:
        list_counter = 0
        cursor_fields = ['StationID', 'Shape@X', 'Shape@Y']
        with arcpy.da.UpdateCursor(Sites_Data, cursor_fields) as cursor:
            for row in cursor:

                # Only loop as many times as there are StationIDs to update
                if (list_counter < len(StationIDs)):

                    # If StationID in Sites_Data equals the StationID in the
                    #  StationIDs list, update the geom for that StationID in Sites_Data
                    if row[0] == StationIDs[list_counter]:
                        ##print '  Updating StationID: {} with new coordinates.'.format(StationIDs[list_counter])

                        # Give Shape@X and Shape@Y their new values
                        row[1] = ShapeXs[list_counter]
                        row[2] = ShapeYs[list_counter]

                        cursor.updateRow(row)

                        list_counter += 1

        del cursor

        #-----------------------------------------------------------------------
        # Calculate X and Y fields in Sites_Data now that the geometry has been updated

        # Calculate the Long_X field
        field = 'Long_X'
        expression = "!Shape.Centroid.X!"
        expression_type="PYTHON_9.3"
        arcpy.CalculateField_management(Sites_Data, field, expression, expression_type)

        # Calculate the Lat_Y field now that the geometry has been updated
        field = 'Lat_Y'
        expression = "!Shape.Centroid.Y!"
        expression_type="PYTHON_9.3"
        arcpy.CalculateField_management(Sites_Data, field, expression, expression_type)

    for Loc in New_Locs:
        print Loc


    print '\nSuccessfully got new Location Descriptions and set New Locations.\n'

    return New_LocDescs, New_Locs

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION: FC to Table
def FC_To_Table(wkgFolder, wkgGDB, dt_to_append, wkgPath):
    """The FC that has been altered and calculated above needs to be turned into
    a table first in order to append the data to the production table.
    """
    print '--------------------------------------------------------------------'
    print 'Exporting working FC to table:'
    in_features = wkgPath
    out_table = '{}\\{}\\C_DPW_Data_to_apnd'.format(wkgFolder, wkgGDB)
    ##out_table = '{}\\{}\\DPW_Data_to_apnd_{}'.format(wkgFolder, wkgGDB, dt_to_append)

    ##print '  Exporting...'
    print '  From: {}'.format(in_features)
    print '  To:   {}'.format(out_table)

    # Process
    arcpy.CopyRows_management(in_features, out_table)

    print 'Successfully exported FC to table\n'

    return out_table

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION: GET FIELD MAPPINGS
def Get_Field_Mappings(orig_table, prod_table, map_fields_csv):
    print '--------------------------------------------------------------------'
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
    print '  There are %s non-default fields to map.' % str(num_fm)

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

    ##print '  Field Mappings:\n  %s' % fms
    print 'Successfully Got Field Mappings\n'
    return fms

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION:  APPEND DATA

def Append_Data(orig_table, target_table, field_mapping):


    print '--------------------------------------------------------------------'
    print 'Appending Data...'
    print '  From: {}'.format(orig_table)
    print '  To:   {}'.format(target_table)

    # Append working data to production data
    schema_type = 'NO_TEST'

    # Process
    arcpy.Append_management(orig_table, target_table, schema_type, field_mapping)

    print 'Successfully appended data.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION:   Export to Excel
def Export_To_Excel(wkg_folder, wkg_FGDB, table_to_export, export_folder, dt_to_append, report_TMDL_csv):
    """Exports the production Field_Data table to a working table, deletes the
    unneeded fields in the working table and then exports that table to excel.
    Essentially creating a 'Report' in Excel based on the where_clause.
    """
    print '--------------------------------------------------------------------'
    print 'Exporting to Excel...'


    #---------------------------------------------------------------------------
    #            Export table_to_export to wkg_FGDB to delete fields
    out_path     = wkg_folder + '\\' + wkg_FGDB
    out_name     = 'Report__Bacteria_TMDL_Outfall'
    where_clause = "Project = 'Bacteria TMDL Outfalls'"

    wkg_table = out_path + '\\' + out_name
    print '  Exporting table to table:'
    print '    From:  {}'.format(table_to_export)
    print '    To:    {}'.format(wkg_table)
    print '    Where: {}'.format(where_clause)

    arcpy.TableToTable_conversion(table_to_export, out_path, out_name, where_clause)


    #---------------------------------------------------------------------------
    #              Delete fields that are not needed/wanted in report

    with open (report_TMDL_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        fields_to_delete = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                f_to_delete = row[0]

                fields_to_delete.append(f_to_delete)
            row_num += 1

    num_deletes = len(fields_to_delete)

    print '  There are {} fields to delete:'.format(num_deletes)

    # If there is at least one field to delete, delete it
    if num_deletes > 0:
        f_counter = 0
        while f_counter < num_deletes:
            drop_field = fields_to_delete[f_counter]
            ##print '    Deleting field: %s...' % drop_field

            arcpy.DeleteField_management(wkg_table, drop_field)

            f_counter += 1
    print '  Fields deleted.'

    #---------------------------------------------------------------------------
    #                            Export table to Excel

    # Make the export file if it doesn't exist
    if not os.path.exists(export_folder):
        os.mkdir(export_folder)

    export_file = export_folder + '\\Bacteria_TMDL_Report_{}.xls'.format(dt_to_append)

    print '  Exporting table to Excel...'
    print '    From: ' + wkg_table
    print '    To :  ' + export_file

    # Process
    arcpy.TableToExcel_conversion(wkg_table, export_file, 'ALIAS')

    print 'Successfully exported database to Excel.\n'

    return export_file

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#            FUNCTION: Export Sites_Data to Survey123's Site_Info csv
def Sites_Data_To_Survey123_csv(Sites_Export_To_CSV_tbl, Sites_Data, Site_Info):
    print '--------------------------------------------------------------------'
    print 'Exporting Sites Data to the Survey123 CSV...'

    # Sites_Export_To_CSV is a table that has the same schema the CSV needs in
    # order to work with Survey123.

    #---------------------------------------------------------------------------
    #               Delete rows in Sites_Export_To_CSV FGDB table

    print '  Deleting Rows in: {}'.format(Sites_Export_To_CSV_tbl)

    arcpy.DeleteRows_management(Sites_Export_To_CSV_tbl)

    #---------------------------------------------------------------------------
    #         Export prod Sites_Data to a working table in the working_FGDB

    working_FGDB = os.path.split(Sites_Export_To_CSV_tbl)[0]  # Get the working FGDB path
    Sites_Data_tbl = 'D_Sites_Data_exported_tbl'
    Sites_Data_tbl_path = working_FGDB + '\\' + Sites_Data_tbl

    print '  Exporting Sites_Data to a working table:'
    print '    From: {}'.format(Sites_Data)
    print '    To:   {}'.format(Sites_Data_tbl_path)

    arcpy.TableToTable_conversion(Sites_Data, working_FGDB, Sites_Data_tbl)

    #---------------------------------------------------------------------------
    #            Append Sites_Data_tbl to the Sites_Export_To_CSV table

    inputs = working_FGDB + '\\' + Sites_Data_tbl

    print '  Appending {}'.format(Sites_Data_tbl)
    print '    From: {}'.format(inputs)
    print '    To:   {}'.format(Sites_Export_To_CSV_tbl)

    arcpy.Append_management(inputs, Sites_Export_To_CSV_tbl, 'TEST')

    #---------------------------------------------------------------------------
    #                         Field Calculations
    # Some field calculations have to be performed on the Sites_Export_To_CSV_tbl
    # in order that the comma and quote sensitive Survey123 app can read it correctly

    # Do a search for ',' and replace with a ' ' to make sure no commas get into
    #  the CSV file in the Loc_Desc field
    field = 'Loc_Desc'
    expression = '!Loc_Desc!.replace(",", " ")'
    expression_type = "PYTHON_9.3"

    print '  Calculating field: {}, so that it equals: {}'.format(field, expression)

    arcpy.CalculateField_management(Sites_Export_To_CSV_tbl, field, expression, expression_type)

    # Do a search for a quote (") and replace with an 'in.' to make sure no quotes get into
    #  the CSV file in the Loc_Desc field
    ##print '  Replacing \" with an \'in.\' in Loc_Desc field'
    field = 'Loc_Desc'
    expression = "!Loc_Desc!.replace('\"', 'in.')"
    expression_type = "PYTHON_9.3"

    print '  Calculating field: {}, so that it equals: {}'.format(field, expression)

    arcpy.CalculateField_management(Sites_Export_To_CSV_tbl, field, expression, expression_type)

    #---------------------------------------------------------------------------
    #                      Export to CSV and clean up files.

    out_path = os.path.split(Site_Info)[0]  # Get the Path
    out_name = os.path.split(Site_Info)[1]  # Get the file name

    print '  Exporting to CSV'
    print '    From: {}'.format(Sites_Export_To_CSV_tbl)
    print '    To:   {}'.format(Site_Info)

    arcpy.TableToTable_conversion(Sites_Export_To_CSV_tbl, out_path, out_name)

    # Delete the extra files that are not needed that are created by the above export
    print '  Deleting extra files auto-generated by export process.'
    schema_file = out_path + '\\schema.ini'
    xml_file = out_path + '\\Site_Info.txt.xml'
    if os.path.exists(schema_file):
        os.remove(schema_file)
    if os.path.exists(xml_file):
        os.remove(xml_file)

    print 'Successfully exported Sites_Data to Survey123 CSV\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION:  Email Results
def Email_Results(errorSTATUS, cfgFile, dpw_email_list, lueg_admin_email, log_file,
                  start_time_obj, dt_last_ret_data, prod_FGDB, attach_folder,
                  dl_features_ls, new_loc_descs, new_locs, excel_report):
    print '--------------------------------------------------------------------'
    print 'Emailing Results...'

    #---------------------------------------------------------------------------
    #         Do some processing to be used in the body of the email

    # Turn the start_time into a formatted string
    start_time = [start_time_obj.strftime('%m/%d/%Y %I:%M:%S %p')]

    # Get the current time and turn into a formatted string
    finish_time_obj = datetime.datetime.now()
    finish_time = [finish_time_obj.strftime('%m/%d/%Y %I:%M:%S %p')]

    # Turn the date data last retrieved into a formatted string
    data_last_retrieved = [dt_last_ret_data.strftime('%m/%d/%Y')]

    # Get the number of downloaded features
    num_dl_features = len(dl_features_ls)

    # Get a formatted string of the new_loc_descs and new_locs
    str_new_loc_descs = ' <br> &nbsp;&nbsp;'.join(new_loc_descs) # join each item in the list with a line break and a tab
    str_new_locs      = ' <br> &nbsp;&nbsp;'.join(new_locs)

    #---------------------------------------------------------------------------
    #                Write the "Success--Data Downloaded" email

    # If there are no errors and at least one feature was downloaded
    if (errorSTATUS == 0 and num_dl_features > 0):
        print '  Writing the "Success--Data Downloaded" email...'

        # Send this email to the dpw_email_list
        email_list = dpw_email_list

        # Attach the excel_report
        attach_excel_report = True

        # Format the Subject for the email
        subj = 'SUCCESSFULLY Completed DPW_Science_and_Monitoring.py Script.  Data Downloaded.'

        # Format the Body in html
        body  = ("""\
        <html>
          <head></head>
          <body>
            <h3>Info:</h3>
            <p>
               There was/were <b>{num}</b> feature(s) downloaded this run.<br>
               Please find excel report attached to this email.<br>
               -----------------------------------------------------------------
            <br><br>
            </p>

            <h3>New Location Descriptions and Moved Sites:</h3>
            <p>
               <h4>Location Description (Suggested Changes):</h4>
               {nld}
              <br><br>
               <h4>Moved Sites:</h4>
               {nl}<br>
               -----------------------------------------------------------------
              <br>
            </p>

            <h3>Times:</h3>
            <p>
               The script started at:             <i>{st}</i><br>
               The script finished at:            <i>{ft}</i><br>
               The data retrieved was from:       <i>{dlr}</i>
                 to the start time of the script.<br>
               -----------------------------------------------------------------
            </p>
            <br>

            <h3>File Locations:</h3>
            <p>
               The <b>FGDB</b> is located at:            <i>{fgdb}</i><br>
               The <b>Images</b> are located at:         <i>{af}</i><br>
               The <b>Log File</b> is located at:        <i>{lf}</i><br>
               The <b>Excel Report</b> is located at:    <i>{er}</i><br>
            </p>
          </body>
        </html>
        """.format(nld = str_new_loc_descs, nl = str_new_locs, st = start_time[0],
                   ft = finish_time[0], dlr = data_last_retrieved[0],
                   num = num_dl_features, fgdb = prod_FGDB, af = attach_folder,
                   lf = log_file, er = excel_report))

    #---------------------------------------------------------------------------
    #             Write the "Success--No Data Downloaded' email

    # If there were no errors but no data was downloaded
    elif(errorSTATUS == 0 and num_dl_features == 0):
        print '  Writing the "Success--No Data Downloaded" email'

        # Send this email to the lueg_admin_emails
        email_list = lueg_admin_email

        # Do not attache an excel report if no data was downloaded
        attach_excel_report = False

        # Format the Subject for the 'No Data Downloaded' email
        subj = 'SUCCESSFULLY Completed DPW_Science_and_Monitoring.py Script.  NO Data Downloaded.'

        # Format the Body in html
        body  = ("""\
        <html>
          <head></head>
          <body>
            <h3>Info:</h3>
            <p>
               There were <b>{num}</b> features downloaded this run.<br>
               This is not an error IF there was no data collected from the
               date the data was last retrieved... <i>{dlr}</i> ... and now.<br>
            <br><br>
            </p>

            <h3>Times:</h3>
            <p>
               The script started at:             <i>{st}</i><br>
               The script finished at:            <i>{ft}</i>
            </p>
            <br><br>

            <h3>File Locations:</h3>
            <p>
               The Log file is located at:        <i>{lf}</i><br>
            </p>
          </body>
        </html>
        """.format(st = start_time[0], ft = finish_time[0], num = num_dl_features,
                   dlr = data_last_retrieved[0], lf = log_file))

    #---------------------------------------------------------------------------
    #                        Write the "Errors" email

    # If there were errors with the script
    elif(errorSTATUS <> 0):
        print '  Writing "Error" email...'

        # Get the current working directory
        cwd = os.getcwd()

        # Send this email to the lueg_admin_emails
        email_list = lueg_admin_email

        # Do not attache an excel report if no data was downloaded
        attach_excel_report = False

        # Format the Subject for the 'Errors' email
        subj = 'ERROR with DPW_Science_and_Monitoring.py Script'

        # Format the Body in html
        body = ("""\
        <html>
          <head></head>
          <body>
            <h2>ERROR</h2>
            <h3>Info:</h3>
            <p>There were ERRORS with the DPW_Science_and_Monitoring.py script.
            <br><br>
            </p>

            <h3>Times:</h3>
            <p>The script started at:             <i>{st}</i><br>
               The error happened at:             <i>{ft}</i>
            <br><br>
            </p>

            <h3>File Locations:</h3>
               The Log file is located at:        <i>{lf}</i><br>
               The script is located at:          <i>{cwd}</i><br>
            </p>
          <body>
        </html>

        """.format(st = start_time[0], ft = finish_time[0], lf = log_file, cwd = cwd))

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                              Send the Email

    # Set the subj, From, To, and body
    msg = MIMEMultipart()
    msg['Subject']   = subj
    msg['From']      = "Python Script"
    msg['To']        = ', '.join(dpw_email_list)  # Join each item in list with a ', '
    msg.attach(MIMEText(body, 'html'))

    # Set the attachment if needed
    if (attach_excel_report == True):
        attachment = MIMEApplication(open(excel_report, 'rb').read())

        # Get name for attachment, which should equal the name of the excel_report
        file_name = os.path.split(excel_report)[1]

        # Set attachment into msg
        attachment['Content-Disposition'] = 'attachment; filename = "{}"'.format(file_name)
        msg.attach(attachment)


    # Get username and password from cfgFile
    config = ConfigParser.ConfigParser()
    config.read(cfgFile)
    email_usr = config.get('email', 'usr')
    email_pwd = config.get('email', 'pwd')

    # Send the email
    SMTP_obj = smtplib.SMTP('smtp.gmail.com',587)
    SMTP_obj.starttls()
    SMTP_obj.login(email_usr, email_pwd)
    SMTP_obj.sendmail(email_usr, email_list, msg.as_string())
    SMTP_obj.quit()

    print 'Successfully emailed results.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                         FUNCTION:  Error Handler
def Error_Handler(func_w_err, e):

    e_str = str(e)
    print '\n*** ERROR in function: "%s" ***\n' % func_w_err
    print '    error: = %s' % e_str

    #---------------------------------------------------------------------------
    #                                Help comments
    # TODO: have the help comments be a part of a list so that I can print out a list of help comments.
    help_comment = ''

    # Help comments for any function
    if (e_str == 'not all arguments converted during string formatting'):
        help_comment = 'There may be a problem with a print statement.  Is it formatted correctly?'


    # Help comments for 'Get_Token' function
    if (func_w_err == 'Get_Token'):
        if (e_str.startswith('No section:')):
            help_comment = '    The section in brakets in "cfgFile" variable may not be in the file, or the file cannot be found.  Check both!'

    # Help comments for 'Get_Data' function
    if (func_w_err == 'Get_Data'):
        if e_str == 'RecordSetObject: Cannot load a table into a FeatureSet':
            help_comment =  '    Error may be the result of a bad query format.  Try the query "where = \'1=1\'" to see if that query works.\n      Or the dates you are using to select does not have any data to download.'
        if e_str == 'RecordSetObject: Cannot open table for Load':
            help_comment = '     Error may be the result of the Feature Service URL not being correctly set.  OR the \'Enable Sync\' option may not be enabled on the AGOL feature layer.  OR the Feature layer may not be shared.'

    # Help comments for 'Get_Attachments' function
    # TODO: figure out why these help comments aren't being assigned
    if (func_w_err == 'Get_Attachments'):
        if e_str == "*":
            help_comment = '    This error may be the result that the field you are using to parse the data from is not correct.  Double check your fields.'
        elif e_str == "URL":
            help_comment = '    This error may be the result of the feature layer may not be shared.\n    Or user in the "cfgFile" may not have permission to access the URL.  Try logging onto AGOL with that user account to see if that user has access to the database.\n    Or the problem may be that the feature layer setting doesn\'t have Sync enabled.\n    Or the URL is incorrect somehow.'
        elif e_str == "*Permission denied:*":
            help_comment = '    This error may be due to the present working directory not being in the right folder.'


    # Help comments for 'Append_Data' function
    if(func_w_err == 'Append_Data'):
        pass

    #---------------------------------------------------------------------------
    if (help_comment == ''):
        print '    No help comment.'
    else:
        print '    Help Comment: ' + help_comment

    # Change errorSTATUS to 1 so that the script doesn't try to perform any
    # other functions
    errorSTATUS = 1
    return errorSTATUS

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                                  RUN MAIN
if __name__ == '__main__':
    main()
