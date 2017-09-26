#-------------------------------------------------------------------------------
# Name:        DPW_Science_and_Monitoring.py
# Purpose:
"""
This script is intended to be used in conjunction with DPW's Science and
Monitoring Survey123 Field Survey.  The Survey is used to gather data in the
field by monitors.  The Survey sends the data to an AGOL database where it waits
to be grabbed by this script.

EXECUTIVE SUMMARY:
To download data stored on AGOL servers that were added via Survey123 by DPW's
  Science and Monitoring Project.
To process the downloaded data and append the downloaded data to a FGDB that
  contains the most up-to-date data.
To download JPEG's taken in the field and name them based on the site ID and the
  Sample Event ID

PROCESS:
1. Import Modules
2. Define main() function.
  2.1 Set variables used in the script.
  2.2 Call functions
    2.2.1  Get the current date and time
    2.2.2  Create a log file and turn the print statement to a logging object.
    2.2.3  Get the last time the data was retrieved from AGOL
    2.2.4  Get the token to gain access to the AGOL database.
    2.2.5  Get the most recent DPW_WP_FIELD_DATA from AGOL and store in a
             working FGDB.  This data is from Survey123.
    2.2.6  Get the attachments from DPW_WP_FIELD_DATA on AGOL and store it in a
             local folder.
    Pause DPW_WP_FIELD_DATA processing
    Begin DPW_WP_SITES processing
    2.2.7  Get all of the the DPW_WP_SITES from AGOL and store in the same
             working folder as above.  This data is from Collector.
    2.2.8  QA/QC the DPW_WP_SITES data.
    2.2.9  If no QA/QC errors in DPW_WP_SITES, calculate X and Y fields.
    2.2.10 If no QA/QC errors in DPW_WP_SITES, Delete the features in
             DPW_WP_SITES in the prod FGDB and append the features just downloaded
             and QA/QC'd.
    Finish DPW_WP_SITES processing.
    Resume DPW_WP_FIELD_DATA processing.
    2.2.11 Copy the original DPW_WP_FIELD_DATA to a working FC.
    2.2.12 Add fields to the working FC.
    2.2.13 Calculate fields in the working FC.
    2.2.14 Delete fields from the working FC.
    2.2.15 Export working FC to TABLE.
    2.2.16 Get non-default field mappings so we can append fields from the TABLE
             to the production database if the field names are not the same
             between the working database and the production database.
    2.2.17 Set the last time the data was retrieved from AGOL to the start_time
             of this script so the next time it is run it will only grab
             new data.
    2.2.14 Append the working DPW_WP_FIELD_DATA to the production database.
    2.2.15 Search for and handle any Duplicates.
    2.2.16 Email results.
  2.3 Print out information about the script

NOTE:  This is a long and complex script.  Navigation between the main function
(which drives the script) and the secondary functions (which drive specific
tasks) will be easier with an IDE such as PyScripter as opposed to the generic
IDLE.
"""
# Author:      mgrue
#
# Created:     22/11/2016
# Copyright:   (c) mgrue 2016
# Licence:     <your licence>
# Modified:    02/24/2017
#-------------------------------------------------------------------------------
# TODO: if the A_FIELD_DATA_orig is locked the script fails, which is OK, but it doens't send a failed email.  Need to make it so it does send an email.
# TODO: Add documentation above for the new SITES related steps and Functions.
# TODO: NEED to change the SITES overwrite to delete features and append features instead of overwriting the FC in order to keep the domains in the prod FGDB linked to the fields.
# Import modules

import arcpy
import ConfigParser
import datetime
import json
import math
import mimetypes
import os
import time
import smtplib
import string
import sys
import time
import urllib
import urllib2
import csv

# Import emailing modules
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email import encoders
from email.message import Message
from email.mime.text import MIMEText

# Set overwrite output
arcpy.env.overwriteOutput = True

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                                    DEFINE MAIN
def main():
    #---------------------------------------------------------------------------
    #                               Set Variables
    #---------------------------------------------------------------------------

    # Variables to control which Functions are run
    run_Get_DateAndTime         = True
    run_Write_Print_To_Log      = True
    run_Get_Last_Data_Ret       = True
    run_Get_Token               = True
    run_Get_Data                = True
    run_Get_Attachments         = True # Requires 'run_Get_Data = True'
    run_Get_SITES_data          = True
    run_Set_Last_Data_Ret       = True # Should be 'False' if testing
    run_Copy_Orig_Data          = True  # Requires 'run_Get_Data = True'
    run_Add_Fields              = True  # Requires 'run_Copy_Orig_Data = True'
    run_Calculate_Fields        = True  # Requires 'run_Copy_Orig_Data = True'
    run_Delete_Fields           = False # Requires 'run_Copy_Orig_Data = True'
    run_New_Loc_LocDesc         = True
    run_FC_To_Table             = True
    run_Get_Field_Mappings      = True  # Requires 'run_Copy_Orig_Data = True'
    run_Append_Data             = True  # Requires 'run_Copy_Orig_Data = True'
    run_Duplicate_Handler       = True  # Requires 'run_Copy_Orig_Data = True'
    run_Email_Results           = True

    # Email lists
    ##dpw_email_list   = ['michael.grue@sdcounty.ca.gov', 'mikedavidg2@gmail.com', 'Joanna.Wisniewska@sdcounty.ca.gov', 'Ryan.Jensen@sdcounty.ca.gov', 'Steven.DiDonna@sdcounty.ca.gov', 'Kenneth.Liddell@sdcounty.ca.gov']
    dpw_email_list   = ['michael.grue@sdcounty.ca.gov']  # The above commented out is for PROD
    lueg_admin_email = ['michael.grue@sdcounty.ca.gov']#['Michael.Grue@sdcounty.ca.gov', 'Gary.Ross@sdcounty.ca.gov', 'Randy.Yakos@sdcounty.ca.gov']

    # Which stage is this script pointing to? 'DEV', 'BETA', 'PROD'
    stage = 'DEV'  # This variable is used to control the path to the varioius stages

    # Control files
    control_files          = r'P:\DPW_ScienceAndMonitoring\Scripts\{v}\{v}_branch\Control_Files'.format(v = stage)
    last_data_retrival_csv = control_files + '\\LastDataRetrival.csv'
    add_fields_csv         = control_files + '\\FieldsToAdd.csv'
    calc_fields_csv        = control_files + '\\FieldsToCalculate.csv'
    delete_fields_csv      = control_files + '\\FieldsToDelete.csv'
    map_fields_csv         = control_files + '\\MapFields.csv'
    report_TMDL_csv        = control_files + '\\Report_TMDL.csv'
    cfgFile                = control_files + '\\accounts.txt'



    # serviceURL ends with .../FeatureServer
    if stage == 'DEV':
        FIELD_DATA_serviceURL = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/service_8e527e6153ed488fad0414f309ed90ed/FeatureServer'
        SITES_serviceURL      = 'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/DPW_WP_SITES_DEV_2/FeatureServer'
        SITES_Edit_WebMap     = 'http://sdcounty.maps.arcgis.com/home/webmap/viewer.html?webmap=756b762cc8fe4a6b82e99d82753016a4'

    elif stage == 'BETA':
        FIELD_DATA_serviceURL = 'http://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/service_65a9e7bda7104cc18dbf6f76463db67d/FeatureServer'
        SITES_serviceURL      = ''
        SITES_Edit_WebMap     = 'http://sdcounty.maps.arcgis.com/home/webmap/viewer.html?webmap=cf87c1d763004981a7290609f11d8819'

    elif stage == 'PROD':
        FIELD_DATA_serviceURL = ''
        SITES_serviceURL      = ''
        SITES_Edit_WebMap     = ''

    # Token and AGOL variables
    gtURL       = "https://www.arcgis.com/sharing/rest/generateToken"
    AGOfields   = '*'
    FIELD_DATA_queryURL =  FIELD_DATA_serviceURL + '/0/query'        # Get FIELD DATA URL
    FIELD_DATA_gaURL    =  FIELD_DATA_serviceURL + '/CreateReplica'  # Get Attachments URL
    SITES_query_url     =  SITES_serviceURL      + '/0/query'        # Get SITES URL


    # Working database locations and names
    wkgFolder   = r'P:\DPW_ScienceAndMonitoring\Scripts\{v}\Data'.format(v = stage)
    wkgGDB      = "DPW_Science_and_Monitoring_wkg.gdb"

    # FIELD_DATA variables
    origFC      = "A_FIELD_DATA_orig"  # Name of downloaded FIELD_DATA
    wkgFC       = 'B_FIELD_DATA_wkg'   # Name of working FIELD_DATA

    #---------------------------------------------------------------------------
    #                          SITES variables

    SITES_FC_orig = 'D_SITES_orig' # FC name to give downloaded SITES data

    # Fields that must have a value in it in order for the SITES data to be
    #   considered 'valid'.  NOTE: Do not include StationID in this list,
    #   as that field is hardcoded to be checked in the Check_Sites_Data()
    SITES_required_fields = ['Copermittee', 'WMA', 'Site_Type', 'Loc_Desc',
                             'Site_Status']

    x_field = 'Long_X_NAD83'  # Name of the X field
    y_field = 'Lat_Y_NAD83'   # Name of the Y field

    #---------------------------------------------------------------------------

    # Production locations and names
    prodGDB                = wkgFolder + "\\DPW_Science_and_Monitoring_prod.gdb"
    prodPath_FldData       = prodGDB + '\\DPW_WP_FIELD_DATA'
    prodPath_SitesData     = prodGDB + '\\DPW_WP_SITES'
    prod_attachments       = wkgFolder + '\\Sci_Monitoring_pics'
    prodPath_Excel         = wkgFolder + '\\Excel'

##    # Survey123 CSV file related variables
##    # site_info = the CSV Survey123 uses to locate the sites in the app. It gets
##    # its data refreshed from the DPW_WP_SITES Feature Class in the
##    # DPW_WP_SITES_To_Survey123_csv() function
##    site_info = r"C:\Users\mgrue\ArcGIS\My Survey Designs\DPW Sci and Mon {}\media\Site_Info.csv".format(stage)
##    Sites_Export_To_CSV_tbl = wkgFolder + '\\' + wkgGDB + '\\E_SITES_export_to_csv'

    # Misc
    log_file = wkgFolder + r'\Logs\DPW_Science_and_Monitoring'
    errorSTATUS = 0
    os.chdir(wkgFolder) # Makes sure we are in the correct directory (if called from Task Scheduler)
    excel_report = ''   # Sets variable to '' if the Export_To_Excel() is not run to create it.

    # Lists
    # These lists are needed in the Email_Results(), but may not be created
    # if no data is downloaded.  Created here to not cause a fail
    ls_type_3_dups = []

    #---------------------------------------------------------------------------
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
            # os.path.split below is used to return just the path to the folder where the log_file lives
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
            origPath, SmpEvntIDs_dl = Get_Data(AGOfields, token, FIELD_DATA_queryURL,
                                                wkgFolder, wkgGDB, origFC,
                                                dt_last_ret_data)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Data', e)

        # Set flag data_was_downloaded based on the number of records in
        # SmpEvntIDs_dl.  This will be used to determine if other functions are
        # called in the main() function.
        if ((len(SmpEvntIDs_dl) == 0) or (SmpEvntIDs_dl == None)):
            data_was_downloaded = False

        else:
            data_was_downloaded = True

    #---------------------------------------------------------------------------
    # Get the ATTACHMENTS from the online database and store it locally
    if (errorSTATUS == 0 and data_was_downloaded and run_Get_Attachments):
        try:
            attach_fldr = Get_Attachments(token, FIELD_DATA_gaURL, prod_attachments,
                                          SmpEvntIDs_dl, dt_to_append)

        except Exception as e:
            errorSTATUS = Error_Handler('Get_Attachments', e)

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                      Pause DPW_WP_FIELD_DATA processing
    #                      Start DPW_WP_SITES processing
    #---------------------------------------------------------------------------

    if run_Get_SITES_data:
        # Get the Data from the DPW_WP_SITES on AGOL
        if (errorSTATUS == 0):
            try:
                Get_AGOL_Data_All(AGOfields, token, SITES_serviceURL, 0, wkgFolder, wkgGDB, SITES_FC_orig)
            except Exception as e:
                errorSTATUS = Error_Handler('Get_AGOL_Data_All', e)

        # Check the downloaded SITES data for any errors, email if errors
        if (errorSTATUS == 0):

            # Set the path to the downloaded SITES data
            SITES_wkg_data = wkgFolder + '\\' + wkgGDB + '\\' + SITES_FC_orig

            try:
                SITES_valid_data = Check_Sites_Data(SITES_wkg_data, SITES_required_fields,
                                               prodPath_SitesData, dpw_email_list, stage,
                                               SITES_Edit_WebMap)
            except Exception as e:
                errorSTATUS = Error_Handler('Check_Sites_Data', e)

        # Calculate the X and Y fields
        if (errorSTATUS == 0):
            if SITES_valid_data:
                try:
                    x_calc  = '!SHAPE.CENTROID.X!'
                    y_calc  = '!SHAPE.CENTROID.Y!'
                    p_version  = 'PYTHON_9.3'

                    print 'Calculating X and Y fields for:\n  {}'.format(SITES_wkg_data)

                    # Calculate the X field
                    print '    Calculating field: "{}" as "{}"'.format(x_field, x_calc)
                    arcpy.CalculateField_management(SITES_wkg_data, x_field,
                                                              x_calc, p_version)

                    # Calculate the Y field
                    print '    Calculating field: "{}" as "{}"'.format(y_field, y_calc)
                    arcpy.CalculateField_management(SITES_wkg_data, y_field,
                                                              y_calc, p_version)

                except Exception as e:
                    errorSTATUS = Error_Handler('main', e)

            else:
                print '*** ERROR! SITES data is NOT valid.  X and Y fields not calculated, please fix QA/QC errors above. ***'

        # Delete prod SITES features and append wkg SITES features
        if (errorSTATUS == 0):
            if SITES_valid_data:
                try:
                    # Delete the prod SITES features
                    print '  Deleting Features at: {}'.format(prodPath_SitesData)
                    arcpy.DeleteFeatures_management(prodPath_SitesData)

                    # Append SITES features from wkg SITES to prod SITES
                    print '  Appending Data'
                    print '    From: {}'.format(SITES_wkg_data)
                    print '      To: {}'.format(prodPath_SitesData)
                    arcpy.Append_management(SITES_wkg_data, prodPath_SitesData, 'TEST')

                except Exception as e:
                    errorSTATUS = Error_Handler('main', e)
            else:
                print '*** ERROR! SITES data is NOT valid.  Data not copied to prod database, please fix errors above. ***'


    #---------------------------------------------------------------------------
    #                      Finished DPW_WP_SITES processing
    #                      Resume   DPW_WP_FIELD_DATA processing
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    # COPY the original data to a working FC
    if (errorSTATUS == 0 and data_was_downloaded and run_Copy_Orig_Data):
        try:
            wkgPath = Copy_Orig_Data(wkgFolder, wkgGDB, wkgFC, origPath)

        except Exception as e:
            errorSTATUS = Error_Handler('Copy_Orig_Data', e)

    #---------------------------------------------------------------------------
    # ADD FIELDS to the working FC
    if (errorSTATUS == 0 and data_was_downloaded and run_Add_Fields):
        try:
            Add_Fields(wkgPath, add_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Add_Fields', e)

    #---------------------------------------------------------------------------
    # CALCULATE FIELDS in the working FC
    if (errorSTATUS == 0 and data_was_downloaded and run_Calculate_Fields):
        try:
            Calculate_Fields(wkgPath, calc_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Calculate_Fields', e)

    #---------------------------------------------------------------------------
    # DELETE FIELDS from the working FC
    if (errorSTATUS == 0 and data_was_downloaded and run_Delete_Fields):
        try:
            Delete_Fields(wkgPath, delete_fields_csv)

        except Exception as e:
            errorSTATUS = Error_Handler('Delete_Fields', e)

    #---------------------------------------------------------------------------
    # EXPORT FC to TABLE
    if (errorSTATUS == 0 and data_was_downloaded and run_FC_To_Table):
        try:
            exported_table = FC_To_Table(wkgFolder, wkgGDB, wkgPath)

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
    # SET THE LAST TIME the data was retrieved from AGOL to the start_time
    # so that it can be used in the where clauses the next time the script is run
    if (errorSTATUS == 0 and data_was_downloaded and run_Set_Last_Data_Ret):
        try:
            Set_Last_Data_Ret(last_data_retrival_csv, start_time)

        except Exception as e:
            errorSTATUS = Error_Handler('Set_Last_Data_Ret', e)

    #---------------------------------------------------------------------------
    # APPEND the data
    if (errorSTATUS == 0 and data_was_downloaded and run_Append_Data):
        try:
            Append_Data(exported_table, prodPath_FldData, field_mappings)

        except Exception as e:
            errorSTATUS = Error_Handler('Append_Data', e)

    #---------------------------------------------------------------------------
    # Handle the DUPLICATES
    if (errorSTATUS == 0 and data_was_downloaded and run_Duplicate_Handler):
        try:
            ls_type_3_dups = Duplicate_Handler(prodPath_FldData)

        except Exception as e:
            errorSTATUS = Error_Handler('Duplicate_Handler', e)

    #---------------------------------------------------------------------------
    # Email results
    if (run_Email_Results):
            try:
                Email_Results(errorSTATUS, cfgFile, dpw_email_list, lueg_admin_email,
                              log_file_date, start_time, dt_last_ret_data, prodGDB,
                              prod_attachments, SmpEvntIDs_dl,
                              stage, ls_type_3_dups)

            except Exception as e:
                errorSTATUS = Error_Handler('Email_Results', e)

    #---------------------------------------------------------------------------
    #                  Print out info about the script
    #---------------------------------------------------------------------------
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

#-------------------------------------------------------------------------------
#                                 DEFINE FUNCTIONS
#-------------------------------------------------------------------------------
#                 FUNCTION:    Get Start Date and Time

def Get_DateAndTime():
    """
    PARAMETERS:
      None

    VARS:
      start_time (dt object)
      date (str): in the format YYYY_MM_DD - No '0' padding
      time (str): in the format HH_MM_SS   - No '0' padding
      dt_to_append (str) : date and time merged into one string

    RETURNS:
      dt_to_append (str): In the format 'YYYY_M_D__H_M_S'
      start_time (dt object): A datetime object created in this function with
        the time of 'now'.

    FUNCTION:
      Get the current date and time so that it can be used in multiple places in
      the rest of the script where a unique name is desired.
      It is also used to get the start_time variable which is written to the
      LastDataRetrival.csv so a record is kept for when the data was last
      retrieved from AGOL.
    """

    print '--------------------------------------------------------------------'
    print 'Getting Date and Time...'

    start_time = datetime.datetime.now()

    date = '%s_%s_%s' % (start_time.year, start_time.month, start_time.day)
    time = '%s_%s_%s' % (start_time.hour, start_time.minute, start_time.second)

    dt_to_append = '%s__%s' % (date, time)

    print '  Date to append: {}'.format(dt_to_append)

    print 'Successfully got Date and Time\n'

    return dt_to_append, start_time

#-------------------------------------------------------------------------------
#              FUNCTION:    Get_Last_Data_Retrival Date

def Get_Last_Data_Retrival(last_ret_csv):
    """
    PARAMETERS:
      last_ret_csv (str): The path to the CSV that stores the date and time
        the script was last run.

    VARS:
      last_ret_str (str): The date and time string read from the CSV's
        3rd row, 1st column.

    RETURNS:
      dt_last_ret_data (dt obj): A datetime object converted from last_ret_str.

    FUNCTION:
      Reads the CSV at the last_ret_csv path to get when the script was last run.
      We want to know this so that we can use it in the query to ask AGOL
      to return features that have been created between the last time the
      script was run and the start_time obtained in the FUNCTION Get_DateAndTime()
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
#-------------------------------------------------------------------------------
#                       FUNCTION:    Get AGOL token

def Get_Token(cfgFile, gtURL):
    """
    PARAMETERS:
      cfgFile (str):
        Path to the .txt file that holds the user name and password of the
        account used to access the data.  This account must be in a group
        that has access to the online database.
      gtURL (str): URL where ArcGIS generates tokens.

    VARS:
      token (str):
        a string 'password' from ArcGIS that will allow us to to access the
        online database.

    RETURNS:
      token (str): A long string that acts as an access code to AGOL servers.
        Used in later functions to gain access to our data.

    FUNCTION: Gets a token from AGOL that allows access to the AGOL data.
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
    ##print token  # For testing purposes

    print "Successfully retrieved token.\n"

    return token

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION:    Get AGOL Data
#############################################################################################################
### http://blogs.esri.com/esri/arcgis/2013/10/10/quick-tips-consuming-feature-services-with-geoprocessing/
### https://geonet.esri.com/thread/118781
#############################################################################################################

def Get_Data(AGOfields_, token, queryURL_, wkgFolder, wkgGDB_, origFC, dt_last_ret_data_):
    """
    PARAMETERS:
      AGOfields_ (str) = The fields we want to have the server return from our query
      token (str) = The token obtained by the Get_Token() which gives access to
        AGOL databases that we have permission to
      queryURL_ (str) = The URL address for the feature service that allows us
        to query the database.
      wkgFolder (str) = Full path to the 'Data' folder that contains the FGDB's,
        Excel files, Logs, and Pictures.
      wkgGDB_ (str) = Name of the working FGDB in the wkgFolder.
      origFC (str) = Name of the FC that will hold the original data downloaded
        by this function.  This FC gets overwritten every time the script is run.
      dt_last_ret_data_ (dt obj): The dt object obtained by Get_Last_Data_Retrival()
        that represents the last time data was retrieved.  Used in the query in
        this function.

    RETURNS:
      origPath_ (str): Full path to the FC created to hold the original data
        downloaded by this function.
      SmpEvntIDs_dl (list of str): A list of STRINGS of all the Sample Event ID's
        that were downloaded this run.

    FUNCTION:
      To get the FIELD_DATA from Survey123.  This is a customized function that
      is used for FIELD_DATA only.  To download other data from AGOL, please use
      'Get_AGOL_Data()'.

      The Get_Data function takes the token we obtained from 'Get_Token'
      function, establishs a connection to the data, creates a FGDB (if needed),
      creates a unique FC to store the data, and then copies the data from AGOL
      to the unique FC.
    """

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

    # Use the dt_last_ret_data variable and tomorrow variable to set the 'where'
    # clause
    where = "CreationDate BETWEEN '{dt.year}-{dt.month}-{dt.day}' and '{ptd.year}-{ptd.month}-{ptd.day}'".format(dt = dt_last_ret_data_, ptd = plus_two_days)
    print '  Getting data where: {}\n    (Second date projected 2 days into future because of conversion from PST to UTC)'.format(where)

    # Encode the where statement so it is readable by URL protocol (ie %27 = ' in URL
    # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding
    where_encoded = urllib.quote(where)

    # If you suspect the where clause is causing the problems, uncomment the below 'where = "1=1"' clause
    ##where = "1=1"  # For testing purposes
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
        ##print 'fsURL %s' % fsURL  # For testing purposes
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
def Get_Attachments(token, gaURL, gaFolder, SmpEvntIDs_dl, dt_to_append):
    """
    PARAMETERS:
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

    VARS:
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

    RETURNS:
        gaFolder (str):
            So that the email can send that information.

    FUNCTION:
      Gets the attachments (images) that are related to the database features and
      stores them as .jpg in a local file inside the wkgFolder.
    """

    print '--------------------------------------------------------------------'
    print 'Getting Attachments...'

    # Flag to set if Attachments were downloaded.  Set to 'True' if downloaded
    attachment_dl = False

    #---------------------------------------------------------------------------
    #                       Get the attachments url (ga)
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
    ##print '  Replica URL: %s' % str(replicaUrl)  # For testing purposes

    # Set the token into the URL so it can be accessed
    replicaUrl_token = replicaUrl + '?&token=' + token + '&f=json'
    ##print '  Replica URL Token: %s' % str(replicaUrl_token)  # For testing purposes

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
    print '  Temp JSON file saved to: ' + jsonFilePath

    #---------------------------------------------------------------------------
    #                       Save the attachments

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
        gaRelId = attachment['parentGlobalId']

        # Now loop through all of the 'features' and break once the corresponding
        #  GlobalId's match so we can save based on the 'StationID'
        #  and 'SampleEventID'
        for feature in data['layers'][0]['features']:
            origId = feature['attributes']['globalid']
            StationID = feature['attributes']['StationID']
            SampleEventID = str(feature['attributes']['SampleEventID'])
            if origId == gaRelId:
                break

        # Test to see if the StationID is one of the features downloaded in
        # FUNCTION Get_Data. Download if so, ignore if not
        if SampleEventID in SmpEvntIDs_dl:
            attachName = '%s__%s' % (StationID, SampleEventID)
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

    if (attachment_dl == False):
        print '    No attachments saved this run.  OK if no attachments submitted since last run.'

    print '  All attachments can be found at: %s' % gaFolder

    # Delete the JSON file since it is no longer needed.
    print '  Deleting JSON file'
    os.remove(jsonFilePath)

    print 'Successfully got attachments.\n'

    return gaFolder

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                             FUNCTION Get_AGOL_Data_All()
def Get_AGOL_Data_All(AGOL_fields, token, FS_url, index_of_layer, wkg_folder, wkg_FGDB, orig_FC):
    """
    PARAMETERS:
      AGOL_fields (str) = The fields we want to have the server return from our query.
        use the string ('*') to return all fields.
      token (str) = The token obtained by the Get_Token() which gives access to
        AGOL databases that we have permission to access.
      FS_url (str) = The URL address for the feature service.
        Should be the service URL on AGOL (up to the '/FeatureServer' part).
      index_of_layer (int)= The index of the specific layer in the FS to download.
        i.e. 0 if it is the first layer in the FS, 1 if it is the second layer, etc.
      wkg_folder (str) = Full path to the folder that contains the FGDB that you
        want to download the data into.  FGDB must already exist.
      wkg_FGDB (str) = Name of the working FGDB in the wkg_folder.
      orig_FC (str) = The name of the FC that will be created to hold the data
        downloaded by this function.  This FC gets overwritten every time the
        script is run.

    RETURNS:
      None

    FUNCTION:
      To download ALL data from a layer in a FS on AGOL, using OBJECTIDs.
      This function, establishs a connection to the
      data, finds out the number of features, gets the highest and lowest OBJECTIDs,
      and the maxRecordCount returned by the server, and then loops through the
      AGOL data and downloads it to the FGDB.  The first time the data is d/l by
      the script it will create a FC.  Any subsequent loops will download the
      next set of data and then append the data to the first FC.  This looping
      will happen until all the data has been downloaded and appended to the one
      FC created in the first loop.

    NOTE:
      Need to have obtained a token from the Get_Token() function.
      Need to have an existing FGDB to download data into.
    """
    print '--------------------------------------------------------------------'
    print 'Starting Get_AGOL_Data_All()'

    # Set URLs
    query_url = FS_url + '/{}/query'.format(index_of_layer)
    print '  Downloading all data found at: {}/{}\n'.format(FS_url, index_of_layer)

    #---------------------------------------------------------------------------
    #        Get the number of records are in the Feature Service layer

    # This query returns ALL the OBJECTIDs that are in a FS regardless of the
    #   'max records returned' setting
    query = "?where=1=1&returnIdsOnly=true&f=json&token={}".format(token)
    obj_count_URL = query_url + query
    ##print obj_count_URL  # For testing purposes
    response = urllib2.urlopen(obj_count_URL)  # Send the query to the web
    obj_count_json = json.load(response)  # Store the response as a json object
    try:
        object_ids = obj_count_json['objectIds']
    except:
        print 'ERROR!'
        print obj_count_json['error']['message']
    num_object_ids = len(object_ids)
    print '  Number of records in FS layer: {}'.format(num_object_ids)

    #---------------------------------------------------------------------------
    #                  Get the lowest and highest OBJECTID
    object_ids.sort()
    lowest_obj_id = object_ids[0]
    highest_obj_id = object_ids[num_object_ids-1]
    print '  The lowest OBJECTID is: {}\n  The highest OBJECTID is: {}'.format(\
                                                  lowest_obj_id, highest_obj_id)

    #---------------------------------------------------------------------------
    #               Get the 'maxRecordCount' of the Feature Service
    # 'maxRecordCount' is the number of records the server will return
    # when we make a query on the data.
    query = '?f=json&token={}'.format(token)
    max_count_url = FS_url + query
    ##print max_count_url  # For testing purposes
    response = urllib2.urlopen(max_count_url)
    max_record_count_json = json.load(response)
    max_record_count = max_record_count_json['maxRecordCount']
    print '  The max record count is: {}\n'.format(str(max_record_count))


    #---------------------------------------------------------------------------

    # Set the variables needed in the loop below
    start_OBJECTID = lowest_obj_id  # i.e. 1
    end_OBJECTID   = lowest_obj_id + max_record_count - 1  # i.e. 1000
    last_dl_OBJECTID = 0  # The last downloaded OBJECTID
    first_iteration = True  # Changes to False at the end of the first loop

    while last_dl_OBJECTID <= highest_obj_id:
        where_clause = 'OBJECTID >= {} AND OBJECTID <= {}'.format(start_OBJECTID, end_OBJECTID)

        # Encode the where_clause so it is readable by URL protocol (ie %27 = ' in URL).
        # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding.
        # If you suspect the where clause is causing the problems, uncomment the
        #   below 'where = "1=1"' clause.
        ##where_clause = "1=1"  # For testing purposes
        print '  Getting data where: {}'.format(where_clause)
        where_encoded = urllib.quote(where_clause)
        query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where_encoded, AGOL_fields, token)
        fsURL = query_url + query

        # Create empty Feature Set object
        fs = arcpy.FeatureSet()

        #---------------------------------------------------------------------------
        #                 Try to load data into Feature Set object
        # This try/except is because the fs.load(fsURL) will fail whenever no data
        # is returned by the query.
        try:
            ##print 'fsURL %s' % fsURL  # For testing purposes
            fs.load(fsURL)
        except:
            print '*** ERROR, data not downloaded ***'

        #-----------------------------------------------------------------------
        # Process d/l data

        if first_iteration == True:  # Then this is the first run and d/l data to the orig_FC
            path = wkg_folder + "\\" + wkg_FGDB + '\\' + orig_FC
        else:
            path = wkg_folder + "\\" + wkg_FGDB + '\\temp_to_append'

        #Copy the features to the FGDB.
        print '    Copying AGOL database features to: %s' % path
        arcpy.CopyFeatures_management(fs,path)

        # If this is a subsequent run then append the newly d/l data to the orig_FC
        if first_iteration == False:
            orig_path = wkg_folder + "\\" + wkg_FGDB + '\\' + orig_FC
            print '    Appending:\n      {}\n      To:\n      {}'.format(path, orig_path)
            arcpy.Append_management(path, orig_path, 'NO_TEST')

            print '    Deleting temp_to_append'
            arcpy.Delete_management(path)

        # Set the last downloaded OBJECTID
        last_dl_OBJECTID = end_OBJECTID

        # Set the starting and ending OBJECTID for the next iteration
        start_OBJECTID = end_OBJECTID + 1
        end_OBJECTID   = start_OBJECTID + max_record_count - 1

        # If we reached this point we have gone through one full iteration
        first_iteration = False
        print ''

    if first_iteration == False:
        print "  Successfully retrieved data.\n"
    else:
        print '  * WARNING, no data was downloaded. *'

    print 'Finished Get_AGOL_Data_All()'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def Check_Sites_Data(wkg_sites_data, required_fields, prod_sites_data, email_list
                     stage, SITES_Edit_WebMap):
    """
    PARAMETERS:
      wkg_sites_data (str): Full path to the downloaded SITES data in a wkg FGDB.
      required_fields (str): List of strings that contains the field names of
        fields that must have a value.  'NULL' and blanks ('') count as not
        having a value.
      prod_sites_data (str): Full path to the production SITES data in a prod
        FGDB.
      email_list (str): List of strings that contains the email addresses of
        anyone who should receive emails if there are 'error' or 'info' emails.
      stage (str): The stage (DEV, BETA, PROD) that this script is running in.
        Used in the email subject line.
      SITES_Edit_WebMap (str): The URL to the Edit map for that stage. This is
        used in the error/info emails so that the DPW users can go directly to
        the web map.

    RETURNS:
      valid_data (Boolean): Returned as 'True' unless there is any one of the
        possible errors checked for below--then it is returned as 'False'.  This
        boolean can be used in the main function to control if the SITES data
        is copied from the wkg database to the prod database.

    FUNCTION:
      To QA/QC for a variety of possible errors in the SITES data from AGOL.
      If there are errors, the returned 'valid_data' boolean value can be used
      to prevent data with errors from overwriting good production data.  An
      email will be sent for each check that results in an error.  If any errors
      exist 'valid_data' will be turned to 'False'.

      The process for this function is as follows:
        1. Check for duplicate Station IDs in the working Sites data
             --Send an email if error
        2. Check for any NULL values in required fields
             --Send an email if error
        3. Check for any NULL values in StationID
             --Send an email if error
        4. Check for any StationID's in the production database that are not
           in the working data
             --Send an email if error
        5. Report any StationID's that are in wkg data but not prod
           (These are probably newly added sites, and not errors)
             --Send an email if any new site in AGOL (not an error email)

    NOTE: This function assumes that the function 'Email_W_Body()' is accessible.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Check_Sites_Data()'
    print '  Checking: {}'.format(wkg_sites_data)

    valid_data = True

    how_to_login =
    """
    To log into the AGOL database, please:<br>
    1. Visit {}<br>
    2. Sign In using your username and password you use to sign into Collector
       and Survey123.<br>
    3. You should be automatically directed to the EDIT Web Map where you can
       make your edits.<br>
    """.format(SITES_Edit_WebMap)

    #---------------------------------------------------------------------------
    #          Check for duplicate Station IDs in the working Sites data
    print '\n  Checking for any duplicate Station IDs'

    # Make a list of all the site ID's in the working data
    unique_ids = []
    duplicate_ids = []

    where_clause = "StationID <> ''"  # No need to search for blank duplicates
    with arcpy.da.SearchCursor(wkg_sites_data, ['StationID'], where_clause) as cursor:
        for row in cursor:
            if row[0] not in unique_ids:
                unique_ids.append(row[0])
            else:
                duplicate_ids.append(row[0])
    del cursor

    if len(duplicate_ids) == 0:  # Then there were no errors
        print '    No duplicate Station IDs'

    else:  # Then there were errors, send an email
        valid_data = False
        duplicate_ids.sort()

        print '*** ERROR! There is/are {} Station IDs that are in the AGOL database more than once ***'.format(str(len(duplicate_ids)))
        print '  Sending an email to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = '{} -- Sci and Mon Error.  There are duplicate Station IDs in the SITES database on AGOL'.format(stage)

        # Format the Body in html
        list_to_string = '<br> '.join(duplicate_ids)

        body = ("""DPW staff, please log on to the AGOL SITES database and
        correct the below duplicates:<br>
        (<i>NOTE: If a Station ID is listed more than once, that ID has more than
        one duplicate</i>)<br><br>
        {}
        <br><br>
        The AGOL SITES database will not be loaded into the production database
        until this error is resolved.
        <br><br>
        {}""".format(list_to_string, how_to_login))

        # Send the email
        Email_W_Body(subj, body, email_list)

    #---------------------------------------------------------------------------
    #             Check for any NULL values in required fields
    print '\n  Checking for any NULL values in required fields'
    ids_w_null_values = []

    for field in required_fields:
        where_clause = "{fld} IS NULL OR {fld} = ''".format(fld = field)
        print '    Checking field: "{}" where: {}'.format(field, where_clause)

        # Get list of StationIDs with NULL values in required fields
        with arcpy.da.SearchCursor(wkg_sites_data, ['StationID', field], where_clause) as cursor:
            for row in cursor:
                if row[0] != None:  # Dont append if there is no StationID
                    ids_w_null_values.append(row[0])
        del cursor

    if len(ids_w_null_values) == 0:  # Then there were no errors
        print '    No null values for any required field'

    else:  # Then there were errors, send an email
        valid_data = False
        ids_w_null_values.sort()

        print '*** ERROR!  There are Station IDs that have NULL values in required fields ***'
        print '  Sending an email to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = '{} -- Sci and Mon Error.  There are null values in required fields'.format(stage)

        # Format the Body in html

        list_to_string = '<br> '.join(ids_w_null_values)
        req_fields_str = ', '.join(required_fields)

        body = ("""DPW staff, the following Station IDs have a NULL value for a
        required field, please logon to the AGOL SITES database and enter values
        for required fields.
        <br>
        (<i>NOTE: If a Station ID is listed more than once, it has NULL values
        for multiple required fields<br>
        The required fields are:</i> <b>{}</b>)
        <br><br>
        {}
        <br><br>
        The AGOL SITES database will not be loaded into the production database
        until this error is resolved.
        <br><br>
        {}""".format(req_fields_str, list_to_string, how_to_login))

        # Send the email
        Email_W_Body(subj, body, email_list)

    #---------------------------------------------------------------------------
    #             Check for any NULL values in StationID
    print '\n  Checking for any NULL values in StationID'
    num_blank_station_ids = 0

    where_clause = "StationID IS NULL OR StationID = ''"
    print '    Where: {}'.format(where_clause)
    with arcpy.da.SearchCursor(wkg_sites_data, ['StationID'], where_clause) as cursor:
        for row in cursor:
            num_blank_station_ids += 1
    del cursor

    if num_blank_station_ids == 0:  # Then there were no errors
        print '    There are no NULL values for StationID'

    else:  # Then there were errors, send an email
        valid_data = False

        print '*** ERROR! There is/are {} Site(s) with a NULL value in StationID'.format(num_blank_station_ids)
        print '  Sending an email to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = '{} -- Sci and Mon Error.  There are stations with no Station IDs'.format(stage)

        # Format the Body in html
        body = ("""DPW staff, please log on to the AGOL SITES database and
        enter a value in the "Station ID" field for the <b>{} Station(s)</b> that
        are blank in that field.
        <br><br>
        The AGOL SITES database will not be loaded into the production database
        until this error is resolved.
        <br><br>
        {}""".format(num_blank_station_ids, how_to_login))

        # Send the email
        Email_W_Body(subj, body, email_list)
    #---------------------------------------------------------------------------
    #            Check for any StationID's in the production database
    #                   that are not in the working data

    print '\n  Checking that all StationIDs in prod are also in the wkg data'

    # Get list of Station ID's that are in the prod data
    prod_station_IDs = []
    with arcpy.da.SearchCursor(prod_sites_data, ['StationID']) as cursor:
        for row in cursor:
            prod_station_IDs.append(row[0])
    del cursor

    # Get list of Station ID's that are in the working data
    wkg_station_IDs = []
    with arcpy.da.SearchCursor(wkg_sites_data, ['StationID']) as cursor:
        for row in cursor:
            wkg_station_IDs.append(row[0])
    del cursor

    # See if each prod Station ID is in the wkg data
    num_in_prod_not_in_wkg = 0
    prod_ids_not_in_wkg = []
    for prod_id in prod_station_IDs:
        if prod_id not in wkg_station_IDs:
            prod_ids_not_in_wkg.append(prod_id)

    if len(prod_ids_not_in_wkg) == 0:
        print '    All Station IDs in prod also in wkg data'

    else:
        valid_data = False
        print '*** ERROR! There are {} Station IDs in the prod database, but is/are missing from the wkg database ***'.format(str(len(prod_ids_not_in_wkg)))
        print '  Sending an email to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = '{} -- Sci and Mon Error.  There are stations we cannot find in the AGOL database'.format(stage)

        # Format the Body in html
        list_to_string = '<br> '.join(prod_ids_not_in_wkg)

        body = ("""DPW staff, please log on to the AGOL SITES database and
        find out what happened to the below sites.<br>
        The below sites exist in the production database, but cannot be found on
        AGOL.  They have been either renamed to a new Station ID, or they have
        been deleted on AGOL:
        <br><br>
        {}
        <br><br>
        The AGOL SITES database will not be loaded into the production database
        until this error is resolved.
        <br><br>
        {}""".format(list_to_string, how_to_login))

        # Send the email
        Email_W_Body(subj, body, email_list)
    print ''

    #---------------------------------------------------------------------------
    #        Report any StationID's that are in wkg data but not prod
    #         (These are probably newly added sites, and not errors)
    print '  Getting a list of all Station IDs that are in wkg data, but not in prod.'

    # Get a list of Station IDs that are in wkg data but not prod
    wkg_station_ids_not_in_prod = []
    ignore_values = [None, '']
    for wkg_id in wkg_station_IDs:
        if wkg_id not in prod_station_IDs and wkg_id not in ignore_values:
            wkg_station_ids_not_in_prod.append(wkg_id)

    # Report findings
    if len(wkg_station_ids_not_in_prod) == 0:
        print '    All Station IDs in wkg data already in prod\n'

    else:  # There are Station IDs that are in wkg data but not in prod yet
        print '    There are Station IDs in wkg data that are not in prod'
        print '    This is not necessarily an error, but emailing list to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = '{} -- Sci and Mon Info.  There were new Station IDs added to AGOL'.format(stage)

        # Format the Body in html

        list_to_string = '<br> '.join(wkg_station_ids_not_in_prod)

        body = ("""DPW staff, the below list are new Station IDs that have been
        added to AGOL database that have not been recorded before.<br>
        If they are all <b>new</b> stations, no action is needed on your part,
        The sites have been added to the production database.
        <br><br>
        <i>However, it is possible that one of the below stations is not a <b>new</b>
        station, but that it was renamed from an <b>existing</b> station.<br>
        This would be a human error because existing sites should not be renamed.<br>
        If you received (today) an email with the subject <u>"There are
        stations we cannot find in the AGOL database"</u>,<br>
        please check the below stations to confirm that they are
        valid new sites and were not renamed from existing sites.</i>
        <br><br>
        <b>Station ID:</b>
        <br>
        {}
        <br><br>
        You may not need to log into AGOL to edit the data, but if you do:
        <br><br>
        {}""".format(list_to_string, how_to_login))

        # Send the email
        Email_W_Body(subj, body, email_list)


    #---------------------------------------------------------------------------

    print '\n  valid_data = {}'.format(valid_data)

    print '\nFinished Check_Sites_Data()\n'
    return valid_data

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION:  Copy_Orig_Data

def Copy_Orig_Data(wkgFolder, wkgGDB, wkgFC, origPath):
    """
    PARAMETERS:
      wkgFolder (str) = Full path to the 'Data' folder that contains the FGDB's,
        Excel files, Logs, and Pictures.
      wkgGDB (str) = Name of the working FGDB in the wkgFolder.
      wkgFC (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.  It is overwritten each time the script is run.
      origPath (str) = Full path to the original FC in the wkgGDB.

    RETURNS:
      wkgPath (str): Full path to the working FC in the wkgGDB.

    FUNCTION:
      To copy the orig FC to a working FC in the same FGDB
    """

    print '--------------------------------------------------------------------'
    print 'Copying original data...'

    #---------------------------------------------------------------------------
    # Copy the orig FC to a working FEATURE CLASS to run processing on.
    in_features = origPath
    wkgPath = out_feature_class = r'%s\%s\%s' % (wkgFolder, wkgGDB, wkgFC)

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
    """
    PARAMETERS:
      wkg_data (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.  It is overwritten each time the script is run.
      add_fields_csv (str) = Full path to the CSV file that lists which fields
        should be created.

    RETURNS:
      None

    FUNCTION:
      To add fields to the wkg_data using a CSV file located at add_fields_csv.
    """

    print '--------------------------------------------------------------------'
    print 'Adding fields to:\n  %s' % wkg_data
    print '  Using Control CSV at:\n    {}\n'.format(add_fields_csv)
    with open (add_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Create blank lists
        f_names = []
        f_types = []
        f_lengths = []

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
    """
    PARAMETERS:
      wkg_data (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.  It is overwritten each time the script is run.
      calc_fields_csv (str) = Full path to the CSV file that lists which fields
        should be calculated, and how they should be calculated.

    RETURNS:
      None

    FUNCTION:
      To calculate fields in the wkg_data using a CSV file located at
      calc_fields_csv. The field [DateOfSurvey] is too complicated to pass a
      calculation via the CSV file, so it is handled in this script directly.
    """

    print '--------------------------------------------------------------------'
    print 'Calculating fields in:\n  %s' % wkg_data
    print '  Using Control CSV at:\n    {}\n'.format(calc_fields_csv)

    # Make a table view so we can perform selections
    arcpy.MakeTableView_management(wkg_data, 'wkg_data_view')

    #---------------------------------------------------------------------------
    #                     Get values from the CSV file: FieldsToCalculate.csv (calc_fields_csv)
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
            # Strip the auto-appended Time from the DateSurveyStart field
            # OPTION 1:
            if (field == 'DateOfSurvey'):
                try:
                    expression = "Remove_Time_From_Date(!{f}!)".format(f = field)
                    expression_type="PYTHON_9.3"
                    code_block = """def Remove_Time_From_Date(date):\n    if date is None:\n        return None\n    else:\n        return date.split(" ").pop(0)"""

                    # Process
                    arcpy.CalculateField_management(in_table, field, expression,
                                                        expression_type, code_block)
                    print '        From selected features, calculated field: %s, so that it removed the auto-appended time from the date\n' % field

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

            #-------------------------------------------------------------------
            # Test if the user wants to calculate the field being equal to
            # ANOTHER FIELD by seeing if the calculation starts or ends with an '!'
            # OPTION 2:
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
            # OPTION 3:
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
    """
    PARAMETERS:
      wkg_data (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.  It is overwritten each time the script is run.
      delete_fields_csv (str) = Full path to the CSV file that lists which
        fields should be deleted.

    RETURNS:
      None

    FUNCTION:
      To delete fields from the wkg_data using a CSV file located at
      delete_fields_csv.
    """

    print '--------------------------------------------------------------------'
    print 'Deleting fields in:\n    %s' % wkg_data
    print '  Using Control CSV at:\n    {}\n'.format(delete_fields_csv)

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
#                           FUNCTION: FC to Table
def FC_To_Table(wkgFolder, wkgGDB, wkgPath):
    """
    PARAMETERS:
      wkgFolder (str) = Full path to the 'Data' folder that contains the FGDB's,
        Excel files, Logs, and Pictures.
      wkgGDB (str) = Name of the working FGDB in the wkgFolder.
      wkgPath (str): Full path to the working FC in the wkgGDB.

    RETURNS:
      out_table (str) = Full path to the converted TABLE (from wkgPath)

    FUNCTION:
      The FC that has been altered and calculated above needs to be turned into
      a table first in order to append the data to the production table.
    """

    print '--------------------------------------------------------------------'
    print 'Exporting working FC to table:'
    in_features = wkgPath
    out_table = '{}\\{}\\C_FIELD_DATA_to_apnd'.format(wkgFolder, wkgGDB)

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
    """
    PARAMETERS:
      orig_table (str): Full path to the converted TABLE (from the working FC).
      prod_table (str): Full path to the production TABLE that contains the
        most up-to-date data.
      map_fields_csv (str): Full path to the CSV file that lists which fields
        in the converted TABLE match the fields in the production TABLE.

    RETURNS:
      fms (arcpy.FieldMappings object): This object contains the field mappings
        that can be used when we append the working data to the production data.

    FUNCTION:
      By using a CSV file that lists fields in the working data and their
      matching field in the production data, this functino gets any non-default
      field mappings between the working data and the production data. The
      field mapping object is returned by the function so it can be used in an
      append function.

    NOTE:
      This function is only useful if there are any field names that are
      different between the working database and the production database.
      This is because the default for an append function is to match the field
      names between the target dataset and the appending dataset.
    """

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

    print 'Successfully Got Field Mappings\n'
    return fms

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                    FUNCTION: Set Last Data Retrival time
def Set_Last_Data_Ret(last_ret_csv, start_time):
    """
    PARAMETERS:
      last_ret_csv (str): The path to the CSV that stores the date and time
        the script was last run.
      start_time (dt object): A datetime object created in the Get_DateAndTime()
        function with the time the script started.

    RETURNS:
      None

    FUNCTION:
      Now that the Data and Attachments have been downloaded, this function sets
      this script's start_time into the CSV file that will be used as the next
      runs last data retrival (dt_last_ret_data) in the
      Get_Last_Data_Retrival() function.
    """
    print '--------------------------------------------------------------------'
    print 'Setting LastDataRetrival.csv time so it equals this runs start_time...'

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
#                        FUNCTION:  APPEND DATA

def Append_Data(input_item, target, field_mapping=None):
    """
    PARAMETERS:
      input_item (str) = Full path to the item to append.
      target (str) = Full path to the item that will be updated.
      field_mapping {arcpy.FieldMappings obj} = Arcpy Field Mapping object.
        Optional.

    RETURNS:
      None

    FUNCTION:
      To append the data from the input_item to the target using an
      optional arcpy field_mapping object to override the default field mapping.
    """

    print '--------------------------------------------------------------------'
    print 'Appending Data...'
    print '  From: {}'.format(input_item)
    print '  To:   {}'.format(target)

    # Append working data to production data
    schema_type = 'NO_TEST'

    # Process
    arcpy.Append_management(input_item, target, schema_type, field_mapping)

    print 'Successfully appended data.\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                         FUNCTION Duplicate Handler

def Duplicate_Handler(target_table):
    """
    PARAMETERS: 'target_table'.  The table to search for the duplicates.

    RETURNS: 'ls_type_3_dups'.  A list of Type 3 duplicates (if any) that were
        found during this run of the script.

    This function does 4 sub tasks:
      A) Get a list of all the SampleEventIDs that occur more than once in the
           target_table (considered 'Duplicates').
           Function stopped if there are no duplicates.
      B) Sort the duplicates into one of two duplicate categories:
            a. Type 1 or Type 2
            b. Type 3
            * Types explained below
      C) Handle the Type 1 or Type 2 duplicates by deleting all of the
           duplicates except for the youngest duplicate (per duplication).
           This means that only the youngest duplicate remains in the dataset.
           (Google 'Last one to sync wins'.  This is a common method for
           handling conflicting data.)
      D) Handle the Type 3 duplicates by renaming the SampleEventID for all
           Type 3 duplicates so that it is obvious that there was a duplicate.
           Make a list of Type 3 duplicates so that they can be mentioned in
           an email

    *TYPES OF DUPLICATES:
      Type 1:  Can occur if a survey is sent late enough in the day that the
          survey arrives to the online database the next day (UTC time).  This
          means the data is retrieved by script the next morning when it is run,
          AND is grabbed again the following day because the script is looking
          for all data that arrived to the database the previous day.  These
          duplicates are IDENTICAL.

          Can be FIXED by deleting either duplication.  We will delete the older.

      Type 2:  Occurs when users go into their 'Sent' folder on their device and
          opens up an already sent survey and resends the survey.  It may be an
          accident, or on purpose.  The survey may be DIFFERENT or IDENTICAL to
          the original, and there may be more than 2 of this type of duplicate.

          Can be FIXED by deleting all of these duplicates, except for the last
          submitted survey.  This is the 'youngest' survey and will act as the
          only true version.  This will allow the users to make corrections in
          the field.

      Type 3:  Occurs when two users start a survey within 1/10th of a second
          of each other on the same day.  Very rare (about once per decade if
          there are 3 monitors submitting 30 records M-F over 6 hours each day).
          These surveys will be completely DIFFERENT with the exception of the
          Sample Event ID.

          Can be FIXED by giving these duplicates a new SampleEventID that can
          still be easily converted back to the original SampleEventID.
          For example, appending an incrementing number to the end of the ID
          so that two duplicates of:
            '20170502.123456'
          becomes:
            '20170502.1234561'
          and:
            '20170502.1234562'
          This script will change the SampleEventIDs as described above

          NOTE: IF a Type 3 duplicate happens and then a monitor resends their
          survey (creaing a Type 2 duplicate), the SampleEventID will have
          Both a Type 3 and a Type 2 duplicate associated with it.  In this event
          The script will file this duplicate as a Type 3 and will rename all
          of the duplicates.
          It will not delete the Type 2 duplicate as might be expected.

          NOTE: If a Type 1 or 2 duplicate has an associated attachment, this
          script will not delete the attachment.  The attachments will be given
          the same names 'PEN-001_20170505.123456' and will append the next
          letter in the alphabet up to '_G'.  This will make it look like a
          different photo was downloaded, but this photo is a duplicate.
          This is a known limitation to the current function.
    """
    print '--------------------------------------------------------------------'
    print 'Starting Duplicate_Handler() for: "{}"...\n'.format(target_table)

    # The script will change the value of all SampleEventIDs of all Type 1 and 2
    # duplicates to 'dup_type_1_2_flag' in order to 'flag' them for deletion
    # later in the script
    dup_type_1_2_flag = 'Duplicate_Delete_Me'

    # This will be a list of the Type 3 duplicates that can be included in an
    # email if we set one up.  If there are no duplicates,
    ls_type_3_dups = ['No duplicates created by two users starting a survey at the same 1/10th of a second (Type 3 duplicates) found during this run of the script.']

    #---------------------------------------------------------------------------
    #              A)  Get list of all duplicate SampleEventIDs

    unique_list   = []
    dup_list      = []
    with arcpy.da.SearchCursor(target_table, ['SampleEventID']) as cursor:
        for row in cursor:

            # Only add duplicate if it is the first instance of a duplication
            if row[0] in unique_list and row[0] not in dup_list:
                dup_list.append((row[0]))

            # Add the SampleEventID to the unique list if it is not there already
            if row[0] not in unique_list:
                unique_list.append(row[0])

    #---------------------------------------------------------------------------
    #                 Stop function if there are no duplicates

    if (len(dup_list) == 0):
        print '  There are no duplicates in table'
        print '\nFinished Duplicate_Handler()'
        return ls_type_3_dups

    #---------------------------------------------------------------------------
    #                     B)  There were duplicates,
    #         categorize the duplicates into (Type 1, 2)  and (Type 3)

    dup_typ_1_2 = []  # List to hold the Type 1 and 2 duplicates
    dup_typ_3   = []  # List to hold the Type 3 duplicates

    dup_list.sort()

    print '  There is/are: "{}" duplicate(s) to categorize:'.format(str(len(dup_list)))

    for dup in dup_list:
        where_clause = "SampleEventID = '{}'".format(dup)
        with arcpy.da.SearchCursor(target_table, ['SampleEventID', 'Creator'], where_clause) as cursor:
            unique_creators = []
            for row in cursor:

                # Get the number of unique creators for this SampleEventID
                if row[1] in unique_creators:
                    pass
                else:
                    unique_creators.append(row[1])

            # Use the # of unique creators to dictate if we have a Type 1/2 or Type 3 duplicate
            if len(unique_creators) == 1:  # Then we have a Type 1 or Type 2 duplicate
                print '    SampleEventID: "{}" = Type 1 or 2 dup'.format(row[0])
                dup_typ_1_2.append(row[0])

            elif len(unique_creators) > 1: # Then we have a Type 3 duplicate
                print '    SampleEventID: "{}" = Type 3 dup'.format(row[0])
                dup_typ_3.append(row[0])

    #---------------------------------------------------------------------------
    #               C)  Handle Type 1 and Type 2 duplicates
    # Handle the Type 1 and 2 duplicates by changing the SampleEventID to
    # 'dup_type_1_2_flag' for all duplicates except for the youngest duplicate.
    # We want to keep the youngest Type 1 or 2 duplicate.
    print '\n  There are "{}" Type 1 / Type 2 duplicates:'.format(str(len(dup_typ_1_2)))

    if len(dup_typ_1_2) == 0:
        print '    So nothing to change'

    # If there are Type 1 / 2 duplicates...
    if len(dup_typ_1_2) > 0:

        # For each duplicated SampleEventID, go through each duplicate and leave
        # the youngest of them alone, but change all of the older ones to dup_typ_1_2_flag
        for dup in dup_typ_1_2:

            where_clause = "SampleEventID = '{}'".format(dup)
            sql_clause   = (None, 'ORDER BY OBJECTID DESC') # This will order the cursor to grab the youngest duplicate first

            with arcpy.da.UpdateCursor(target_table, ['SampleEventID', 'OBJECTID'], where_clause, '', '', sql_clause) as cursor:
                i = 0
                for row in cursor:
                    if i == 0:
                        print '\n    Not changing youngest SampleEventID duplicate: "{}"'.format(str(row[0]))

                    if i > 0:  # Only update the SampleEventID for the older duplicates (i.e. NOT the first duplicate in this cursor)
                        print '    Changing older duplicate SampleEventID from: "{}" to: "{}"'.format(str(row[0]), str(dup_type_1_2_flag))
                        row[0] = dup_type_1_2_flag
                        cursor.updateRow(row)

                    i += 1

        # Select the older Type 1 and Type 2 duplicates that were flagged for deletion
        arcpy.MakeTableView_management(target_table, 'target_table_view')

        where_clause = "SampleEventID = '{}'".format(str(dup_type_1_2_flag))
        arcpy.SelectLayerByAttribute_management('target_table_view', 'NEW_SELECTION', where_clause)

        # Test to see how many records were selected
        result = arcpy.GetCount_management('target_table_view')
        count_selected = int(result.getOutput(0))

        # Only perform deletion if there are selected rows
        if count_selected > 0:

            print '\n    Deleting "{}" Type 1 and Type 2 duplicates with SampleEventID = "{}"'.format(count_selected, dup_type_1_2_flag)

            # Delete the older Type 1 and Type 2 duplicates that were flagged for deletion
            arcpy.DeleteRows_management('target_table_view')

    #---------------------------------------------------------------------------
    #                      D)  Handle Type 3 Duplicates
    print '\n  There is/are "{}" Type 3 duplicate(s):'.format(str(len(dup_typ_3)))

    if len(dup_typ_3) == 0:
        print '    So nothing to change'

    # If there are Type 3 duplicates...
    if len(dup_typ_3) > 0:

        # If there are Type 3 duplicates, reset the list so we can start fresh and append to it below
        ls_type_3_dups = ['  Below are duplicates that were created by two or more users starting their survey at the same 1/10th of a second (Type 3 duplicate)\n  Their SampleEventIDs have been changed:']

        # For each duplicated SampleEventID, append a 7th number to the SampleEventIDs to make them unique
        for dup in dup_typ_3:

            num_to_append = 1
            where_clause = "SampleEventID = '{}'".format(dup)

            with arcpy.da.UpdateCursor(target_table, ['SampleEventID', 'Creator'], where_clause) as cursor:
                for row in cursor:

                    new_sampID = row[0] + str(num_to_append)
                    notification_dup_type_3 = '    SampleEventID: "{}" with Creator: "{}" was changed to: "{}"'.format(row[0], row[1], str(new_sampID))
                    row[0] = new_sampID
                    cursor.updateRow(row)

                    print notification_dup_type_3
                    ls_type_3_dups.append(notification_dup_type_3)

                    num_to_append += 1

        ls_type_3_dups.append("""  GIS: Please check the above Type 3 duplicates for any lingering Type 2 duplicates that still need to be removed from the database.
    Any associated attachments should be manually renamed.  See documentation in script for Duplicate Types in Duplicate_Handler() Function.""")

    print '\nFinished Duplicate_Handler()'

    return ls_type_3_dups

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                           FUNCTION:  Email Results
def Email_Results(errorSTATUS, cfgFile, dpw_email_list, lueg_admin_email, log_file_date,
                  start_time_obj, dt_last_ret_data, prod_FGDB, attach_folder,
                  dl_features_ls, stage, ls_type_3_dups):
    """
    PARAMETERS:
      <many>: This function is not a standard email function.  It receives a lot
      of data from the script that it reports on.

    RETURNS:
      None

    FUNCTION:
      To send an email after running.  Email may be:
        Successful. Data downloaded.
        Successful. Data not downloaded.
        Warning.    Data downloaded, but a Type 3 duplicate was found, GIS analysis needed
        Error.      There was an error with the script.
    """

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

    #---------------------------------------------------------------------------
    #                Write the "Success--Data Downloaded" email

    # If there are no errors and at least one feature was downloaded
    if (errorSTATUS == 0 and num_dl_features > 0):
        print '  Writing the "Success--Data Downloaded" email...'

        # Send this email to...
        email_list = lueg_admin_email

        # Set the Subject of the email
        first_type_3_duplicate = ls_type_3_dups[0]  # First result in list
        if not first_type_3_duplicate.startswith('No duplicates created by two users'):  # Will be triggered if there was a type 3 duplicate (Very rare event)
            subj = '{} -- WARNING! Completed DPW_Science_and_Monitoring.py Script.  Data Downloaded, but Type 3 duplicate found, see log file.'.format(stage)
        else:
            subj = '{} -- SUCCESSFULLY Completed DPW_Science_and_Monitoring.py Script.  Data Downloaded.'.format(stage)  # Normal email subject

        # Format the Body in html
        body  = ("""\
        <html>
          <head></head>
          <body>
            <h3>Info:</h3>
            <p>
               There was/were <b>{num}</b> feature(s) downloaded this run.<br>
               -----------------------------------------------------------------
            <br><br>
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

            <h3>File Locations (on GIS Blue Network):</h3>
            <p>
               The below files are on the GIS Blue Network, will not be available
               for DPW staff during Beta testing.                     <br>
               The <b>FGDB</b> is located at:            <i>{fgdb}</i><br>
               The <b>Images</b> are located at:         <i>{af}  </i><br>
               The <b>Log File</b> is located at:        <i>{lf}  </i><br>
            </p>
          </body>
        </html>
        """.format(st = start_time[0],
                   ft = finish_time[0], dlr = data_last_retrieved[0],
                   num = num_dl_features, fgdb = prod_FGDB, af = attach_folder,
                   lf = log_file_date))

    #---------------------------------------------------------------------------
    #             Write the "Success--No Data Downloaded' email

    # If there were no errors but no data was downloaded
    elif(errorSTATUS == 0 and num_dl_features == 0):
        print '  Writing the "Success--No Data Downloaded" email'

        # Send this email to the lueg_admin_emails
        email_list = lueg_admin_email

        # Format the Subject for the 'No Data Downloaded' email
        subj = '{} -- SUCCESSFULLY Completed DPW_Science_and_Monitoring.py Script.  NO Data Downloaded.'.format(stage)

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
                   dlr = data_last_retrieved[0], lf = log_file_date))

    #---------------------------------------------------------------------------
    #                        Write the "Errors" email

    # If there were errors with the script
    elif(errorSTATUS <> 0):
        print '  Writing "Error" email...'

        # Get the current working directory
        cwd = os.getcwd()

        # Send this email to the lueg_admin_emails
        email_list = lueg_admin_email

        # Format the Subject for the 'Errors' email
        subj = '{} -- ERROR with DPW_Science_and_Monitoring.py Script'.format(stage)

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
               The data is located at:          <i>{cwd}</i><br>
            </p>
          <body>
        </html>

        """.format(st = start_time[0], ft = finish_time[0], lf = log_file_date, cwd = cwd))

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                              Send the Email

    # Set the subj, From, To, and body
    msg = MIMEMultipart()
    msg['Subject']   = subj
    msg['From']      = "Python Script"
    msg['To']        = ', '.join(email_list)  # Join each item in list with a ', '
    msg.attach(MIMEText(body, 'html'))

    # Get username and password from cfgFile
    config = ConfigParser.ConfigParser()
    config.read(cfgFile)
    email_usr = config.get('email', 'usr')
    email_pwd = config.get('email', 'pwd')

    # Send the email
    print '  Sending the email to: \n    {}'.format(email_list)
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
    """
    PARAMETERS:
      func_w_err (str): The name of the function that had the error in it.
      e (error object): The python error that resulted from an Exception being
        raised.

    RETURNS:
      errorSTATUS (int): Integer representing that an error happened.

    FUNCTION:
      To print out an error statement that may help the user figure out what
      went wrong.

      If this function is used, that means that there was an
      error in the script and this variable is changed from the 0 it was
      assigned at the beginning of hte script to 1 so that other functions
      are not called and the email function can send the Error email.
    """

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

    # Help comments for 'Get_Last_Data_Retrival' function
    if(func_w_err == 'Get_Last_Data_Retrival'):
        if (e_str.endswith("does not match format '%m/%d/%Y %I:%M:%S %p'")):
            help_comment = 'Open Control File: LastDataRetrival.csv in a text editor and make sure the format is "MM/DD/YYYY HH:MM:SS AM"'

    # Help comments for 'Get_Token' function
    if (func_w_err == 'Get_Token'):
        if (e_str.startswith('No section:')):
            help_comment = '    The section in brakets in "cfgFile" variable may not be in the file, or the "dfgFile" cannot be found.  Check both!'

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
    # other functions besides emailing
    errorSTATUS = 1
    return errorSTATUS

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def Email_W_Body(subj, body, email_list, cfgFile=
    r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\DEV_branch\Control_Files\accounts.txt"):

    """
    PARAMETERS:
      subj (str): Subject of the email
      body (str): Body of the email in HTML.  Can be a simple string, but you
        can use HTML markup like <b>bold</b>, <i>italic</i>, <br>carriage return
        <h1>Header 1</h1>, etc.
      email_list (str): List of strings that contains the email addresses to
        send the email to.
      cfgFile {str}: Path to a config file with username and password.
        The format of the config file should be as below with
        <username> and <password> completed:

          [email]
          usr: <username>
          pwd: <password>

        OPTIONAL. A default will be used if one isn't given.

    RETURNS:
      None

    FUNCTION: To send an email to the listed recipients.
      If you want to provide a log file to include in the body of the email,
      please use function Email_w_LogFile()
    """
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import ConfigParser, smtplib

    print '  Starting Email_W_Body()'

    # Set the subj, From, To, and body
    msg = MIMEMultipart()
    msg['Subject']   = subj
    msg['From']      = "Python Script"
    msg['To']        = ', '.join(email_list)  # Join each item in list with a ', '
    msg.attach(MIMEText(body, 'html'))

    # Get username and password from cfgFile
    config = ConfigParser.ConfigParser()
    config.read(cfgFile)
    email_usr = config.get('email', 'usr')
    email_pwd = config.get('email', 'pwd')

    # Send the email
    ##print '  Sending the email to:  {}'.format(', '.join(email_list))
    SMTP_obj = smtplib.SMTP('smtp.gmail.com',587)
    SMTP_obj.starttls()
    SMTP_obj.login(email_usr, email_pwd)
    SMTP_obj.sendmail(email_usr, email_list, msg.as_string())
    SMTP_obj.quit()
    time.sleep(2)

    print '  Successfully emailed results.'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                                 FUNCTION Copy_Features()
def Copy_Features(in_FC, out_FC):
    """
    PARAMETERS:
      in_FC (str): Full path to an input feature class.
      out_FC (str): Full path to an existing output feature class.

    RETURNS:
      None

    FUNCTION:
      To copy the features from one feature class to another existing
      feature class.
    """

    print 'Starting Copy_Features()...'

    print '  Copying Features from: "{}"'.format(in_FC)
    print '                     To: "{}"'.format(out_FC)

    arcpy.CopyFeatures_management(in_FC, out_FC)

    print 'Finished Copy_Features()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                                  RUN MAIN
if __name__ == '__main__':
    main()
