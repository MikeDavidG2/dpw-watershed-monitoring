#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mgrue
#
# Created:     25/08/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import arcpy, urllib, urllib2, json, time
arcpy.env.overwriteOutput = True

def main():
    cfgFile = r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\DEV_branch\Control_Files\accounts.txt"

    AGOL_fields = '*'
    FS_url   = 'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/DPW_WP_SITES_DEV/FeatureServer'
    index_of_layer = 0
    wkg_folder      = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data'
    wkg_FGDB        = r'DPW_Science_and_Monitoring_wkg.gdb'
    orig_FC         = 'D_SITES_all_orig'

    sites_data      = wkg_folder + '\\' + wkg_FGDB + '\\' + orig_FC
    prod_sites_data = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\DPW_Science_and_Monitoring_prod.gdb\DPW_WP_SITES'
    required_fields = ['WMA', 'Location_Description']

    email_list = ['michael.grue@sdcounty.ca.gov']

    token = Get_Token(cfgFile)

    Get_AGOL_Data_All(AGOL_fields, token, FS_url, index_of_layer, wkg_folder, wkg_FGDB, orig_FC)

    valid_data = Check_Sites_Data(sites_data, required_fields, prod_sites_data, email_list)

    if valid_data:
        Copy_Features(sites_data, prod_sites_data)
    else:
        print 'SITES data is NOT valid.  Data not copied to prod database, please fix errors above.'

    print 'FINISHED'
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                       FUNCTION:    Get AGOL token

def Get_Token(cfgFile, gtURL="https://www.arcgis.com/sharing/rest/generateToken"):
    """
    PARAMETERS:
      cfgFile (str):
        Path to the .txt file that holds the user name and password of the
        account used to access the data.  This account must be in a group
        that has access to the online database.
      gtURL {str}: URL where ArcGIS generates tokens. OPTIONAL.
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

    import ConfigParser, urllib, urllib2, json

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

    print 'Finished Get_AGOL_Data_All()\n'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def Check_Sites_Data(wkg_sites_data, required_fields, prod_sites_data, email_list):
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
        3. Check for any NULL values in Station_ID
             --Send an email if error
        4. Check for any Station_ID's in the production database that are not
           in the working data
             --Send an email if error
        5. Report any Station_ID's that are in wkg data but not prod
           (These are probably newly added sites, and not errors)
             --Send an email if any new site in AGOL (not an error email)

    NOTE: This function assumes that the function 'Email_W_Body()' is accessible.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Check_Sites_Data()'
    print '  Checking: {}'.format(wkg_sites_data)

    valid_data = True

    #---------------------------------------------------------------------------
    #          Check for duplicate Station IDs in the working Sites data
    print '\n  Checking for any duplicate Station IDs'

    # Make a list of all the site ID's in the working data
    unique_ids = []
    duplicate_ids = []

    where_clause = "Station_ID <> ''"  # No need to search for blank duplicates
    with arcpy.da.SearchCursor(wkg_sites_data, ['Station_ID'], where_clause) as cursor:
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
        subj = 'Sci and Mon Error.  There are duplicate Station IDs in the SITES database on AGOL'

        # Format the Body in html
        list_to_string = '<br> '.join(duplicate_ids)

        body = ("""DPW staff, please log on to the AGOL SITES database and
        correct the below duplicates:<br>
        (<i>NOTE: If a Station ID is listed more than once, that ID has more than
        one duplicate</i>)<br><br>
        {}""".format(list_to_string))

        # Send the email
        Email_W_Body(subj, body, email_list)

    #---------------------------------------------------------------------------
    #             Check for any NULL values in required fields
    print '\n  Checking for any NULL values in required fields'
    ids_w_null_values = []

    for field in required_fields:
        where_clause = "{fld} IS NULL OR {fld} = ''".format(fld = field)
        print '    Checking field: "{}" where: {}'.format(field, where_clause)

        # Get list of Station_IDs with NULL values
        with arcpy.da.SearchCursor(wkg_sites_data, ['Station_ID', field], where_clause) as cursor:
            for row in cursor:
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
        subj = 'Sci and Mon Error.  There are null values in required fields'

        # Format the Body in html
        list_to_string = '<br> '.join(ids_w_null_values)
        req_fields_str = ', '.join(required_fields)

        body = ("""DPW staff, please log on to the AGOL SITES database.  The
        following Station IDs have a NULL value for a required field, please
        enter a value.<br>
        (<i>NOTE: If a Station ID is listed more than once, it has NULL values
        for multiple required fields<br>
        The required fields are: <b>{}</b></i>)<br><br>
        {}""".format(req_fields_str, list_to_string))

        # Send the email
        Email_W_Body(subj, body, email_list)

    #---------------------------------------------------------------------------
    #             Check for any NULL values in Station_ID
    print '\n  Checking for any NULL values in Station_ID'
    num_blank_station_ids = 0

    where_clause = "Station_ID IS NULL OR Station_ID = ''"
    print '    Where: {}'.format(where_clause)
    with arcpy.da.SearchCursor(wkg_sites_data, ['Station_ID'], where_clause) as cursor:
        for row in cursor:
            num_blank_station_ids += 1
    del cursor

    if num_blank_station_ids == 0:  # Then there were no errors
        print '    There are no NULL values for Station_ID'

    else:  # Then there were errors, send an email
        valid_data = False

        print '*** ERROR! There are {} Sites with a NULL value in Station_ID'.format(num_blank_station_ids)
        print '  Sending an email to: {}'.format(', '.join(email_list))

        # Set the email subject
        subj = 'Sci and Mon Error.  There are stations with no Station IDs'

        # Format the Body in html
        body = ("""DPW staff, please log on to the AGOL SITES database and
        enter a value in the Station ID field for the <b>{} Station(s)</b> that
        is/are missing a value in that field.""".format(num_blank_station_ids))

        # Send the email
        Email_W_Body(subj, body, email_list)
    #---------------------------------------------------------------------------
    #            Check for any Station_ID's in the production database
    #                   that are not in the working data

    print '\n  Checking that all Station_IDs in prod are also in the wkg data'

    # Get list of Station ID's that are in the prod data
    prod_station_IDs = []
    with arcpy.da.SearchCursor(prod_sites_data, ['Station_ID']) as cursor:
        for row in cursor:
            prod_station_IDs.append(row[0])
    del cursor

    # Get list of Station ID's that are in the working data
    wkg_station_IDs = []
    with arcpy.da.SearchCursor(wkg_sites_data, ['Station_ID']) as cursor:
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
        subj = 'Sci and Mon Error.  There are stations we cannot find in the AGOL database'

        # Format the Body in html
        list_to_string = '<br> '.join(prod_ids_not_in_wkg)

        body = ("""DPW staff, please log on to the AGOL SITES database and
        find out what happened to the below sites.<br>
        The below sites exist in the production database, but have been either
        renamed to a new Station ID, or they have been deleted on AGOL:<br><br>
        {}""".format(list_to_string))

        # Send the email
        Email_W_Body(subj, body, email_list)
    print ''

    #---------------------------------------------------------------------------
    #        Report any Station_ID's that are in wkg data but not prod
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
        subj = 'Sci and Mon Info.  There were new Station IDs added to AGOL'

        # Format the Body in html

        list_to_string = '<br> '.join(wkg_station_ids_not_in_prod)

        body = ("""DPW staff, the below list are new Station IDs that have been
        added to AGOL database that have not been recorded before.<br>
        Most likely these are new stations, no action is needed on your part.<br>
        <i>However, it is possible that one of the below stations is not a new
        station, but that it was renamed from an existing station.
        This would be an error as existing sites should not be renamed.<br>
        If you received today an email with stations we could not find in the
        AGOL database, please check the below stations to confirm that they are
        valid new sites and were not renamed from existing sites. <i><br><br>
        {}""".format(list_to_string))

        # Send the email
        Email_W_Body(subj, body, email_list)


    #---------------------------------------------------------------------------

    print '  valid_data = {}'.format(valid_data)

    print '\nFinished Check_Sites_Data()'
    return valid_data

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
if __name__ == '__main__':
    main()