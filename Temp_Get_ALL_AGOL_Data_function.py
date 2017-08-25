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
import arcpy, urllib, urllib2
arcpy.env.overwriteOutput = True
def main():
    cfgFile = r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\DEV_branch\Control_Files\accounts.txt"

    AGOL_fields = '*'
    query_url   = 'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/DPW_WP_SITES_DEV/FeatureServer/0/query'
    dl_in_groups_of = 500
    wkg_folder      = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data'
    wkg_FGDB        = r'DPW_Science_and_Monitoring_wkg.gdb'
    orig_FC         = 'D_SITES_all_orig'


    token = Get_Token(cfgFile)



    Get_ALL_AGOL_Data(AGOL_fields, token, query_url, dl_in_groups_of, wkg_folder, wkg_FGDB, orig_FC)
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
#                             FUNCTION Get_AGOL_Data()
def Get_ALL_AGOL_Data(AGOL_fields, token, query_url, dl_in_groups_of, wkg_folder, wkg_FGDB, orig_FC):
    """
    PARAMETERS:
      AGOL_fields (str) = The fields we want to have the server return from our query.
        use the string ('*') to return all fields.
      token (str) = The token obtained by the Get_Token() which gives access to
        AGOL databases that we have permission to access.
      query_url (str) = The URL address for the feature service that allows us
        to query the database.
        Should be the service URL on AGOL (up to the '/FeatureServer' part
        plus the string '/0/query'.
      where_clause (str) = The where clause to add to the query to receive a
        subset of the full dataset.
      wkg_folder (str) = Full path to the 'Data' folder that contains the FGDB's,
        Excel files, Logs, and Pictures.
      wkg_FGDB (str) = Name of the working FGDB in the wkgFolder.
      orig_FC (str) = Name of the FC that will hold the original data downloaded
        by this function.  This FC gets overwritten every time the script is run.

    RETURNS:
      None

    FUNCTION:
      To download data from AGOL.  This function, establishs a connection to the
      data, creates a FGDB (if needed), creates a FC (or overwrites the existing
      one to store the data, and then copies the data from AGOL to the FC.

    NOTE:
      Need to have obtained a token from the Get_Token() function.
    """
    # TODO: finish testing this function and clean up the comments and documentation.
    # TODO: get this function into myFunc when finished testing/documenting.
    # TODO: get this function into DPW_Science_and_Monitoring.py and connect
    #       to the main() to download all SITES data.

    print 'Starting Get_ALL_AGOL_Data()'


    start_OBJECTID = 1
    end_OBJECTID   = dl_in_groups_of
    first_iteration = True
    try_get_data = True

    while try_get_data:
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
            # If no data downloaded, stop the loop
            print '\n  No more data to download'
            print '  Feature Service: %s' % str(fsURL)
            try_get_data = False

        #-----------------------------------------------------------------------
        # Process d/l data

        if try_get_data == True:
            if first_iteration == True:  # Then this is the first run and d/l data to the orig_FC
                path = wkg_folder + "\\" + wkg_FGDB + '\\' + orig_FC
            else:
                path = wkg_folder + "\\" + wkg_FGDB + '\\temp_to_append'

            #Copy the features to the FGDB.
            print '  Copying AGOL database features to: %s' % path
            arcpy.CopyFeatures_management(fs,path)

            # If this is a subsequent run then append the newly d/l data to the orig_FC
            if first_iteration == False:
                orig_path = wkg_folder + "\\" + wkg_FGDB + '\\' + orig_FC
                print 'Appending\n  {}\n  to\n  {}'.format(path, orig_path)
                arcpy.Append_management(path, orig_path, 'NO_TEST')

                print 'Deleting temp_to_append'
                arcpy.Delete_management(path)

            start_OBJECTID = end_OBJECTID + 1
            end_OBJECTID   = end_OBJECTID + dl_in_groups_of
            first_iteration = False

    if first_iteration == False:
        print '  At least 1 round of data was downloaded'
        print "  Successfully retrieved data.\n"
    else:
        print '  * WARNING, no data was downloaded. *'

    print 'Finished Get_AGOL_Data()'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
