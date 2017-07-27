#-------------------------------------------------------------------------------
# Name:        Update_DPW_w_MasterData.py
# Purpose:
"""
To update a database (to_update_db) with data from an SDE (master_SDE).
Intended to be run manually by DPW when they want to update their database.
Script uses a SDE file to access SDEP data to copy over to the Access Database.

PROCESS:
1. Creates a log file.
2. Gets a list of the Tables and FCs in master_SDE.
3. Tests to see if the 'item' is in master_SDE.
4. Tests to see if the 'item' is in Access and if it can have a Schema lock
   placed on it.
5. Deletes rows in Access for that item
6. Copies rows from 'master_SDE' item to 'to_update_db' item
"""
# Author:      mgrue
#
# Created:     09/06/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import arcpy, datetime, os

arcpy.env.overwriteOutput = True

def main():

    print 'Starting to run Update_DPW_w_MasterData.py\n'
    #---------------------------------------------------------------------------
    #                              Set variables
    #---------------------------------------------------------------------------

    # Master FGDB used to update
    master_SDE = r"W:\Script\Connection_Files\AD @ SDEP.sde"
    master_prefix = 'SDEP.SANGIS.'

    # Target database to be updated
    to_update_db = r"W:\AccessDatabase.mdb"

    # Items to be updated that exist in both 'Master' and 'Target' databases
    # Items in 'Master' and 'Target' need the same name and respective schema
    items_to_update = ['DPW_WP_FIELD_DATA', 'DPW_WP_SITES']


    # Set to "True" to have 'print' statements be written to the log_file
    # Set to "False" to have 'print' statements print to screen
    run_Write_Print_To_Log = True  # TODO: Change this to 'True' when done testing
    log_file = r'W:\Script\Logs\Update_DPW_w_MasterData'

    # Flag that is changed to "False" if there are errors
    success = True

    #---------------------------------------------------------------------------
    #                         Start Running Script
    #---------------------------------------------------------------------------

    # Turn the 'print' statement into a logging object
    if (run_Write_Print_To_Log):
        orig_stdout = Write_Print_To_Log(log_file)
        
    # Get a list of Tables and Feature Classes in master_SDE
    print 'Getting list of FCs and Tables in "{}"'.format(master_SDE)
    arcpy.env.workspace = master_SDE
    fcs_in_sde    = arcpy.ListFeatureClasses()
    print 'Got FCs'
    tables_in_sde = arcpy.ListTables()
    print 'Got Tables'
    # TODO: remove the below print statements when done testing
    print 'xxxxxxxxxxxxxxxxxxxxxxxxxx'
    print fcs_in_sde
    print 'xxxxxxxxxxxxxxxxxxxxxxxxxx'
    print tables_in_sde
    print 'xxxxxxxxxxxxxxxxxxxxxxxxxx'
    
    print '----------------------------------------------------------'

    for item in items_to_update:

        print 'Processing item: {}\n'.format(item)

        # Set paths to 'Master' and 'Target' item
        master_item    = os.path.join(master_SDE, master_prefix + item)
        item_to_update = os.path.join(to_update_db, item)
        print '  SDE path:    "{}"'.format(master_item)
        print '  Access path: "{}"'.format(item_to_update)

        # Test to see if the item is in workspace.
        if ((master_prefix + item) in fcs_in_sde) or ((master_prefix + item) in tables_in_sde):
            item_in_workspace = True
        else:
            item_in_workspace = False
        print '  Item "{}" in workspace = "{}"\n'.format((master_prefix + item), item_in_workspace)

        # Test to make sure there is no existing schema lock, and that the item
        #   exists in the Access Database.  If it does not exist in Access,
        #   then do not continue processing it.
        no_schema_lock = Test_Schema_Lock(item_to_update)

        # If there is no schema lock, and the item is in master_SDE
        #   then delete the rows of the item in Access then copy new data to it
        if no_schema_lock and item_in_workspace:
            Delete_Rows(item_to_update)

            # Get the dataset type (i.e. 'Table' or 'FeatureClass') to determine
            # if script should copy ROWS or copy FEATURES.
            dataset_type = Get_Dataset_Type(item_to_update)

            if (dataset_type == 'Table'):
                Copy_Rows(master_item, item_to_update)

            if (dataset_type == 'FeatureClass'):
                Copy_Features(master_item, item_to_update)

            print 'Success updating "{}"'.format(item_to_update)
            print '\n----------------------------------------------------------'

        if no_schema_lock == False:  # Item doesn't exist, or there is already a schema lock on item in Access
            print '***ERROR!  The item "{}" could not be found, OR There was a schema lock on it.***\n  Not able to update this item in Access.'.format(item_to_update)
            print '  Please make sure that item exists, and that everyone has disconnected from the database.'
            print '  Then rerun this script.'
            print '\n----------------------------------------------------------'
            success = False

        if item_in_workspace == False:  # Item is not in the master_SDE
            print '***ERROR!  The item "{}" is not in "{}", the Access Database was not updated.'.format(item, master_SDE)
            print '\n----------------------------------------------------------'
            success = False

    #---------------------------------------------------------------------------
    #                         End of script reporting
    #---------------------------------------------------------------------------
    if success == True:
        print '\nSUCCESSFULLY ran script.'

    else:
        print '***ERRORS running script!  See above for messages.***'

    # Return sys.stdout back to its original setting
    if (run_Write_Print_To_Log):

        # Footer for log file
        finish_time_str = [datetime.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')][0]
        print '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
        print '                    {}'.format(finish_time_str)
        print '              Finished Update_DPW_w_MasterData.py'
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

        sys.stdout = orig_stdout

        print '\nDone with script.  Success = {}.'.format(str(success))

    if success == False:
        print '*** ERROR!  There were errors with script.'
        print '  Please find log file location above for more info.'
        raw_input = 'Press ENTER to quit'

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#                           START DEFINING FUNCTIONS
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Write_Print_To_Log()
def Write_Print_To_Log(log_file):
    """
    PARAMETERS:
      log_file (str): Path to log file.  The part after the last "\" will be the
        name of the .log file after the date, time, and ".log" is appended to it.

    RETURNS:
      orig_stdout (os object): The original stdout is saved in this variable so
        that the script can access it and return stdout back to its orig settings.

    FUNCTION:
      To turn all the 'print' statements into a log-writing object.  A new log
        file will be created based on log_file with the date, time, ".log"
        appended to it.  And any print statements after the command
        "sys.stdout = write_to_log" will be written to this log.
      It is a good idea to use the returned orig_stdout variable to return sys.stdout
        back to its original setting.
      NOTE: This function needs the function Get_DT_To_Append() to run

    """
    print 'Starting Write_Print_To_Log()...'

    # Get the original sys.stdout so it can be returned to normal at the
    #    end of the script.
    orig_stdout = sys.stdout

    # Get DateTime to append
    dt_to_append = Get_DT_To_Append()

    # Create the log file with the datetime appended to the file name
    log_file_date = '{}_{}.log'.format(log_file,dt_to_append)
    write_to_log = open(log_file_date, 'w')

    # Make the 'print' statement write to the log file
    print '  Setting "print" command to write to a log file found at:\n    {}'.format(log_file_date)
    sys.stdout = write_to_log

    # Header for log file
    start_time = datetime.datetime.now()
    start_time_str = [start_time.strftime('%m/%d/%Y  %I:%M:%S %p')][0]
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print '                  {}'.format(start_time_str)
    print '             START Update_DPW_w_MasterData.py'
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n'

    return orig_stdout

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Get_dt_to_append
def Get_DT_To_Append():
    """
    PARAMETERS:
      none

    RETURNS:
      dt_to_append (str): Which is in the format 'YYYY_M_D__H_M_S'

    FUNCTION:
      To get a formatted datetime string that can be used to append to files
      to keep them unique.
    """
    print '  Starting Get_DT_To_Append()...'

    start_time = datetime.datetime.now()

    date = '%s_%s_%s' % (start_time.year, start_time.month, start_time.day)
    time = '%s_%s_%s' % (start_time.hour, start_time.minute, start_time.second)

    dt_to_append = '%s__%s' % (date, time)

    print '    DateTime to append: "{}"'.format(dt_to_append)

    print '  Finished Get_DT_To_Append()'
    return dt_to_append

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Test_Schema_Lock()
def Test_Schema_Lock(dataset):
    """
    PARAMETERS:
      dataset (str): Full path to a dataset to be tested if there is a schema lock

    RETURNS:
      no_schema_lock (Boolean): "True" or "False" if there is no schema lock

    FUNCTION:
      To perform a test on a dataset and return "True" if there is no schema
      lock, and "False" if a schema lock already exists.
    """

    print 'Starting Test_Schema_Lock()...'

    print '  Testing dataset: "{}"'.format(dataset)

    no_schema_lock = arcpy.TestSchemaLock(dataset)

    print '  Dataset available to have a schema lock applied to it = "{}"'.format(no_schema_lock)

    print 'Finished Test_Schema_Lock()\n'

    return no_schema_lock

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                            FUNCTION Delete_Rows()
def Delete_Rows(in_item):
    """
    PARAMETERS:
      in_item (str): Full path to an item.

    RETURNS:
      None

    FUNCTION:
      To delete the rows from one item.
    """

    print 'Starting Delete_Rows()...'

    print '  Deleting Rows from: "{}"'.format(in_item)

    arcpy.DeleteRows_management(in_item)

    print 'Finished Delete_Rows()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                                 FUNCTION Get_Dataset_Type()
def Get_Dataset_Type(in_item):
    """
    PARAMETERS:
      in_item (str): Full path to an item.

    RETURNS:
      dataset_type (str): The dataset type of the item.  Common results include:
        'FeatureClass'
        'Table'
        'GeometricNetwork'
        'RasterDataset'

    FUNCTION:
      To get the dataset type of the 'in_item' and return a string describing
      the type of dataset.  Used when the main() may want to treat the item
      differently based on the dataset type.

      For example:
        A 'Table' may require an        'arcpy.CopyRows_management()' while,
        A 'FeatureClass' may require an 'arcpy.CopyFeatures_management()'
    """

    print 'Starting Get_Dataset_Type()...'
    print '  Getting Dataset Type of: "{}":'.format(in_item)

    desc = arcpy.Describe(in_item)
    dataset_type = desc.datasetType

    print '    "{}"'.format(dataset_type)
    print 'Finished Get_Dataset_Type\n'

    return dataset_type

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                                 FUNCTION Copy_Rows()
def Copy_Rows(in_table, out_table):
    """
    PARAMETERS:
      in_table (str): Full path to an input table.
      out_table (str): Full path to an existing output table.

    RETURNS:
      None

    FUNCTION:
      To copy the rows from one table to another table.
    """

    print 'Starting Copy_Rows()...'

    print '  Copying Rows from: "{}"'.format(in_table)
    print '                 To: "{}"'.format(out_table)

    arcpy.CopyRows_management(in_table, out_table)

    print 'Finished Copy_Rows()\n'

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
    print '                 To: "{}"'.format(out_FC)

    arcpy.CopyFeatures_management(in_FC, out_FC)

    print 'Finished Copy_Features()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

# Run the main()
if __name__ == '__main__':
    main()
