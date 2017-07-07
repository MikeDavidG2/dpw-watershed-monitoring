#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
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
    master_FGDB = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\DEV\Data\DPW_Science_and_Monitoring_prod.gdb'

    # Target database to be updated
    to_update_db = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\DEV\Data\Testing_AccessDatabase.mdb'

    # Items to be updated that exist in both 'Master' and 'Target' databases
    # Items in 'Master' and 'Target' need the same respective schema
    items_to_update = ['Field_Data', 'Sites_Data']


    # Set to "True" to have 'print' statements be written to the log_file
    # Set to "False" to have 'print' statements print to screen
    run_Write_Print_To_Log = True
    log_file = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\DEV\Data\Logs\Update_DPW_w_MasterData'

    # Flag that is changed to "False" if there are errors
    success = True
    #---------------------------------------------------------------------------
    #                         Start calling Functions()
    #---------------------------------------------------------------------------

    # Turn the 'print' statement into a logging object
    if (run_Write_Print_To_Log):
        orig_stdout = Write_Print_To_Log(log_file)

    for item in items_to_update:

        print 'Processing item: {}\n'.format(item)
        # Set paths to 'Master' and 'Target' item
        master_item    = os.path.join(master_FGDB, item)
        item_to_update = os.path.join(to_update_db, item)

        # Test to make sure there is no existing schema lock
        no_schema_lock = Test_Schema_Lock(item_to_update)

        # If there is no schema lock, delete the rows in the item then copy new data to it
        if no_schema_lock:
            Delete_Rows(item_to_update)

            # Get the dataset type (i.e. 'Table' or 'FeatureClass') to determine
            # if script should copy ROWS or copy FEATURES.
            dataset_type = Get_Dataset_Type(item_to_update)

            if (dataset_type == 'Table'):
                Copy_Rows(master_item, item_to_update)

            if (dataset_type == 'FeatureClass'):
                Copy_Features(master_item, item_to_update)

            print 'Success updating "{}"'.format(item_to_update)
            print '------------------------------------------------------------\n'

        else:
            print '***ERROR!  There was a schema lock on "{}", OR the item couldn\'t be found.***\n  Not able to update database.'.format(item_to_update)
            print '  Please make sure that item exists, and that everyone has disconnected from the database.'
            print '  Then rerun this script.'
            print '------------------------------------------------------------\n'
            success = False

    #---------------------------------------------------------------------------
    #                         End of script reporting
    #---------------------------------------------------------------------------
    if success == True:
        print 'SUCCESSFULLY ran script.'

    else:
        print '***ERRORS running script!  See above for messages.***'

    # Return sys.stdout back to its original setting
    if (run_Write_Print_To_Log):

        # Footer for log file
        finish_time_str = [datetime.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')][0]
        print '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
        print '                    {}'.format(finish_time_str)
        print '              Finished Update_DPW_w_MasterData.py'
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

        sys.stdout = orig_stdout

        print '\nDone with script.  Success = {}.'.format(str(success))
        print '  Please find log file location above for more info.'

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
